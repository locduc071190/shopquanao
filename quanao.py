import streamlit as st
from datetime import datetime, timedelta
# Import thêm Text từ sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, Text, func
from sqlalchemy.orm import sessionmaker, declarative_base, relationship, joinedload 
import pandas as pd
import os

# ---------- Database setup ----------
Base = declarative_base()

class Product(Base):
    __tablename__ = 'products'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    price = Column(Float, default=0.0)
    # THÊM CỘT GIÁ NHẬP
    cost_price = Column(Float, default=0.0) 
    stock = Column(Integer, default=0)
    image_path = Column(String, default='')
    notes = Column(Text, default='')

class Order(Base):
    __tablename__ = 'orders'
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    total = Column(Float, default=0.0)
    items = relationship('OrderItem', back_populates='order', cascade='all, delete-orphan')

class OrderItem(Base):
    __tablename__ = 'order_items'
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey('orders.id'))
    product_id = Column(Integer, ForeignKey('products.id'))
    qty = Column(Integer, default=1)
    price = Column(Float, default=0.0)
    # THÊM CỘT GIÁ NHẬP
    cost_price = Column(Float, default=0.0)
    product = relationship('Product')
    order = relationship('Order', back_populates='items')

class StockMovement(Base):
    __tablename__ = 'stock_movements'
    id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.id'))
    change = Column(Integer)
    reason = Column(String)
    timestamp = Column(DateTime, default=datetime.utcnow)
    product = relationship('Product')

# ---------- Database connection ----------
if not os.path.exists('images'):
    os.makedirs('images')

engine = create_engine('sqlite:///shop_data.db', connect_args={"check_same_thread": False})
Base.metadata.create_all(engine) 

# --- SỬA LỖI MIGRATION (THÊM CỘT VÀO DB CŨ) ---
# Đoạn code này sẽ đảm bảo các cột mới (cost_price) được thêm vào database
# hiện tại mà không cần xóa file shop_data.db
try:
    with engine.connect() as connection:
        # Thêm cột cost_price vào bảng products nếu chưa có
        connection.execute(
            Text("ALTER TABLE products ADD COLUMN cost_price FLOAT DEFAULT 0.0")
        )
        # Thêm cột cost_price vào bảng order_items nếu chưa có
        connection.execute(
            Text("ALTER TABLE order_items ADD COLUMN cost_price FLOAT DEFAULT 0.0")
        )
        connection.commit()
    # Nếu thêm cột thành công, xóa cache để load lại Product với cột mới
    st.cache_data.clear() 
except Exception as e:
    # Bỏ qua lỗi nếu cột đã tồn tại (Lỗi "duplicate column name")
    if "duplicate column name" not in str(e) and "already exists" not in str(e):
        pass

SessionLocal = sessionmaker(bind=engine)

# ---------- Caching Helpers ----------
@st.cache_data
def load_products():
    with SessionLocal() as session:
        return session.query(Product).all()
        
@st.cache_data
def load_orders():
    with SessionLocal() as session:
        return session.query(Order).options(
            joinedload(Order.items).joinedload(OrderItem.product) 
        ).all()

@st.cache_data
def load_stock_movements():
    with SessionLocal() as session:
        return session.query(StockMovement).options(joinedload(StockMovement.product)).all()

def clear_data_cache():
    """Xóa cache sau khi thực hiện thay đổi vào DB."""
    load_products.clear()
    load_orders.clear()
    load_stock_movements.clear()

# ---------- Database Helper Functions ----------

def add_product(name, price, cost_price, stock, notes='', image_file=None):
    img_path = ''
    if image_file:
        ext = os.path.splitext(image_file.name)[1]
        filename = f"{datetime.utcnow().timestamp():.0f}{ext}"
        save_path = os.path.join('images', filename)
        with open(save_path, 'wb') as f:
            f.write(image_file.read())
        img_path = save_path
    with SessionLocal() as session:
        p = Product(name=name, price=price, cost_price=cost_price, stock=stock, notes=notes, image_path=img_path)
        session.add(p)
        session.flush() 
        add_stock_movement(p.id, stock, 'Initial / Import', commit=False, session=session)
        
        product_id = p.id
        product_name = p.name

        session.commit()
        return product_id, product_name

def add_stock_movement(product_id, change, reason='manual', commit=True, session=None):
    if session is None:
        session = SessionLocal()
        close_session = True
    else:
        close_session = False
        
    try:
        m = StockMovement(product_id=product_id, change=change, reason=reason, timestamp=datetime.utcnow())
        p = session.get(Product, product_id)
        if p:
            p.stock = (p.stock or 0) + change
            session.add(m)
        else:
            raise ValueError(f"Sản phẩm id={product_id} không tồn tại")
        
        if commit:
            session.commit()
        return m
    finally:
        if close_session:
            session.close()

def create_order(items):
    with SessionLocal() as session:
        total = 0.0
        
        for it in items:
            p = session.get(Product, it['product_id'])
            if not p:
                raise ValueError(f"Sản phẩm id={it['product_id']} không tồn tại")
            if p.stock < it['qty']:
                raise ValueError(f"Không đủ tồn cho **{p.name}** (còn **{p.stock}**, cần **{it['qty']}**)")

        o = Order(created_at=datetime.utcnow(), total=0.0)
        session.add(o)
        session.flush()
        
        for it in items:
            p = session.get(Product, it['product_id'])
            
            oi = OrderItem(
                order_id=o.id, 
                product_id=p.id, 
                qty=it['qty'], 
                price=p.price,
                cost_price=p.cost_price 
            )
            session.add(oi)
            
            p.stock -= it['qty']
            total += p.price * it['qty']
            
            sm = StockMovement(product_id=p.id, change=-it['qty'], reason='Sale', timestamp=datetime.utcnow())
            session.add(sm)
            
        o.total = total
        order_id = o.id
        order_total = o.total
        
        session.commit()
        return order_id, order_total


# ---------- Streamlit UI ----------
st.set_page_config(page_title='Shop Manager', layout='wide')
st.title('👗 Shop Manager - Full Version')

menu = st.sidebar.selectbox('Chức năng', ['Dashboard', 'Sản phẩm', 'Đơn hàng (POS)', 'Nhập kho', 'Thống kê & Báo cáo', 'Xuất dữ liệu'])

# --- Dashboard & Sản phẩm & Đơn hàng (POS) & Nhập kho ---

if menu == 'Dashboard':
    st.header('📈 Dashboard')
    products = load_products()
    orders = load_orders()
    total_products = len(products)
    total_orders = len(orders)
    total_stock = sum([p.stock for p in products])
    
    col1, col2, col3 = st.columns(3)
    col1.metric('Tổng sản phẩm', total_products)
    col2.metric('Tổng đơn hàng', total_orders)
    col3.metric('Tổng tồn kho', total_stock)
    
    st.caption('Dữ liệu được làm mới sau mỗi thao tác thêm/sửa/tạo đơn.')

elif menu == 'Sản phẩm':
    st.header('📦 Quản lý sản phẩm')
    
    with st.expander('➕ Thêm sản phẩm mới'):
        with st.form('add_product'):
            name = st.text_input('Tên sản phẩm')
            col_p, col_c = st.columns(2)
            with col_p:
                price = st.number_input('Giá Bán', value=0.0, step=1000.0)
            with col_c:
                cost_price = st.number_input('Giá Nhập (Cost Price)', value=0.0, step=1000.0)
                
            stock = st.number_input('Tồn ban đầu', min_value=0, value=0)
            notes = st.text_area('Ghi chú')
            image_file = st.file_uploader('Ảnh sản phẩm', type=['jpg','jpeg','png'])
            submitted = st.form_submit_button('Thêm')
            
            if submitted:
                if not name:
                    st.error('Cần tên sản phẩm')
                elif price < cost_price:
                    st.error('Giá bán phải lớn hơn hoặc bằng Giá nhập.')
                else:
                    try:
                        product_id, product_name = add_product(name, price, cost_price, int(stock), notes, image_file)
                        
                        # Hiển thị thông báo thành công và sau đó dùng st.rerun()
                        st.success(f'✅ Đã thêm **{product_name}** | ID: **{product_id}**')
                        
                        # YÊU CẦU 1: RELOAD GIAO DIỆN ĐỂ XÓA GIÁ TRỊ NHẬP CŨ
                        clear_data_cache()
                        st.rerun() 
                        
                    except Exception as e:
                        st.error(f"Lỗi khi thêm sản phẩm: {e}")

    products = load_products()
    st.subheader('Danh sách sản phẩm hiện tại')
    
    if products:
        
        header_cols = st.columns([1, 1, 2, 2, 1, 1])
        header_cols[0].markdown('**Ảnh**')
        header_cols[1].markdown('**ID**')
        header_cols[2].markdown('**Tên sản phẩm**')
        header_cols[3].markdown('**Giá (Bán/Nhập)**')
        header_cols[4].markdown('**Tồn kho**')
        header_cols[5].markdown('**Ghi chú**')
        
        st.markdown('---') 
        
        for p in products:
            cols = st.columns([1, 1, 2, 2, 1, 1])
            
            with cols[0]:
                if p.image_path and os.path.exists(p.image_path):
                    st.image(p.image_path, width=60)
                else:
                    st.write('🖼️')
                    
            cols[1].write(p.id)
            cols[2].write(p.name)
            
            cols[3].markdown(f"**Bán:** {p.price:,.0f} VND <br> **Nhập:** {p.cost_price:,.0f} VND", unsafe_allow_html=True)
            
            stock_display = f'**{p.stock}**' if p.stock > 10 else f'**:red[{p.stock}]**'
            cols[4].markdown(stock_display)

            cols[5].write(p.notes[:30] + '...' if len(p.notes) > 30 else p.notes)
            
            st.markdown('---') 

    else:
        st.info('Chưa có sản phẩm nào được thêm.')

elif menu == 'Đơn hàng (POS)':
    st.header('🛒 POS - Tạo đơn bán')
    st.markdown('***(Chức năng dành cho nhân viên cửa hàng)***')
    products = load_products() 
    active_products = [p for p in products if p.stock > 0]
    
    # Dùng placeholder để hiển thị thông báo cố định
    status_placeholder = st.empty() 

    if not active_products:
        st.info('Chưa có sản phẩm còn tồn kho để bán.')
    else:
        st.markdown('### Chọn sản phẩm và số lượng bán')
        order_items_input = {}
        total_estimated = 0.0

        cols = st.columns([1, 1, 3, 1, 1])
        cols[0].markdown('**Ảnh**')
        cols[1].markdown('**ID**')
        cols[2].markdown('**Tên sản phẩm / Giá**')
        cols[3].markdown('**Tồn**')
        cols[4].markdown('**SL Bán**')
        st.markdown('---')

        for p in active_products:
            c = st.columns([1, 1, 3, 1, 1])
            
            with c[0]:
                if p.image_path and os.path.exists(p.image_path):
                    st.image(p.image_path, width=60)
                else:
                    st.write('🖼️')
                    
            c[1].write(p.id)
            c[2].write(f'{p.name} (Bán: {p.price:,.0f} VND)')
            
            stock_display = f'**{p.stock}**' if p.stock > 10 else f'**:red[{p.stock}]**'
            c[3].markdown(stock_display)
            
            qty = c[4].number_input(
                'SL', 
                min_value=0, 
                max_value=p.stock, 
                value=0, 
                key=f'qty_pos_{p.id}', 
                label_visibility="collapsed"
            )
            
            if qty > 0:
                order_items_input[p.id] = int(qty)
                total_estimated += p.price * qty
            
            st.markdown('---')

        st.divider()
        st.markdown(f"#### 💰 Tổng tiền: **{total_estimated:,.0f} VND**")

        if st.button('✅ Thanh toán / Tạo đơn', type='primary'):
            try:
                if not order_items_input:
                    status_placeholder.warning('Chưa chọn sản phẩm để tạo đơn.')
                else:
                    order_items_list = [{'product_id': pid, 'qty': qty} for pid, qty in order_items_input.items()]
                    
                    order_id, order_total = create_order(order_items_list)
                    
                    # YÊU CẦU 2: HIỂN THỊ THÔNG BÁO LÂU HƠN/CỐ ĐỊNH
                    # Dùng status_placeholder để hiển thị thông báo thành công cố định
                    status_placeholder.success(f'🎉 Đã tạo đơn **#{order_id}** thành công! Tổng cộng: **{order_total:,.0f} VND**. (Thông báo sẽ mất khi thao tác tiếp theo hoặc tải lại trang)')
                    clear_data_cache()
                    # Không dùng st.rerun() để thông báo cố định được giữ lại
                    
            except ValueError as e:
                status_placeholder.error(f"❌ Lỗi tồn kho: {e}")
            except Exception as e:
                status_placeholder.error(f"❌ Lỗi hệ thống khi tạo đơn: {e}")
                
elif menu == 'Nhập kho':
    st.header('➕ Nhập/Xuất kho (Stock Movement)')
    products = load_products()
    
    if not products:
        st.warning('Vui lòng thêm sản phẩm trước khi nhập kho.')
        
    with st.form('stock_adjustment'):
        product_options = {p.id: f"{p.name} (Tồn: {p.stock})" for p in products}
        
        selected_option = st.selectbox('Chọn sản phẩm', options=list(product_options.values()))
        
        selected_id = next((pid for pid, name_stock in product_options.items() if name_stock == selected_option), None)

        if selected_id:
            st.info(f"Sản phẩm đang chọn: **{selected_option}**")
            
            change = st.number_input('Số lượng thay đổi (+ để nhập, - để xuất/hỏng)', step=1, value=0)
            reason = st.text_area('Lý do (Nhập hàng/Kiểm kho/Hỏng hóc...)')
            
            submitted = st.form_submit_button('Cập nhật tồn kho')
            
            if submitted and change != 0:
                try:
                    m = add_stock_movement(selected_id, int(change), reason)
                    st.success(f'✅ Đã cập nhật **{change}** đơn vị cho sản phẩm.')
                    clear_data_cache()
                    st.rerun() 
                except Exception as e:
                    st.error(f"Lỗi: {e}")
            elif submitted and change == 0:
                st.warning('Vui lòng nhập số lượng thay đổi.')

# ----------------------------------------------------------------------
# 📊 Thống kê & Báo cáo 
# ----------------------------------------------------------------------

elif menu == 'Thống kê & Báo cáo':
    st.header('📊 Thống kê & Báo cáo Bán hàng')
    
    orders = load_orders() 
    if not orders:
        st.info('Chưa có dữ liệu đơn hàng để thống kê.')
    else:
        # Chuẩn bị dữ liệu cho thống kê
        order_data = []
        for o in orders:
            for item in o.items:
                item_cost_price = item.cost_price if hasattr(item, 'cost_price') else 0.0
                gross_profit = item.qty * (item.price - item_cost_price)
                
                order_data.append({
                    'Order ID': o.id,
                    'Ngày': o.created_at.date(),
                    'Tháng': o.created_at.strftime('%Y-%m'),
                    'Tên sản phẩm': item.product.name,
                    'Sản phẩm ID': item.product_id,
                    'Số lượng bán': item.qty,
                    'Giá Bán (SP)': item.price,
                    'Giá Nhập (SP)': item_cost_price,
                    'Tổng tiền Bán Item': item.qty * item.price,
                    'Tổng Vốn Item': item.qty * item_cost_price,
                    'Lợi nhuận Gộp Item': gross_profit,
                    'Tổng tiền Đơn': o.total
                })
        
        df_orders = pd.DataFrame(order_data)
        
        # --- 1. Tổng quan (Trong Expander) ---
        with st.expander('📈 1. Tổng quan Doanh thu & Lợi nhuận', expanded=True):
            
            total_orders_count = df_orders['Order ID'].nunique()
            total_revenue = df_orders.groupby('Order ID')['Tổng tiền Đơn'].first().sum() 
            total_gross_profit = df_orders['Lợi nhuận Gộp Item'].sum()
            
            col_a, col_b, col_c = st.columns(3)
            col_a.metric('Tổng Doanh thu (Sales)', f"{total_revenue:,.0f} VND")
            col_b.metric('Tổng Lợi nhuận Gộp', f"{total_gross_profit:,.0f} VND", delta=f"{total_gross_profit / total_revenue * 100:.2f}%" if total_revenue > 0 else None)
            col_c.metric('Doanh thu TB/Đơn', f"{total_revenue / total_orders_count:,.0f} VND" if total_orders_count > 0 else "0 VND")

        # --- 2. Biểu đồ theo thời gian (Trong Expander) ---
        with st.expander('📅 2. Biểu đồ Doanh thu & Lợi nhuận theo thời gian'):
            
            daily_sales = df_orders.groupby('Order ID')['Tổng tiền Đơn'].first().reset_index()
            daily_sales['Ngày'] = df_orders.groupby('Order ID')['Ngày'].first().reset_index()['Ngày']
            revenue_by_date = daily_sales.groupby('Ngày')['Tổng tiền Đơn'].sum().reset_index().rename(columns={'Tổng tiền Đơn': 'Doanh thu'})
            
            profit_by_date = df_orders.groupby('Ngày')['Lợi nhuận Gộp Item'].sum().reset_index().rename(columns={'Lợi nhuận Gộp Item': 'Lợi nhuận'})

            chart_data = pd.merge(revenue_by_date, profit_by_date, on='Ngày', how='outer').set_index('Ngày')
            
            st.line_chart(chart_data)
            st.dataframe(chart_data.sort_values(by='Ngày', ascending=False), use_container_width=True)

        # --- 3. Top 5 sản phẩm (Trong Expander) ---
        with st.expander('🥇 3. Top 5 sản phẩm bán chạy nhất & Lợi nhuận'):
            
            product_sales = df_orders.groupby('Tên sản phẩm').agg(
                {'Số lượng bán': 'sum', 'Lợi nhuận Gộp Item': 'sum'}
            ).reset_index()
            product_sales = product_sales.sort_values(by='Số lượng bán', ascending=False).head(5)
            
            col_bar, col_data = st.columns([2, 1])
            with col_bar:
                st.bar_chart(product_sales.set_index('Tên sản phẩm')['Lợi nhuận Gộp Item'])
                st.caption('Lợi nhuận gộp theo Top 5 sản phẩm bán chạy')
            with col_data:
                st.dataframe(product_sales, hide_index=True)

        # --- 4. Lịch sử đơn hàng chi tiết (Trong Expander) ---
        with st.expander('🧾 4. Lịch sử các đơn hàng chi tiết (Log POS)'):
            
            def format_order_details(group):
                items = []
                for _, row in group.iterrows():
                    items.append(f"{row['Tên sản phẩm']} x {row['Số lượng bán']} ({row['Giá Bán (SP)']:,.0f} VND)")
                return " | ".join(items)

            order_summary = df_orders.groupby('Order ID').agg(
                Ngày=('Ngày', 'first'),
                Tổng_tiền=('Tổng tiền Đơn', 'first'),
                Tổng_Lợi_nhuận=('Lợi nhuận Gộp Item', 'sum')
            ).reset_index()

            details_series = df_orders.groupby('Order ID').apply(format_order_details).rename('Chi tiết sản phẩm')
            
            order_summary = pd.merge(order_summary, details_series.reset_index(), on='Order ID')

            order_summary = order_summary.rename(columns={
                'Order ID': 'ID',
                'Ngày': 'Ngày tạo',
                'Tổng_tiền': 'Tổng tiền (VND)',
                'Tổng_Lợi_nhuận': 'Lợi nhuận Gộp (VND)',
            })
            
            st.dataframe(order_summary.sort_values(by='Ngày tạo', ascending=False), use_container_width=True, hide_index=True)

# ----------------------------------------------------------------------
# 💾 Xuất dữ liệu (Log) 
# ----------------------------------------------------------------------

elif menu == 'Xuất dữ liệu':
    st.header('💾 Xuất Log & Báo cáo')
    st.markdown('***(Chức năng chỉ tải file CSV. Để xem lịch sử, vui lòng dùng mục "Thống kê & Báo cáo")***')
    
    st.subheader('1. Xuất Log Đơn hàng chi tiết (Orders & Items)')
    
    orders = load_orders()
    df_orders_export = pd.DataFrame() 

    if orders:
        order_data = []
        for o in orders:
            for item in o.items:
                item_cost_price = item.cost_price if hasattr(item, 'cost_price') else 0.0
                order_data.append({
                    'Order ID': o.id,
                    'Created At': o.created_at.strftime('%Y-%m-%d %H:%M:%S'),
                    'Product ID': item.product_id,
                    'Product Name': item.product.name,
                    'Quantity': item.qty,
                    'Selling Price (per item)': item.price,
                    'Cost Price (per item)': item_cost_price,
                    'Gross Profit (per item)': item.price - item_cost_price,
                    'Total Order Value': o.total
                })
        df_orders_export = pd.DataFrame(order_data)

    if not df_orders_export.empty:
        csv_orders = df_orders_export.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Tải Log Đơn hàng chi tiết (.csv)",
            data=csv_orders,
            file_name='shop_orders_detail_log.csv',
            mime='text/csv',
        )
        st.success(f"Log Đơn hàng ({len(df_orders_export)} dòng) đã sẵn sàng để tải xuống.")
    else:
        st.info('Không có dữ liệu đơn hàng để xuất.')

    st.subheader('2. Xuất Log Nhập/Xuất kho (Stock Movements)')
    
    movements = load_stock_movements()
    df_movements = pd.DataFrame()

    if movements:
        movement_data = [{
            'Movement ID': m.id,
            'Timestamp': m.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'Product ID': m.product_id,
            'Product Name': m.product.name,
            'Change (+Nhập/-Xuất)': m.change,
            'Reason': m.reason,
            'Current Stock': m.product.stock 
        } for m in movements]
        df_movements = pd.DataFrame(movement_data)
        
        csv_movements = df_movements.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Tải Log Kho (.csv)",
            data=csv_movements,
            file_name='shop_stock_movements_log.csv',
            mime='text/csv',
        )
        st.success(f"Log Kho ({len(df_movements)} dòng) đã sẵn sàng để tải xuống.")

    else:
        st.info('Không có dữ liệu thay đổi kho để xuất.')