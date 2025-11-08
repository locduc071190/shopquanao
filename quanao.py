import streamlit as st
import pandas as pd
from datetime import datetime
import os
import uuid
import numpy as np 
from io import BytesIO
from sqlalchemy import text, inspect
from sqlalchemy.exc import ProgrammingError

# --- Table Names and Constants (SQL) ---
TABLE_PRODUCTS = 'products'
TABLE_ORDERS = 'orders'
TABLE_ORDER_ITEMS = 'order_items'
TABLE_STOCK_MOVEMENTS = 'stock_movements'
CONNECTION_NAME = 'shop_db' # Phải khớp với [connections.shop_db] trong secrets.toml

# ---------- POSTGRESQL CONNECTION & DATA INITIALIZATION ----------

@st.cache_resource(ttl=3600)
def get_sql_connection():
    # Kiểm tra cấu hình kết nối SQL trong secrets.toml
    if f"connections.{CONNECTION_NAME}" not in st.secrets:
        st.error(f"Lỗi: Không tìm thấy cấu hình '[connections.{CONNECTION_NAME}]' trong file .streamlit/secrets.toml. Vui lòng kiểm tra lại cấu hình Supabase URL.")
        st.stop()
    
    try:
        # Sử dụng API của Streamlit để kết nối SQL
        conn = st.connection(CONNECTION_NAME, type='sql')
        return conn
    except Exception as e:
        st.error(f"Lỗi kết nối PostgreSQL. Vui lòng kiểm tra URL, mật khẩu và quyền truy cập database. Lỗi chi tiết: {e}")
        st.stop()

db_conn = get_sql_connection()

# Hàm tạo bảng nếu chưa tồn tại (Dùng SQLAlchemy inspect và execute)
def initialize_database():
    try:
        with db_conn.session as s:
            inspector = inspect(db_conn.engine)
            
            # 1. Bảng PRODUCTS
            if TABLE_PRODUCTS not in inspector.get_table_names():
                st.info(f"Đang tạo bảng '{TABLE_PRODUCTS}'...")
                s.execute(text(f"""
                    CREATE TABLE {TABLE_PRODUCTS} (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        price REAL DEFAULT 0,
                        cost_price REAL DEFAULT 0,
                        stock INTEGER DEFAULT 0,
                        image_path TEXT,
                        notes TEXT
                    );
                """))
            
            # 2. Bảng ORDERS
            if TABLE_ORDERS not in inspector.get_table_names():
                st.info(f"Đang tạo bảng '{TABLE_ORDERS}'...")
                s.execute(text(f"""
                    CREATE TABLE {TABLE_ORDERS} (
                        id TEXT PRIMARY KEY,
                        created_at TIMESTAMP WITHOUT TIME ZONE,
                        total REAL DEFAULT 0
                    );
                """))

            # 3. Bảng ORDER_ITEMS
            if TABLE_ORDER_ITEMS not in inspector.get_table_names():
                st.info(f"Đang tạo bảng '{TABLE_ORDER_ITEMS}'...")
                s.execute(text(f"""
                    CREATE TABLE {TABLE_ORDER_ITEMS} (
                        id TEXT PRIMARY KEY,
                        order_id TEXT REFERENCES {TABLE_ORDERS}(id),
                        product_id TEXT REFERENCES {TABLE_PRODUCTS}(id),
                        qty INTEGER DEFAULT 0,
                        price REAL DEFAULT 0,
                        cost_price REAL DEFAULT 0
                    );
                """))
                
            # 4. Bảng STOCK_MOVEMENTS
            if TABLE_STOCK_MOVEMENTS not in inspector.get_table_names():
                st.info(f"Đang tạo bảng '{TABLE_STOCK_MOVEMENTS}'...")
                s.execute(text(f"""
                    CREATE TABLE {TABLE_STOCK_MOVEMENTS} (
                        id TEXT PRIMARY KEY,
                        product_id TEXT REFERENCES {TABLE_PRODUCTS}(id),
                        change INTEGER DEFAULT 0,
                        reason TEXT,
                        timestamp TIMESTAMP WITHOUT TIME ZONE
                    );
                """))
            
            s.commit()
    
    except ProgrammingError as e:
        # Xử lý lỗi khi bảng đã tồn tại (nếu inspect không hoạt động chính xác)
        st.info("Database đã được khởi tạo.")
    except Exception as e:
        st.error(f"Lỗi khởi tạo Database: {e}")
        st.stop()


# NEW: Tải dữ liệu từ một bảng
@st.cache_data(ttl=5) # Cache 5 giây
def load_data(table_name):
    try:
        # Sử dụng db_conn.query() để tải dữ liệu vào DataFrame
        df = db_conn.query(f"SELECT * FROM {table_name};", ttl=5)
        return df
    except Exception as e:
        st.warning(f"Lỗi đọc dữ liệu từ bảng '{table_name}'. Vui lòng kiểm tra lại cấu trúc bảng. Lỗi: {e}")
        # Trả về DataFrame trống với các cột cần thiết nếu đọc lỗi
        required_cols = {
            TABLE_PRODUCTS: ['id', 'name', 'price', 'cost_price', 'stock', 'image_path', 'notes'],
            TABLE_ORDERS: ['id', 'created_at', 'total'],
            TABLE_ORDER_ITEMS: ['id', 'order_id', 'product_id', 'qty', 'price', 'cost_price'],
            TABLE_STOCK_MOVEMENTS: ['id', 'product_id', 'change', 'reason', 'timestamp']
        }.get(table_name, [])
        return pd.DataFrame(columns=required_cols)

# NEW: Các hàm tải dữ liệu cụ thể
def load_products():
    return load_data(TABLE_PRODUCTS)

def load_orders():
    return load_data(TABLE_ORDERS)

def load_order_items():
    return load_data(TABLE_ORDER_ITEMS)

def load_stock_movements():
    return load_data(TABLE_STOCK_MOVEMENTS)

def clear_data_cache():
    """Xóa cache sau khi thực hiện thay đổi vào DB."""
    load_data.clear() 

# ---------- Database Helper Functions (SQL OVERHAUL) ----------

def add_product(name, price, cost_price, stock, notes='', image_file=None):
    
    img_path = ''
    if image_file:
        ext = os.path.splitext(image_file.name)[1]
        filename = f"{datetime.utcnow().timestamp():.0f}{ext}"
        # Đảm bảo thư mục 'images' tồn tại
        if not os.path.exists('images'):
            os.makedirs('images')
        save_path = os.path.join('images', filename)
        with open(save_path, 'wb') as f:
            f.write(image_file.read())
        img_path = save_path
    
    new_product_id = str(uuid.uuid4())
    
    with db_conn.session as s:
        # 1. Thêm sản phẩm
        s.execute(text(f"""
            INSERT INTO {TABLE_PRODUCTS} (id, name, price, cost_price, stock, image_path, notes)
            VALUES (:id, :name, :price, :cost_price, :stock, :image_path, :notes)
        """), {
            'id': new_product_id,
            'name': name,
            'price': float(price),
            'cost_price': float(cost_price),
            'stock': int(stock),
            'image_path': img_path,
            'notes': notes
        })
        s.commit()
    
    # 2. Thêm movement ban đầu
    add_stock_movement(new_product_id, stock, 'Initial / Import', skip_product_update=True)
    clear_data_cache()
    return new_product_id, name

def update_product(product_id, name, price, cost_price, notes, image_file=None, remove_image=False):
    
    products_df = load_products()
    p = products_df[products_df['id'] == product_id]
    
    if p.empty:
        raise ValueError(f"Sản phẩm id={product_id} không tồn tại")
    
    old_image_path = p['image_path'].iloc[0] if pd.notna(p['image_path'].iloc[0]) else ''
    img_path_update = old_image_path # Mặc định giữ nguyên

    # 1. Handle image removal
    if remove_image and old_image_path and os.path.exists(old_image_path):
        os.remove(old_image_path)
        img_path_update = '' 

    # 2. Handle new image upload
    if image_file:
        if old_image_path and os.path.exists(old_image_path):
            os.remove(old_image_path)
            
        ext = os.path.splitext(image_file.name)[1]
        filename = f"{datetime.utcnow().timestamp():.0f}{ext}"
        save_path = os.path.join('images', filename)
        with open(save_path, 'wb') as f:
            f.write(image_file.read())
        img_path_update = save_path
    
    # 3. Update fields
    with db_conn.session as s:
        s.execute(text(f"""
            UPDATE {TABLE_PRODUCTS}
            SET name = :name, price = :price, cost_price = :cost_price, 
                notes = :notes, image_path = :image_path
            WHERE id = :id
        """), {
            'name': name,
            'price': float(price),
            'cost_price': float(cost_price),
            'notes': notes,
            'image_path': img_path_update,
            'id': product_id
        })
        s.commit()
    
    clear_data_cache()
    return product_id, name

def add_stock_movement(product_id, change, reason='manual', skip_product_update=False):
    
    products_df = load_products()
    p = products_df[products_df['id'] == product_id]
    
    if p.empty:
        raise ValueError(f"Sản phẩm id={product_id} không tồn tại")

    current_stock = p['stock'].iloc[0]
    new_stock = current_stock + change
    
    with db_conn.session as s:
        # 1. Cập nhật tồn kho (Nếu không bị skip)
        if not skip_product_update:
            s.execute(text(f"""
                UPDATE {TABLE_PRODUCTS}
                SET stock = :new_stock
                WHERE id = :id
            """), {'new_stock': int(new_stock), 'id': product_id})

        # 2. Thêm movement
        new_movement_id = str(uuid.uuid4())
        s.execute(text(f"""
            INSERT INTO {TABLE_STOCK_MOVEMENTS} (id, product_id, "change", reason, timestamp)
            VALUES (:id, :product_id, :change, :reason, :timestamp)
        """), {
            'id': new_movement_id,
            'product_id': product_id,
            'change': int(change),
            'reason': reason,
            'timestamp': datetime.utcnow()
        })
        s.commit()
    
    clear_data_cache()
    return new_movement_id


def create_order(items):
    
    products_df = load_products()
    total = 0.0
    
    # 1. Kiểm tra tồn kho và lấy giá (chỉ cần đọc)
    for it in items:
        product_id = it['product_id']
        qty = it['qty']
        
        p = products_df[products_df['id'] == product_id]
        if p.empty:
            raise ValueError(f"Sản phẩm id={product_id} không tồn tại")
        
        p_name = p['name'].iloc[0]
        p_stock = p['stock'].iloc[0]
        
        if p_stock < qty:
            raise ValueError(f"Không đủ tồn cho **{p_name}** (còn **{p_stock}**, cần **{qty}**)")

    # 2. Tạo Order Header
    new_order_id = str(uuid.uuid4())
    order_created_at = datetime.utcnow()
    
    with db_conn.session as s:
        
        # 3. Xử lý items, cập nhật tồn kho và tạo movement (Transaction)
        for it in items:
            product_id = it['product_id']
            qty = it['qty']
            
            p = products_df[products_df['id'] == product_id].iloc[0]
            
            item_price = p['price']
            item_cost_price = p['cost_price']

            # Cập nhật tồn kho (TRỰC TIẾP trong DB)
            s.execute(text(f"""
                UPDATE {TABLE_PRODUCTS}
                SET stock = stock - :qty
                WHERE id = :product_id
            """), {'qty': int(qty), 'product_id': product_id})

            # Tạo Order Item
            new_item_id = str(uuid.uuid4())
            s.execute(text(f"""
                INSERT INTO {TABLE_ORDER_ITEMS} (id, order_id, product_id, qty, price, cost_price)
                VALUES (:id, :order_id, :product_id, :qty, :price, :cost_price)
            """), {
                'id': new_item_id,
                'order_id': new_order_id,
                'product_id': product_id,
                'qty': int(qty),
                'price': float(item_price),
                'cost_price': float(item_cost_price)
            })
            
            total += item_price * qty
            
            # Tạo Stock Movement
            new_movement_id = str(uuid.uuid4())
            s.execute(text(f"""
                INSERT INTO {TABLE_STOCK_MOVEMENTS} (id, product_id, "change", reason, timestamp)
                VALUES (:id, :product_id, :change, :reason, :timestamp)
            """), {
                'id': new_movement_id,
                'product_id': product_id,
                'change': -int(qty),
                'reason': 'Sale',
                'timestamp': order_created_at
            })

        # 4. Thêm Order Header
        s.execute(text(f"""
            INSERT INTO {TABLE_ORDERS} (id, created_at, total)
            VALUES (:id, :created_at, :total)
        """), {
            'id': new_order_id,
            'created_at': order_created_at,
            'total': float(total)
        })
        
        s.commit() # Commit tất cả các thay đổi cùng một lúc

    clear_data_cache()
    return new_order_id, total


# ---------- Streamlit UI ----------
st.set_page_config(page_title='Shop Manager', layout='wide')
st.title('👗 Shop Manager - Persistent Version (PostgreSQL)')

# Khởi tạo DB nếu cần (tạo bảng)
initialize_database()

menu = st.sidebar.selectbox('Chức năng', ['Dashboard', 'Sản phẩm', 'Đơn hàng (POS)', 'Nhập kho', 'Thống kê & Báo cáo', 'Xuất dữ liệu'])

# --- Dashboard & Sản phẩm & Đơn hàng (POS) & Nhập kho ---

if menu == 'Dashboard':
    st.header('📈 Dashboard')
    products_df = load_products()
    orders_df = load_orders()
    
    total_products = len(products_df)
    total_orders = len(orders_df)
    total_stock = products_df['stock'].sum() if not products_df.empty else 0
    
    col1, col2, col3 = st.columns(3)
    col1.metric('Tổng sản phẩm', total_products)
    col2.metric('Tổng đơn hàng', total_orders)
    col3.metric('Tổng tồn kho', total_stock)
    
    st.caption('Dữ liệu được làm mới sau mỗi thao tác thêm/sửa/tạo đơn. (Dữ liệu được lưu trên PostgreSQL)')

elif menu == 'Sản phẩm':
    st.header('📦 Quản lý sản phẩm')

    if 'editing_product_id' not in st.session_state:
        st.session_state.editing_product_id = None
        
    products_df = load_products()
    
    # Tìm sản phẩm cần chỉnh sửa
    if st.session_state.editing_product_id:
        p_to_edit_series = products_df[products_df['id'] == st.session_state.editing_product_id].iloc[0]
        p_to_edit = p_to_edit_series.to_dict()
    else:
        p_to_edit = None

    # --- CHỈNH SỬA SẢN PHẨM ---
    if p_to_edit and st.session_state.editing_product_id is not None:
        
        st.subheader(f"✏️ Chỉnh sửa sản phẩm: **{p_to_edit['name']}** (ID: {p_to_edit['id']})")
        
        with st.form('edit_product_form', clear_on_submit=False):
            name = st.text_input('Tên sản phẩm', value=p_to_edit['name'], key='edit_name')
            
            col_p, col_c = st.columns(2)
            with col_p:
                price = st.number_input('Giá Bán', value=float(p_to_edit['price']), step=1000.0, key='edit_price')
            with col_c:
                cost_price = st.number_input('Giá Nhập (Cost Price)', value=float(p_to_edit['cost_price']), step=1000.0, key='edit_cost_price')
                
            notes = st.text_area('Ghi chú', value=p_to_edit['notes'], key='edit_notes')
            
            # Image Handling
            st.markdown('**Quản lý Ảnh**')
            current_image = p_to_edit.get('image_path')
            
            if current_image and os.path.exists(current_image):
                st.image(current_image, width=100)
                remove_image = st.checkbox('Xóa ảnh hiện tại?', key='remove_img')
            else:
                remove_image = False
            
            new_image_file = st.file_uploader('Tải lên ảnh MỚI (sẽ thay thế ảnh cũ)', type=['jpg','jpeg','png'], key='new_image_file')

            col_btn1, col_btn2 = st.columns([1, 4])
            with col_btn1:
                submitted = st.form_submit_button('💾 Lưu thay đổi', type='primary')
            with col_btn2:
                if st.form_submit_button('❌ Hủy bỏ'):
                    st.session_state.editing_product_id = None
                    st.rerun() 
            
            if submitted:
                if not name:
                    st.error('Cần tên sản phẩm')
                elif price < cost_price:
                    st.error('Giá bán phải lớn hơn hoặc bằng Giá nhập.')
                else:
                    try:
                        product_id, product_name = update_product(
                            p_to_edit['id'], 
                            name, 
                            price, 
                            cost_price, 
                            notes, 
                            new_image_file, 
                            remove_image
                        )
                        st.session_state.editing_product_id = None
                        st.success(f'✅ Đã cập nhật **{product_name}** | ID: **{product_id}**')
                        st.rerun() 
                        
                    except Exception as e:
                        st.error(f"Lỗi khi cập nhật sản phẩm: {e}")
        
        st.markdown('---')
    
    # --- THÊM SẢN PHẨM MỚI ---
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
                        
                        st.success(f'✅ Đã thêm **{product_name}** | ID: **{product_id}**')
                        st.rerun() 
                        
                    except Exception as e:
                        st.error(f"Lỗi khi thêm sản phẩm: {e}")

    # --- DANH SÁCH SẢN PHẨM ---
    products_df = load_products()
    st.subheader('Danh sách sản phẩm hiện tại')
    
    if not products_df.empty:
        
        header_cols = st.columns([1, 1, 2, 2, 1, 1, 1]) 
        header_cols[0].markdown('**Ảnh**')
        header_cols[1].markdown('**ID**')
        header_cols[2].markdown('**Tên sản phẩm**')
        header_cols[3].markdown('**Giá (Bán/Nhập)**')
        header_cols[4].markdown('**Tồn kho**')
        header_cols[5].markdown('**Ghi chú**')
        header_cols[6].markdown('**Thao tác**')

        st.markdown('---') 
        
        for index, p in products_df.iterrows():
            cols = st.columns([1, 1, 2, 2, 1, 1, 1])
            
            with cols[0]:
                if p.get('image_path') and os.path.exists(p['image_path']):
                    st.image(p['image_path'], width=60)
                else:
                    st.write('🖼️')
                    
            cols[1].write(p['id'][:4] + '...') # Hiển thị ID rút gọn
            cols[2].write(p['name'])
            
            cols[3].markdown(f"**Bán:** {p['price']:,.0f} VND <br> **Nhập:** {p['cost_price']:,.0f} VND", unsafe_allow_html=True)
            
            stock_display = f"**{p['stock']}**" if p['stock'] > 10 else f"**:red[{p['stock']}]**"
            cols[4].markdown(stock_display)

            cols[5].write(p['notes'][:30] + '...' if len(str(p['notes'])) > 30 else p['notes'])
            
            # Nút Sửa
            with cols[6]:
                st.button(
                    '✏️ Sửa', 
                    key=f'edit_btn_{p["id"]}', 
                    on_click=lambda pid=p['id']: st.session_state.update(editing_product_id=pid), 
                    disabled=(st.session_state.editing_product_id is not None)
                )

            st.markdown('---') 

    else:
        st.info('Chưa có sản phẩm nào được thêm.')

elif menu == 'Đơn hàng (POS)':
    st.header('🛒 POS - Tạo đơn bán')
    st.markdown('***(Chức năng dành cho nhân viên cửa hàng)***')
    products_df = load_products() 
    active_products = products_df[products_df['stock'] > 0]
    
    status_placeholder = st.empty() 

    if active_products.empty:
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

        for index, p in active_products.iterrows():
            c = st.columns([1, 1, 3, 1, 1])
            
            with c[0]:
                if p.get('image_path') and os.path.exists(p['image_path']):
                    st.image(p['image_path'], width=60)
                else:
                    st.write('🖼️')
                    
            c[1].write(p['id'][:4] + '...')
            c[2].write(f"{p['name']} (Bán: {p['price']:,.0f} VND)")
            
            stock_display = f"**{p['stock']}**" if p['stock'] > 10 else f"**:red[{p['stock']}]**"
            c[3].markdown(stock_display)
            
            qty = c[4].number_input(
                'SL', 
                min_value=0, 
                max_value=p['stock'], 
                value=0, 
                key=f"qty_pos_{p['id']}", 
                label_visibility="collapsed"
            )
            
            if qty > 0:
                order_items_input[p['id']] = int(qty)
                total_estimated += p['price'] * qty
            
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
                    
                    status_placeholder.success(f'🎉 Đã tạo đơn **#{order_id[:8]}** thành công! Tổng cộng: **{order_total:,.0f} VND**. (Dữ liệu được lưu vĩnh viễn trên PostgreSQL)')
                    st.rerun() 
                    
            except ValueError as e:
                status_placeholder.error(f"❌ Lỗi tồn kho: {e}")
            except Exception as e:
                status_placeholder.error(f"❌ Lỗi hệ thống khi tạo đơn: {e}")
                
elif menu == 'Nhập kho':
    st.header('➕ Nhập/Xuất kho (Stock Movement)')
    products_df = load_products()
    
    if products_df.empty:
        st.warning('Vui lòng thêm sản phẩm trước khi nhập kho.')
        
    with st.form('stock_adjustment'):
        
        product_options = {row['id']: f"{row['name']} (Tồn: {row['stock']})" for index, row in products_df.iterrows()}
        
        selected_option = st.selectbox('Chọn sản phẩm', options=list(product_options.values()) if product_options else [])
        
        selected_id = next((pid for pid, name_stock in product_options.items() if name_stock == selected_option), None)

        if selected_id:
            st.info(f"Sản phẩm đang chọn: **{selected_option}**")
            
            change = st.number_input('Số lượng thay đổi (+ để nhập, - để xuất/hỏng)', step=1, value=0)
            reason = st.text_area('Lý do (Nhập hàng/Kiểm kho/Hỏng hóc...)')
            
            submitted = st.form_submit_button('Cập nhật tồn kho')
            
            if submitted and change != 0:
                try:
                    m_id = add_stock_movement(selected_id, int(change), reason)
                    st.success(f'✅ Đã cập nhật **{change}** đơn vị cho sản phẩm.')
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
    
    orders_df = load_orders() 
    order_items_df = load_order_items()
    products_df = load_products()
    
    if orders_df.empty or order_items_df.empty:
        st.info('Chưa có dữ liệu đơn hàng để thống kê.')
    else:
        # Chuẩn bị dữ liệu cho thống kê (Tạo 1 DataFrame lớn)
        df_merged = pd.merge(order_items_df, orders_df[['id', 'created_at']], 
                             left_on='order_id', right_on='id', suffixes=('_item', '_order'))
        df_merged = pd.merge(df_merged, products_df[['id', 'name', 'cost_price']], 
                             left_on='product_id', right_on='id', suffixes=('_merged', '_product'))
        
        # Đổi tên cột
        df_merged.rename(columns={'id_order': 'Order ID', 'created_at': 'Ngày tạo', 'name': 'Tên sản phẩm', 'cost_price_product': 'cost_price_product'}, inplace=True)
        
        # Tính toán
        df_merged['Ngày'] = pd.to_datetime(df_merged['Ngày tạo']).dt.date
        df_merged['Tổng tiền Bán Item'] = df_merged['qty'] * df_merged['price']
        
        # Lấy giá cost_price từ bảng order_items (đã lưu tại thời điểm bán)
        df_merged['Tổng Vốn Item'] = df_merged['qty'] * df_merged['cost_price_item']
        
        df_merged['Lợi nhuận Gộp Item'] = df_merged['Tổng tiền Bán Item'] - df_merged['Tổng Vốn Item']
        
        df_orders = df_merged.copy()

        # --- 1. Tổng quan (Trong Expander) ---
        with st.expander('📈 1. Tổng quan Doanh thu & Lợi nhuận', expanded=True):
            
            total_orders_count = df_orders['Order ID'].nunique()
            total_revenue = df_orders['Tổng tiền Bán Item'].sum()
            total_gross_profit = df_orders['Lợi nhuận Gộp Item'].sum()
            
            col_a, col_b, col_c = st.columns(3)
            col_a.metric('Tổng Doanh thu (Sales)', f"{total_revenue:,.0f} VND")
            col_b.metric('Tổng Lợi nhuận Gộp', f"{total_gross_profit:,.0f} VND", delta=f"{total_gross_profit / total_revenue * 100:.2f}%" if total_revenue > 0 else None)
            col_c.metric('Doanh thu TB/Đơn', f"{total_revenue / total_orders_count:,.0f} VND" if total_orders_count > 0 else "0 VND")

        # --- 2. Biểu đồ theo thời gian (Trong Expander) ---
        with st.expander('📅 2. Biểu đồ Doanh thu & Lợi nhuận theo thời gian'):
            
            revenue_by_date = df_orders.groupby('Ngày')['Tổng tiền Bán Item'].sum().reset_index().rename(columns={'Tổng tiền Bán Item': 'Doanh thu'})
            profit_by_date = df_orders.groupby('Ngày')['Lợi nhuận Gộp Item'].sum().reset_index().rename(columns={'Lợi nhuận Gộp Item': 'Lợi nhuận'})

            chart_data = pd.merge(revenue_by_date, profit_by_date, on='Ngày', how='outer').set_index('Ngày')
            
            st.line_chart(chart_data)
            st.dataframe(chart_data.sort_values(by='Ngày', ascending=False), use_container_width=True)

        # --- 3. Top 5 sản phẩm (Trong Expander) ---
        with st.expander('🥇 3. Top 5 sản phẩm bán chạy nhất & Lợi nhuận'):
            
            product_sales = df_orders.groupby('Tên sản phẩm').agg(
                {'qty': 'sum', 'Lợi nhuận Gộp Item': 'sum'}
            ).reset_index().rename(columns={'qty': 'Số lượng bán'})
            
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
                    items.append(f"{row['Tên sản phẩm']} x {row['qty']} ({row['price']:,.0f} VND)")
                return " | ".join(items)

            order_summary = df_orders.groupby('Order ID').agg(
                Ngày_tạo=('Ngày tạo', 'first'),
                Tổng_tiền=('Tổng tiền Bán Item', 'sum'),
                Tổng_Lợi_nhuận=('Lợi nhuận Gộp Item', 'sum')
            ).reset_index()

            details_series = df_orders.groupby('Order ID').apply(format_order_details, include_groups=False).rename('Chi tiết sản phẩm')
            
            order_summary = pd.merge(order_summary, details_series.reset_index(), on='Order ID')
            order_summary.sort_values(by='Ngày tạo', ascending=False, inplace=True)
            
            order_summary.rename(columns={
                'Order ID': 'ID',
                'Tổng_tiền': 'Tổng tiền (VND)',
                'Tổng_Lợi_nhuận': 'Lợi nhuận Gộp (VND)',
            }, inplace=True)
            
            st.dataframe(order_summary, use_container_width=True, hide_index=True)

# ----------------------------------------------------------------------
# 💾 Xuất dữ liệu (Log) 
# ----------------------------------------------------------------------

elif menu == 'Xuất dữ liệu':
    st.header('💾 Xuất Log & Báo cáo')
    st.markdown('***(Dữ liệu được tải trực tiếp từ PostgreSQL)***')
    
    st.subheader('1. Xuất Log Đơn hàng chi tiết (Orders & Items)')
    
    orders_df = load_orders()
    order_items_df = load_order_items()
    products_df = load_products()

    if not orders_df.empty and not order_items_df.empty:
        # Tái tạo lại logic merge như phần thống kê
        df_orders_export = pd.merge(order_items_df, orders_df[['id', 'created_at', 'total']], 
                                    left_on='order_id', right_on='id', suffixes=('_item', '_order'))
        df_orders_export = pd.merge(df_orders_export, products_df[['id', 'name']], 
                                    left_on='product_id', right_on='id', suffixes=('_export', '_product'))
        
        df_orders_export.rename(columns={
            'id_order': 'Order ID',
            'created_at': 'Created At',
            'id_export': 'OrderItem ID',
            'total': 'Total Order Value',
            'name': 'Product Name',
            'qty': 'Quantity',
            'price_item': 'Selling Price (per item)',
            'cost_price_item': 'Cost Price (per item)',
        }, inplace=True)
        
        df_orders_export['Gross Profit (per item)'] = df_orders_export['Selling Price (per item)'] - df_orders_export['Cost Price (per item)']
        
        cols_to_export = [
            'Order ID', 'Created At', 'OrderItem ID', 'product_id', 'Product Name', 
            'Quantity', 'Selling Price (per item)', 'Cost Price (per item)', 
            'Gross Profit (per item)', 'Total Order Value'
        ]
        
        csv_orders = df_orders_export[cols_to_export].to_csv(index=False).encode('utf-8')
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
    
    movements_df = load_stock_movements()
    
    if not movements_df.empty:
        # Cần JOIN để lấy tên sản phẩm và tồn kho hiện tại (stock)
        movements_df = pd.merge(movements_df, products_df[['id', 'name', 'stock']], 
                                    left_on='product_id', right_on='id', suffixes=('_mov', '_prod'))
        
        movements_df.rename(columns={
            'id_mov': 'Movement ID',
            'timestamp': 'Timestamp',
            'name': 'Product Name',
            'change': 'Change (+Nhập/-Xuất)',
            'stock': 'Current Stock'
        }, inplace=True)
        
        cols_to_export = ['Movement ID', 'Timestamp', 'product_id', 'Product Name', 'Change (+Nhập/-Xuất)', 'reason', 'Current Stock']
        
        csv_movements = movements_df[cols_to_export].to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Tải Log Kho (.csv)",
            data=csv_movements,
            file_name='shop_stock_movements_log.csv',
            mime='text/csv',
        )
        st.success(f"Log Kho ({len(movements_df)} dòng) đã sẵn sàng để tải xuống.")

    else:
        st.info('Không có dữ liệu thay đổi kho để xuất.')