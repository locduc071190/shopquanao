import streamlit as st
import pandas as pd
from datetime import datetime
import os
import uuid
import numpy as np 
from io import BytesIO

# --- Sheet Names and Constants ---
SHEET_PRODUCTS = 'products'
SHEET_ORDERS = 'orders'
SHEET_ORDER_ITEMS = 'order_items'
SHEET_STOCK_MOVEMENTS = 'stock_movements'

# ---------- GOOGLE SHEETS CONNECTION & DATA LOADING ----------

# NEW: Kết nối Google Sheets, sử dụng st.cache_resource để chỉ kết nối 1 lần
@st.cache_resource(ttl=3600)
def get_gheets_connection():
    # Sử dụng st.secrets["spreadsheet_url"] đã cấu hình trong .streamlit/secrets.toml
    if "spreadsheet_url" not in st.secrets:
        st.error("Lỗi: Không tìm thấy 'spreadsheet_url' trong file .streamlit/secrets.toml. Vui lòng kiểm tra lại cấu hình.")
        st.stop()
    
    try:
        # Sử dụng API của Streamlit để kết nối Google Sheets
        # LƯU Ý: Phải đảm bảo requirements.txt có thư viện cần thiết (st-gsheets-connection/streamlit-gsheets/gspread/protobuf)
        conn = st.connection("gheets", type="google_sheets", url=st.secrets["spreadsheet_url"])
        return conn
    except Exception as e:
        st.error(f"Lỗi kết nối Google Sheets. Vui lòng kiểm tra file secrets.toml và quyền chia sẻ Service Account. Lỗi chi tiết: {e}")
        st.stop()

db_conn = get_gheets_connection()

# NEW: Tải dữ liệu từ một Sheet
@st.cache_data(ttl=5) # Cache 5 giây để tránh quá tải API
def load_data(sheet_name):
    try:
        # Đọc dữ liệu, bỏ qua các cột trống
        df = db_conn.read(worksheet=sheet_name)
        
        # Bổ sung các cột bị thiếu nếu sheet trống (đảm bảo cấu trúc)
        required_cols = {
            SHEET_PRODUCTS: ['id', 'name', 'price', 'cost_price', 'stock', 'image_path', 'notes'],
            SHEET_ORDERS: ['id', 'created_at', 'total'],
            SHEET_ORDER_ITEMS: ['id', 'order_id', 'product_id', 'qty', 'price', 'cost_price'],
            SHEET_STOCK_MOVEMENTS: ['id', 'product_id', 'change', 'reason', 'timestamp']
        }.get(sheet_name, [])
        
        for col in required_cols:
            if col not in df.columns:
                df[col] = pd.NA

        # Ép kiểu dữ liệu (đảm bảo tính toán chính xác)
        if sheet_name == SHEET_PRODUCTS:
            df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(float)
            df['cost_price'] = pd.to_numeric(df['cost_price'], errors='coerce').fillna(0).astype(float)
            df['stock'] = pd.to_numeric(df['stock'], errors='coerce').fillna(0).astype(int)
        
        if sheet_name == SHEET_ORDERS:
            df['total'] = pd.to_numeric(df['total'], errors='coerce').fillna(0).astype(float)

        if sheet_name == SHEET_ORDER_ITEMS:
            df['qty'] = pd.to_numeric(df['qty'], errors='coerce').fillna(0).astype(int)
            df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0).astype(float)
            df['cost_price'] = pd.to_numeric(df['cost_price'], errors='coerce').fillna(0).astype(float)

        return df[required_cols] # Trả về đúng thứ tự cột

    except Exception as e:
        st.warning(f"Chưa có dữ liệu trong Sheet '{sheet_name}' hoặc lỗi đọc: {e}. Tạo DataFrame trống.")
        empty_df = pd.DataFrame(columns=required_cols)
        return empty_df

# NEW: Hàm ghi dữ liệu (Viết lại toàn bộ Sheet)
def write_data(df, sheet_name):
    # Sử dụng lock file để tránh race condition khi nhiều người ghi cùng lúc
    df.fillna('', inplace=True) # Thay thế NaN bằng chuỗi rỗng trước khi ghi
    db_conn.write(df, worksheet=sheet_name)
    load_data.clear() # Xóa cache sau khi ghi thành công

# NEW: Các hàm tải dữ liệu cụ thể
@st.cache_data(ttl=5)
def load_products():
    return load_data(SHEET_PRODUCTS)

@st.cache_data(ttl=5)
def load_orders():
    return load_data(SHEET_ORDERS)

@st.cache_data(ttl=5)
def load_order_items():
    return load_data(SHEET_ORDER_ITEMS)

@st.cache_data(ttl=5)
def load_stock_movements():
    return load_data(SHEET_STOCK_MOVEMENTS)

def clear_data_cache():
    """Xóa cache sau khi thực hiện thay đổi vào DB."""
    load_data.clear() 

# ---------- Database Helper Functions (OVERHAULED) ----------

def add_product(name, price, cost_price, stock, notes='', image_file=None):
    
    # 1. Xử lý ảnh (giữ nguyên logic file system)
    img_path = ''
    if image_file:
        ext = os.path.splitext(image_file.name)[1]
        filename = f"{datetime.utcnow().timestamp():.0f}{ext}"
        save_path = os.path.join('images', filename)
        with open(save_path, 'wb') as f:
            f.write(image_file.read())
        img_path = save_path
    
    # 2. Tạo record mới
    df_products = load_products()
    new_product_id = str(uuid.uuid4())
    
    new_row = pd.DataFrame([{
        'id': new_product_id,
        'name': name,
        'price': price,
        'cost_price': cost_price,
        'stock': stock,
        'image_path': img_path,
        'notes': notes
    }])
    
    df_products = pd.concat([df_products, new_row], ignore_index=True)
    
    # 3. Ghi lại products và thêm movement
    write_data(df_products, SHEET_PRODUCTS)
    add_stock_movement(new_product_id, stock, 'Initial / Import', skip_product_update=True)
    
    return new_product_id, name

def update_product(product_id, name, price, cost_price, notes, image_file=None, remove_image=False):
    
    df_products = load_products()
    idx = df_products[df_products['id'] == product_id].index
    
    if idx.empty:
        raise ValueError(f"Sản phẩm id={product_id} không tồn tại")
    
    # Lấy đường dẫn ảnh cũ
    p = df_products.loc[idx[0]]
    old_image_path = p['image_path'] if pd.notna(p['image_path']) else ''

    # 1. Handle image removal
    if remove_image and old_image_path and os.path.exists(old_image_path):
        os.remove(old_image_path)
        df_products.loc[idx, 'image_path'] = ''
        old_image_path = '' # Đánh dấu đã xóa

    # 2. Handle new image upload
    if image_file:
        # Delete old image if it exists and hasn't been removed yet
        if old_image_path and os.path.exists(old_image_path):
            os.remove(old_image_path)
            
        ext = os.path.splitext(image_file.name)[1]
        filename = f"{datetime.utcnow().timestamp():.0f}{ext}"
        save_path = os.path.join('images', filename)
        with open(save_path, 'wb') as f:
            f.write(image_file.read())
        df_products.loc[idx, 'image_path'] = save_path
    
    # 3. Update fields
    df_products.loc[idx, 'name'] = name
    df_products.loc[idx, 'price'] = price
    df_products.loc[idx, 'cost_price'] = cost_price
    df_products.loc[idx, 'notes'] = notes
            
    write_data(df_products, SHEET_PRODUCTS)
    return product_id, name

def add_stock_movement(product_id, change, reason='manual', skip_product_update=False):
    
    df_products = load_products()
    df_movements = load_stock_movements()
    
    idx = df_products[df_products['id'] == product_id].index
    
    if idx.empty:
        raise ValueError(f"Sản phẩm id={product_id} không tồn tại")

    # 1. Cập nhật tồn kho (Nếu không bị skip)
    if not skip_product_update:
        current_stock = df_products.loc[idx, 'stock'].iloc[0]
        new_stock = current_stock + change
        df_products.loc[idx, 'stock'] = new_stock
        write_data(df_products, SHEET_PRODUCTS) # Ghi lại products

    # 2. Thêm movement
    new_movement_id = str(uuid.uuid4())
    new_row = pd.DataFrame([{
        'id': new_movement_id,
        'product_id': product_id,
        'change': change,
        'reason': reason,
        'timestamp': datetime.utcnow().isoformat()
    }])
    df_movements = pd.concat([df_movements, new_row], ignore_index=True)
    write_data(df_movements, SHEET_STOCK_MOVEMENTS) # Ghi lại movements
    
    return new_movement_id

def create_order(items):
    
    df_products = load_products()
    df_orders = load_orders()
    df_order_items = load_order_items()
    df_movements = load_stock_movements()
    
    total = 0.0
    
    # 1. Kiểm tra tồn kho và lấy giá
    for it in items:
        product_id = it['product_id']
        qty = it['qty']
        
        p = df_products[df_products['id'] == product_id]
        if p.empty:
            raise ValueError(f"Sản phẩm id={product_id} không tồn tại")
        
        p_name = p['name'].iloc[0]
        p_stock = p['stock'].iloc[0]
        
        if p_stock < qty:
            raise ValueError(f"Không đủ tồn cho **{p_name}** (còn **{p_stock}**, cần **{qty}**)")

    # 2. Tạo Order Header
    new_order_id = str(uuid.uuid4())
    order_created_at = datetime.utcnow().isoformat()
    
    # 3. Xử lý items, cập nhật tồn kho và tạo movement
    order_items_rows = []
    movement_rows = []
    
    for it in items:
        product_id = it['product_id']
        qty = it['qty']
        
        idx = df_products[df_products['id'] == product_id].index
        p = df_products.loc[idx[0]]
        
        # Cập nhật tồn kho
        df_products.loc[idx, 'stock'] -= qty
        
        # Tạo Order Item
        new_item_id = str(uuid.uuid4())
        item_price = p['price']
        item_cost_price = p['cost_price']

        order_items_rows.append({
            'id': new_item_id,
            'order_id': new_order_id,
            'product_id': product_id,
            'qty': qty,
            'price': item_price,
            'cost_price': item_cost_price
        })
        
        total += item_price * qty
        
        # Tạo Stock Movement
        new_movement_id = str(uuid.uuid4())
        movement_rows.append({
            'id': new_movement_id,
            'product_id': product_id,
            'change': -qty,
            'reason': 'Sale',
            'timestamp': order_created_at
        })

    # 4. Thêm Order Header vào DataFrame
    new_order_row = pd.DataFrame([{
        'id': new_order_id,
        'created_at': order_created_at,
        'total': total
    }])
    df_orders = pd.concat([df_orders, new_order_row], ignore_index=True)
    
    # 5. Thêm Order Items và Stock Movements vào DataFrames
    df_order_items = pd.concat([df_order_items, pd.DataFrame(order_items_rows)], ignore_index=True)
    df_movements = pd.concat([df_movements, pd.DataFrame(movement_rows)], ignore_index=True)

    # 6. Ghi lại tất cả DataFrames đã thay đổi
    write_data(df_products, SHEET_PRODUCTS)
    write_data(df_orders, SHEET_ORDERS)
    write_data(df_order_items, SHEET_ORDER_ITEMS)
    write_data(df_movements, SHEET_STOCK_MOVEMENTS)

    return new_order_id, total


# ---------- Streamlit UI ----------
st.set_page_config(page_title='Shop Manager', layout='wide')
st.title('👗 Shop Manager - Persistent Version (Google Sheets)')

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
    
    st.caption('Dữ liệu được làm mới sau mỗi thao tác thêm/sửa/tạo đơn. (Dữ liệu được lưu trên Google Sheets)')

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
                        clear_data_cache()
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
                        clear_data_cache()
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
                    
                    status_placeholder.success(f'🎉 Đã tạo đơn **#{order_id[:8]}** thành công! Tổng cộng: **{order_total:,.0f} VND**. (Dữ liệu được lưu vĩnh viễn trên Google Sheets)')
                    clear_data_cache()
                    
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
    
    orders_df = load_orders() 
    order_items_df = load_order_items()
    products_df = load_products()
    
    if orders_df.empty or order_items_df.empty:
        st.info('Chưa có dữ liệu đơn hàng để thống kê.')
    else:
        # Chuẩn bị dữ liệu cho thống kê (Tạo 1 DataFrame lớn)
        df_merged = pd.merge(order_items_df, orders_df[['id', 'created_at']], 
                             left_on='order_id', right_on='id', suffixes=('_item', '_order'))
        df_merged = pd.merge(df_merged, products_df[['id', 'name']], 
                             left_on='product_id', right_on='id', suffixes=('_merged', '_product'))
        
        # Đổi tên cột
        df_merged.rename(columns={'id_order': 'Order ID', 'created_at': 'Ngày tạo', 'name': 'Tên sản phẩm'}, inplace=True)
        
        # Tính toán
        df_merged['Ngày'] = pd.to_datetime(df_merged['Ngày tạo']).dt.date
        df_merged['Tháng'] = pd.to_datetime(df_merged['Ngày tạo']).dt.strftime('%Y-%m')
        df_merged['Tổng tiền Bán Item'] = df_merged['qty'] * df_merged['price']
        df_merged['Tổng Vốn Item'] = df_merged['qty'] * df_merged['cost_price']
        df_merged['Lợi nhuận Gộp Item'] = df_merged['Tổng tiền Bán Item'] - df_merged['Tổng Vốn Item']
        
        df_orders = df_merged.copy()

        # --- 1. Tổng quan (Trong Expander) ---
        with st.expander('📈 1. Tổng quan Doanh thu & Lợi nhuận', expanded=True):
            
            total_orders_count = df_orders['Order ID'].nunique()
            total_revenue = df_orders.groupby('Order ID')['Tổng tiền Bán Item'].sum().sum()
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

            details_series = df_orders.groupby('Order ID').apply(format_order_details).rename('Chi tiết sản phẩm')
            
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
    st.markdown('***(Dữ liệu được tải trực tiếp từ Google Sheets)***')
    
    st.subheader('1. Xuất Log Đơn hàng chi tiết (Orders & Items)')
    
    orders_df = load_orders()
    order_items_df = load_order_items()
    products_df = load_products()

    if not orders_df.empty and not order_items_df.empty:
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
        
        # Lọc và tính toán lại cho chắc chắn
        df_orders_export['Gross Profit (per item)'] = df_orders_export['Selling Price (per item)'] - df_orders_export['Cost Price (per item)']
        
        cols_to_export = [
            'Order ID', 'Created At', 'OrderItem ID', 'Product ID', 'Product Name', 
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
        df_movements = pd.merge(movements_df, products_df[['id', 'name', 'stock']], 
                                    left_on='product_id', right_on='id', suffixes=('_mov', '_prod'))
        
        df_movements.rename(columns={
            'id_mov': 'Movement ID',
            'timestamp': 'Timestamp',
            'name': 'Product Name',
            'change': 'Change (+Nhập/-Xuất)',
            'stock': 'Current Stock'
        }, inplace=True)
        
        cols_to_export = ['Movement ID', 'Timestamp', 'Product ID', 'Product Name', 'Change (+Nhập/-Xuất)', 'Reason', 'Current Stock']
        
        csv_movements = df_movements[cols_to_export].to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Tải Log Kho (.csv)",
            data=csv_movements,
            file_name='shop_stock_movements_log.csv',
            mime='text/csv',
        )
        st.success(f"Log Kho ({len(df_movements)} dòng) đã sẵn sàng để tải xuống.")

    else:
        st.info('Không có dữ liệu thay đổi kho để xuất.')