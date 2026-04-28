import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import date
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

# ─── PAGE CONFIG ──────────────────────────────────────────────
st.set_page_config(page_title="DHL Operations Portal", layout="wide", page_icon="📦")

# ─── CUSTOM CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #f4f5f6; }
    h1, h2, h3 { color: #d40511; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
    .stButton>button { background-color: #ffcc00; color: #d40511; font-weight: bold; border-radius: 6px; border: 1px solid #d40511; width: 100%; transition: all 0.3s ease; }
    .stButton>button:hover { background-color: #d40511; color: #ffcc00; transform: scale(1.02); }
    .metric-card { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); border-top: 5px solid #d40511; text-align: center; }
    .metric-val { font-size: 28px; font-weight: 800; color: #212529; }
    .metric-lbl { font-size: 13px; color: #6c757d; text-transform: uppercase; letter-spacing: 1px; }
    .download-btn { margin-top: 10px; }
</style>
""", unsafe_allow_html=True)

# ─── DATABASE CONNECTION ──────────────────────────────────────
DATABASE_URL = "postgresql://neondb_owner:npg_0ZedV7DiuxhP@ep-still-firefly-aofa2awl.c-2.ap-southeast-1.aws.neon.tech/neondb?sslmode=require"

@st.cache_resource
def init_connection():
    return psycopg2.connect(DATABASE_URL)

try:
    conn = init_connection()
except Exception as e:
    st.error(f"Database connection failed: {e}")
    st.stop()

# ─── HEADER ───────────────────────────────────────────────────
colA, colB = st.columns([1, 6])
colA.image("https://upload.wikimedia.org/wikipedia/commons/thumb/a/ac/DHL_Logo.svg/512px-DHL_Logo.svg.png", width=110)
with colB:
    st.title("Global Vendor & Shipment Visibility")
    st.markdown("**(Production Environment)** Connected to **Neon.tech PostgreSQL 16** via psycopg2.")

st.write("---")

# ─── TABS ─────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📊 Executive Dashboard", 
    "🛣️ Route & City Tracking", 
    "🔍 Vendor Intelligence",
    "📝 Operations (CRUD)",
    "📈 Export Center"
])

# ══════════════════════════════════════════════════════════════
# TAB 1: EXECUTIVE DASHBOARD (Visual Focus)
# ══════════════════════════════════════════════════════════════
with tab1:
    st.subheader("High-Level Operations Overview")
    
    with st.spinner('Loading live metrics...'):
        status_counts = pd.read_sql("SELECT Status, COUNT(*) as count FROM Shipment GROUP BY Status", conn)
        
        # Top Metric Cards
        c1, c2, c3, c4 = st.columns(4)
        statuses = ["In Transit", "Delivered", "Delayed", "Pending"]
        cols = [c1, c2, c3, c4]
        
        for col, stat in zip(cols, statuses):
            val_series = status_counts[status_counts['status'] == stat]['count']
            val = val_series.sum() if not val_series.empty else 0
            col.markdown(f"<div class='metric-card'><div class='metric-lbl'>{stat}</div><div class='metric-val'>{val}</div></div>", unsafe_allow_html=True)
            
        st.write("<br>", unsafe_allow_html=True)
        
        # Interactive Charts
        ch1, ch2 = st.columns(2)
        
        with ch1:
            fig_pie = px.pie(status_counts, values='count', names='status', hole=0.4, 
                             title="Shipment Status Distribution", 
                             color_discrete_sequence=['#d40511', '#ffcc00', '#212529', '#6c757d'])
            st.plotly_chart(fig_pie, use_container_width=True)
            
        with ch2:
            delay_data = pd.read_sql("SELECT s.Origin, AVG(d.DelayDuration) as AvgDelay FROM Shipment s JOIN Delivery d ON s.ShipmentID = d.ShipmentID WHERE d.DelayDuration > 0 GROUP BY s.Origin ORDER BY AvgDelay DESC LIMIT 5", conn)
            if not delay_data.empty:
                fig_bar = px.bar(delay_data, x='origin', y='avgdelay', 
                                 title="Top 5 Origins by Average Delay (Hours)",
                                 labels={'origin': 'Origin City', 'avgdelay': 'Avg Delay (Hrs)'},
                                 color_discrete_sequence=['#d40511'])
                st.plotly_chart(fig_bar, use_container_width=True)
            else:
                st.info("No delay data available to chart.")

# ══════════════════════════════════════════════════════════════
# TAB 2: ROUTE & CITY TRACKING
# ══════════════════════════════════════════════════════════════
with tab2:
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("City Arrivals Manager")
        df_cities = pd.read_sql("SELECT DISTINCT Destination FROM Shipment ORDER BY Destination", conn)
        selected_city = st.selectbox("Select Destination City:", df_cities['destination'])
        
        city_query = f"SELECT ShipmentID, Origin, ExpectedDeliveryDate, Status FROM Shipment WHERE Destination = '{selected_city}' ORDER BY ExpectedDeliveryDate"
        df_city_shipments = pd.read_sql(city_query, conn)
        st.dataframe(df_city_shipments, use_container_width=True, hide_index=True)
        
    with col2:
        st.subheader("Route Delay Analytics")
        df_routes = pd.read_sql("SELECT DISTINCT Origin, Destination FROM Shipment ORDER BY Origin", conn)
        route_list = [f"{row['origin']} ➔ {row['destination']}" for _, row in df_routes.iterrows()]
        selected_route = st.selectbox("Select Route:", route_list)
        origin, destination = selected_route.split(" ➔ ")
        
        stat_query = f"SELECT COUNT(s.ShipmentID) as TotalShipments, AVG(COALESCE(d.DelayDuration, 0)) as AvgDelay FROM Shipment s LEFT JOIN Delivery d ON s.ShipmentID = d.ShipmentID WHERE s.Origin = '{origin}' AND s.Destination = '{destination}'"
        route_stats = pd.read_sql(stat_query, conn).iloc[0]
        
        st.markdown(f"**Total Processed:** {route_stats['totalshipments']} | **Avg Delay:** {round(route_stats['avgdelay'], 1)} Hrs")
        
        with st.expander("View Raw Route Data (SQL)"):
            active_query = f"SELECT ShipmentID, DispatchDate, ExpectedDeliveryDate, Status FROM Shipment WHERE Origin = '{origin}' AND Destination = '{destination}'"
            st.dataframe(pd.read_sql(active_query, conn), use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════
# TAB 3: VENDOR INTELLIGENCE
# ══════════════════════════════════════════════════════════════
with tab3:
    st.subheader("Vendor Risk & Reliability Profile")
    
    df_vendors = pd.read_sql("SELECT VendorID, Name FROM Vendor ORDER BY Name", conn)
    vendor_dict = dict(zip(df_vendors['name'], df_vendors['vendorid']))
    selected_vendor = st.selectbox("Search Vendor Database:", list(vendor_dict.keys()))
    v_id = vendor_dict[selected_vendor]
    
    v_data = pd.read_sql(f"SELECT Region, ContactInfo, ReliabilityScore FROM Vendor WHERE VendorID = {v_id}", conn).iloc[0]
    
    c1, c2, c3 = st.columns(3)
    c1.markdown(f"<div class='metric-card'><div class='metric-lbl'>Region</div><div class='metric-val'>{v_data['region']}</div></div>", unsafe_allow_html=True)
    c2.markdown(f"<div class='metric-card'><div class='metric-lbl'>Contact</div><div class='metric-val'>{v_data['contactinfo']}</div></div>", unsafe_allow_html=True)
    c3.markdown(f"<div class='metric-card'><div class='metric-lbl'>Reliability</div><div class='metric-val'>{v_data['reliabilityscore']}/5.0</div></div>", unsafe_allow_html=True)
    
    st.write("<br>", unsafe_allow_html=True)
    st.markdown("##### Historical Shipments")
    history_query = f"SELECT s.ShipmentID, s.Origin, w.Location AS Destination, s.Status, d.DelayDuration FROM Shipment s JOIN Warehouse w ON s.DestinationWarehouseID = w.WarehouseID LEFT JOIN Delivery d ON s.ShipmentID = d.ShipmentID WHERE s.VendorID = {v_id} ORDER BY s.DispatchDate DESC;"
    st.dataframe(pd.read_sql(history_query, conn), use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════
# TAB 4: OPERATIONS (CRUD)
# ══════════════════════════════════════════════════════════════
with tab4:
    st.subheader("Secure Data Entry (Transaction Protocol)")
    st.markdown("This form executes a secure SQL Transaction with Rollback capabilities to prevent partial data writes.")
    
    with st.form("delivery_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        with col1:
            ship_id = st.number_input("Shipment ID", min_value=1, step=1)
            actual_date = st.date_input("Actual Delivery Date", date.today())
        with col2:
            delay_hrs = st.number_input("Delay Duration (Hours)", min_value=0, step=1)
            status = st.selectbox("Delivery Status", ["On-Time", "Delayed"])
        
        submit = st.form_submit_button("Execute SQL Transaction")
        
        if submit:
            with st.spinner("Writing to cloud database..."):
                try:
                    cur = conn.cursor()
                    insert_query = "INSERT INTO Delivery (ShipmentID, ActualDeliveryDate, DelayDuration, DeliveryStatus) VALUES (%s, %s, %s, %s)"
                    cur.execute(insert_query, (ship_id, actual_date, delay_hrs, status))
                    
                    update_query = "UPDATE Shipment SET Status = 'Delivered' WHERE ShipmentID = %s"
                    cur.execute(update_query, (ship_id,))
                    
                    conn.commit()
                    st.success(f"✅ Transaction Successful! Shipment {ship_id} marked as Delivered.")
                    st.balloons()
                except Exception as e:
                    conn.rollback() 
                    st.error("🚨 SQL State 23503: Foreign Key Violation Detected. Transaction Rolled Back.")
                    st.code(str(e))

# ══════════════════════════════════════════════════════════════
# TAB 5: EXPORT CENTER
# ══════════════════════════════════════════════════════════════
with tab5:
    st.subheader("Data Export & BI Reporting")
    st.markdown("Generate and download complex SQL views for offline analysis.")
    
    report_type = st.radio("Select Query Output:", ["Cargo Value per Vendor", "Inventory Forecast", "Products Never Shipped"], horizontal=True)
    
    df_report = pd.DataFrame()
    
    with st.spinner("Compiling report..."):
        if report_type == "Cargo Value per Vendor":
            q_cargo = "SELECT v.Name AS Vendor, v.Region, COUNT(DISTINCT s.ShipmentID) AS TotalShipments, SUM(si.Quantity * p.UnitCost) AS TotalCargoValue FROM Vendor v JOIN Shipment s ON v.VendorID = s.VendorID JOIN ShipmentItem si ON s.ShipmentID = si.ShipmentID JOIN Product p ON si.ProductID = p.ProductID GROUP BY v.VendorID, v.Name, v.Region ORDER BY TotalCargoValue DESC;"
            df_report = pd.read_sql(q_cargo, conn)
            
        elif report_type == "Inventory Forecast":
            q_inv = "SELECT p.Name AS Product, w.Location AS Warehouse, i.QuantityAvailable, i.ReorderLevel FROM Inventory i JOIN Product p ON i.ProductID = p.ProductID JOIN Warehouse w ON i.WarehouseID = w.WarehouseID ORDER BY i.QuantityAvailable ASC;"
            df_report = pd.read_sql(q_inv, conn)

        elif report_type == "Products Never Shipped":
            q_never = "SELECT ProductID, Name, Category FROM Product WHERE ProductID NOT IN (SELECT DISTINCT ProductID FROM ShipmentItem);"
            df_report = pd.read_sql(q_never, conn)

    # Show the table
    st.dataframe(df_report, use_container_width=True, hide_index=True)
    
    # Download Button
    csv = df_report.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Download as CSV",
        data=csv,
        file_name=f"{report_type.replace(' ', '_')}.csv",
        mime='text/csv',
    )