# dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import os

# --- Page Configuration ---
st.set_page_config(layout="wide", page_title="Fastener Pricing Intelligence")

# --- Constants ---
# It's good practice to define file paths at the top.
# This assumes your script is in the root and data is in a subfolder.
SCRAPING_DATA_FOLDER = 'data/scraping_data'
PRODUCTS_CSV = os.path.join(SCRAPING_DATA_FOLDER, 'products.csv')
SNAPSHOTS_CSV = os.path.join(SCRAPING_DATA_FOLDER, 'price_inventory_snapshots.csv')

# --- Data Loading ---
@st.cache_data
def load_data():
    """Load and merge data, handling potential file errors."""
    if not os.path.exists(PRODUCTS_CSV) or not os.path.exists(SNAPSHOTS_CSV):
        return None
    
    products_df = pd.read_csv(PRODUCTS_CSV)
    snapshots_df = pd.read_csv(SNAPSHOTS_CSV)
    
    # Convert date column for proper sorting and plotting
    snapshots_df['date_scraped'] = pd.to_datetime(snapshots_df['date_scraped'])
    
    # Merge the two dataframes to have all info in one place
    merged_df = pd.merge(snapshots_df, products_df, on='product_id')
    return merged_df

df = load_data()

st.title("Titanium Fastener Pricing Intelligence Engine (MVP)")

if df is None:
    st.error(f"Data not found. Ensure '{PRODUCTS_CSV}' and '{SNAPSHOTS_CSV}' exist. Run your scraping script.")
else:
    # --- Sidebar Filters ---
    st.sidebar.header("Global Filters")
    fastener_type = st.sidebar.selectbox("Select Fastener Type", df['fastener_type'].unique())
    
    # Filter dataframe based on selection
    filtered_df = df[df['fastener_type'] == fastener_type].copy()
    
    # ==================================
    # === FIX: Correct Depletion Rate Calculation ===
    # Sort by product and date to ensure correct calculation order
    filtered_df = filtered_df.sort_values(['product_id', 'date_scraped'])
    
    # Correctly calculate depletion: Previous Day's Inventory - Current Day's Inventory
    # .diff() calculates (current - previous), so we multiply by -1
    inventory_change = filtered_df.groupby('product_id')['inventory_level'].diff() * -1
    
    # Depletion is only positive changes. NaN (for the first day) and negative (restocks) become 0.
    filtered_df['daily_depletion_rate'] = inventory_change.apply(lambda x: x if pd.notna(x) and x > 0 else 0)
    # ==================================

    # --- Dashboard Tabs ---
    tab1, tab2, tab3 = st.tabs(["Price Benchmarking", "Inventory Velocity", "Market Opportunity"])

    with tab1:
        st.header(f"Price Benchmarking for {fastener_type.title()}")
        
        # Get latest snapshot for each product for accurate current pricing
        latest_df = filtered_df.loc[filtered_df.groupby('product_id')['date_scraped'].idxmax()]

        # Visualization
        fig = px.box(
            latest_df, 
            x='material', 
            y='price_per_unit', 
            color='material',
            points='all',
            hover_data=['product_id', 'manufacturer'],
            title=f"Price Per Unit Distribution by Material"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Price Gap Calculation
        try:
            titanium_price = latest_df[latest_df['material'] == 'titanium']['price_per_unit'].median()
            steel_price = latest_df[latest_df['material'].str.contains('steel', case=False)]['price_per_unit'].median()

            if pd.notna(titanium_price) and pd.notna(steel_price):
                price_gap = titanium_price - steel_price
                price_ratio = titanium_price / steel_price
                st.subheader("Titanium vs. Steel Price Gap (Median)")
                col1, col2, col3 = st.columns(3)
                col1.metric("Median Titanium Price", f"${titanium_price:,.2f}")
                col2.metric("Median Steel Price", f"${steel_price:,.2f}")
                col3.metric("Titanium Premium", f"{price_ratio:,.2f}x (${price_gap:,.2f})")
            else:
                st.warning("Could not calculate price gap. Ensure both Titanium and Steel products exist for this category.")
        except Exception as e:
            st.warning(f"An error occurred during price gap calculation: {e}")


    with tab2:
        st.header("Inventory Velocity Analysis")
        
        # Check if there are any products to select
        if filtered_df.empty:
            st.warning("No products found for the selected fastener type.")
        else:
            product_to_track = st.selectbox(
                "Select a Product to Track", 
                filtered_df['product_id'].unique()
            )
            
            product_df = filtered_df[filtered_df['product_id'] == product_to_track].copy()
            
            # --- FIX: No need to recalculate depletion rate here, it's already on the dataframe ---

            # Plotting
            col1, col2 = st.columns(2)
            
            with col1:
                fig_inv = px.line(
                    product_df, 
                    x='date_scraped', 
                    y='inventory_level', 
                    title=f"Inventory Level Over Time for {product_to_track}",
                    markers=True
                )
                st.plotly_chart(fig_inv, use_container_width=True)
                
            with col2:
                fig_dep = px.bar(
                    product_df, 
                    x='date_scraped', 
                    y='daily_depletion_rate', 
                    title=f"Daily Depletion Rate for {product_to_track}"
                )
                st.plotly_chart(fig_dep, use_container_width=True)
            st.dataframe(product_df[['date_scraped', 'price_per_unit', 'inventory_level', 'daily_depletion_rate']].sort_values('date_scraped', ascending=False))


    with tab3:
        st.header("Market Opportunity Matrix")
        st.markdown("Find high-price, high-velocity steel products that are prime candidates for titanium conversion.")
        
        # Filter for steel products only
        steel_df = filtered_df[filtered_df['material'].str.contains('steel', case=False)].copy()
        
        if steel_df.empty:
            st.warning("No steel products found for this fastener type.")
        else:
            # Calculate average price and velocity for each steel product
            steel_summary = steel_df.groupby('product_id').agg(
                avg_price=('price_per_unit', 'mean'),
                total_depletion=('daily_depletion_rate', 'sum'), # Use sum of depletion as a proxy for total velocity
                days_tracked=('date_scraped', 'nunique')
            ).reset_index()
            
            # Avoid division by zero
            steel_summary['avg_daily_depletion'] = steel_summary.apply(
                lambda row: row['total_depletion'] / row['days_tracked'] if row['days_tracked'] > 0 else 0,
                axis=1
            )
            
            if not steel_summary.empty:
                # Get median values to draw quadrant lines
                median_price = steel_summary['avg_price'].median()
                median_velocity = steel_summary['avg_daily_depletion'].median()

                fig_matrix = px.scatter(
                    steel_summary,
                    x='avg_price',
                    y='avg_daily_depletion',
                    text='product_id',
                    title="Steel Products: Price vs. Sales Velocity"
                )
                fig_matrix.add_vline(x=median_price, line_dash="dash", annotation_text="Median Price")
                fig_matrix.add_hline(y=median_velocity, line_dash="dash", annotation_text="Median Velocity")
                fig_matrix.update_traces(textposition='top center')
                st.plotly_chart(fig_matrix, use_container_width=True)
                
                st.info("**Target Quadrant (Top-Right):** Products in this area are expensive (for steel) yet still sell fast. They represent the best opportunity for a premium titanium substitute.")