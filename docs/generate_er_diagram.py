"""
Kimball Star Schema ER Diagram Generator

Regenerates docs/er_diagram.png from the model definitions in dbt/models/marts.
Run from the repository root after changing the star schema:

    python docs/generate_er_diagram.py
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches

def draw_table(ax, x, y, width, height, title, rows, header_color="#2c3e50", body_color="#ecf0f1", border_color="#34495e"):
    # Outer box
    rect = patches.FancyBboxPatch(
        (x, y), width, height,
        boxstyle="round,pad=0.02,rounding_size=0.03",
        linewidth=2, edgecolor=border_color, facecolor=body_color
    )
    ax.add_patch(rect)

    # Header box
    header_height = 0.08 * (height / 0.5)
    header_rect = patches.FancyBboxPatch(
        (x, y + height - header_height), width, header_height,
        boxstyle="round,pad=0.02,rounding_size=0.03",
        linewidth=2, edgecolor=border_color, facecolor=header_color
    )
    ax.add_patch(header_rect)

    # Title text
    ax.text(x + width / 2, y + height - header_height / 2, title,
            ha='center', va='center', color='white', weight='bold', fontsize=11)

    # Rows text
    row_y = y + height - header_height - 0.04
    line_spacing = (height - header_height - 0.05) / max(len(rows), 1)
    
    for row in rows:
        font_weight = "normal"
        text_color = "#2c3e50"
        
        if row.startswith("[PK]") or row.startswith("[SK]"):
            font_weight = "bold"
            text_color = "#c0392b"
        elif row.startswith("[FK]"):
            font_weight = "bold"
            text_color = "#2980b9"
        elif "SCD2" in row:
            font_weight = "bold"
            text_color = "#8e44ad"

        ax.text(x + 0.03, row_y, row, ha='left', va='center',
                fontsize=9, weight=font_weight, color=text_color, fontfamily='monospace')
        row_y -= line_spacing

def draw_arrow(ax, start, end, label=""):
    ax.annotate(
        "", xy=end, xytext=start,
        arrowprops=dict(arrowstyle="-|>", color="#7f8c8d", lw=2, mutation_scale=15)
    )
    if label:
        mid_x = (start[0] + end[0]) / 2
        mid_y = (start[1] + end[1]) / 2
        ax.text(mid_x, mid_y + 0.02, label, ha='center', va='bottom',
                fontsize=8, color="#555", weight='bold', backgroundcolor='white')

def generate_er_diagram(output_path="docs/er_diagram.png"):
    fig, ax = plt.subplots(figsize=(16, 11), dpi=300)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis('off')

    # Background color
    fig.patch.set_facecolor('#f8f9fa')
    ax.set_facecolor('#f8f9fa')

    # Title
    plt.title("E-Commerce Data Warehouse: Kimball Star Schema Architecture\n(Orchestrated via Airflow • Modeled via dbt • Microsoft SQL Server 2025 / PostgreSQL 16)",
              fontsize=16, weight='bold', color="#1a252f", pad=25)

    # 1. Central Fact Table (fct_orders)
    fact_x, fact_y, fact_w, fact_h = 0.35, 0.28, 0.30, 0.44
    fact_rows = [
        "[PK] fct_order_item_key",
        "     order_item_id (NK)",
        "     order_id (Degenerate)",
        "     session_id (Degenerate)",
        "[FK] customer_key",
        "[FK] product_key",
        "[FK] order_date_key",
        "     order_timestamp",
        "     order_status",
        "     payment_method",
        "     --- Measures ---",
        "     quantity",
        "     unit_price",
        "     unit_cost",
        "     gross_amount",
        "     discount_amount",
        "     net_revenue",
        "     cogs",
        "     gross_profit",
        "     profit_margin_pct"
    ]
    draw_table(ax, fact_x, fact_y, fact_w, fact_h,
               "marts.fct_orders (Fact Table)", fact_rows,
               header_color="#16a085", body_color="#e8f8f5", border_color="#117864")

    # 2. dim_customers (SCD Type 2) - Top Left
    cust_x, cust_y, cust_w, cust_h = 0.04, 0.52, 0.26, 0.38
    cust_rows = [
        "[SK] customer_key",
        "     customer_id (NK)",
        "     first_name",
        "     last_name",
        "     email",
        "     city",
        "     state (Historical)",
        "     postal_code",
        "     country",
        "     created_at",
        "     --- SCD2 Tracking ---",
        "     valid_from",
        "     valid_to",
        "     effective_updated_at",
        "     is_current (BIT)"
    ]
    draw_table(ax, cust_x, cust_y, cust_w, cust_h,
               "marts.dim_customers (SCD Type 2)", cust_rows,
               header_color="#8e44ad", body_color="#f4ecf7", border_color="#6c3483")

    # 3. dim_products - Top Right
    prod_x, prod_y, prod_w, prod_h = 0.70, 0.52, 0.26, 0.38
    prod_rows = [
        "[SK] product_key",
        "     product_id (NK)",
        "     sku",
        "     product_name",
        "     category",
        "     unit_cost",
        "     retail_price",
        "     baseline_margin_pct",
        "     created_at"
    ]
    draw_table(ax, prod_x, prod_y, prod_w, prod_h,
               "marts.dim_products (Dimension)", prod_rows,
               header_color="#2980b9", body_color="#ebf5fb", border_color="#1f618d")

    # 4. dim_date - Bottom Center/Right
    date_x, date_y, date_w, date_h = 0.70, 0.06, 0.26, 0.38
    date_rows = [
        "[PK] date_key (YYYYMMDD)",
        "     full_date",
        "     day_of_week",
        "     day_name",
        "     day_of_month",
        "     month",
        "     month_name",
        "     quarter",
        "     fiscal_quarter",
        "     year",
        "     is_weekend (BIT)"
    ]
    draw_table(ax, date_x, date_y, date_w, date_h,
               "marts.dim_date (Role-Playing Dim)", date_rows,
               header_color="#d35400", body_color="#fef5e7", border_color="#a04000")

    # 5. fct_sessions - Bottom Left
    sess_x, sess_y, sess_w, sess_h = 0.04, 0.06, 0.26, 0.38
    sess_rows = [
        "[PK] session_key",
        "     session_id (Degenerate)",
        "[FK] customer_key",
        "[FK] session_date_key",
        "     session_start / _end",
        "     device_type",
        "     traffic_source",
        "     --- Measures ---",
        "     duration_seconds",
        "     duration_minutes",
        "     page_views_count",
        "     is_converted (BIT)",
        "     converted_order_id",
        "     order_count",
        "     session_attributed_revenue"
    ]
    draw_table(ax, sess_x, sess_y, sess_w, sess_h,
               "marts.fct_sessions (Fact Table)", sess_rows,
               header_color="#16a085", body_color="#e8f8f5", border_color="#117864")

    # Draw Relationship Connectors
    # dim_customers -> fct_orders
    draw_arrow(ax, (cust_x + cust_w, cust_y + 0.18), (fact_x, fact_y + 0.32), "1 : N (SCD2 Point-in-Time)")

    # dim_products -> fct_orders
    draw_arrow(ax, (prod_x, prod_y + 0.18), (fact_x + fact_w, fact_y + 0.32), "1 : N")

    # dim_date -> fct_orders
    draw_arrow(ax, (date_x, date_y + 0.25), (fact_x + fact_w, fact_y + 0.12), "1 : N")

    # conformed dimensions also serve the session fact
    draw_arrow(ax, (cust_x + cust_w * 0.5, cust_y), (sess_x + sess_w * 0.5, sess_y + sess_h), "1 : N (SCD2)")
    draw_arrow(ax, (date_x, date_y + 0.06), (sess_x + sess_w, sess_y + 0.06), "1 : N (Conformed)")

    # Legend / Key
    ax.text(0.04, 0.96, "● [PK] Primary Key   ● [SK] Surrogate Key   ● [FK] Foreign Key   ● SCD2 Slowly Changing Dim",
            fontsize=10, weight='bold', color="#333", bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#bdc3c7"))

    # Derived marts built on top of the star
    ax.text(0.5, -0.055,
            "Derived marts: marts.fct_daily_sales (periodic snapshot, 1 row per trading day)  •  "
            "marts.mart_customer_360 (1 row per customer, RFM segmentation)\n"
            "Every dimension carries a '-1' Unknown member so a failed key lookup is never NULL",
            ha='center', va='bottom', fontsize=9, color="#555", style='italic')

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"ER diagram generated at: {output_path}")

if __name__ == "__main__":
    generate_er_diagram()
