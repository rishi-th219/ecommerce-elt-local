"""
Kimball Star Schema ER Diagram Generator
Generates docs/er_diagram.png using matplotlib.
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
        prefix = ""
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
    plt.title("E-Commerce Data Warehouse: Kimball Star Schema Architecture\n(Orchestrated via Airflow • Modeled via dbt • Local PostgreSQL)",
              fontsize=16, weight='bold', color="#1a252f", pad=25)

    # 1. Central Fact Table (fct_orders)
    fact_x, fact_y, fact_w, fact_h = 0.35, 0.28, 0.30, 0.44
    fact_rows = [
        "[PK] fct_order_item_key",
        "     order_item_id (NK)",
        "     order_id (Degenerate)",
        "[FK] customer_key",
        "[FK] product_key",
        "[FK] order_date_key",
        "[FK] session_id",
        "     order_timestamp",
        "     order_status",
        "     payment_method",
        "     --- Measures ---",
        "     quantity",
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
        "     country",
        "     --- SCD2 Tracking ---",
        "     valid_from",
        "     valid_to",
        "     is_current (Boolean)"
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
        "     is_weekend (Boolean)"
    ]
    draw_table(ax, date_x, date_y, date_w, date_h,
               "marts.dim_date (Role-Playing Dim)", date_rows,
               header_color="#d35400", body_color="#fef5e7", border_color="#a04000")

    # 5. int_sessions - Bottom Left
    sess_x, sess_y, sess_w, sess_h = 0.04, 0.06, 0.26, 0.38
    sess_rows = [
        "[PK] session_id",
        "[FK] customer_id",
        "     session_start",
        "     session_end",
        "     duration_seconds",
        "     duration_minutes",
        "     device_type",
        "     traffic_source",
        "     page_views_count",
        "     is_converted",
        "     session_revenue"
    ]
    draw_table(ax, sess_x, sess_y, sess_w, sess_h,
               "intermediate.int_sessions", sess_rows,
               header_color="#34495e", body_color="#f2f4f4", border_color="#2c3e50")

    # Draw Relationship Connectors
    # dim_customers -> fct_orders
    draw_arrow(ax, (cust_x + cust_w, cust_y + 0.18), (fact_x, fact_y + 0.32), "1 : N (SCD2 Point-in-Time)")

    # dim_products -> fct_orders
    draw_arrow(ax, (prod_x, prod_y + 0.18), (fact_x + fact_w, fact_y + 0.32), "1 : N")

    # dim_date -> fct_orders
    draw_arrow(ax, (date_x, date_y + 0.25), (fact_x + fact_w, fact_y + 0.12), "1 : N")

    # int_sessions -> fct_orders
    draw_arrow(ax, (sess_x + sess_w, sess_y + 0.25), (fact_x, fact_y + 0.12), "1 : N (Attribution)")

    # Legend / Key
    ax.text(0.04, 0.96, "● [PK] Primary Key   ● [SK] Surrogate Key   ● [FK] Foreign Key   ● SCD2 Slowly Changing Dim",
            fontsize=10, weight='bold', color="#333", bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="#bdc3c7"))

    plt.tight_layout()
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"ER diagram generated at: {output_path}")

if __name__ == "__main__":
    generate_er_diagram()
