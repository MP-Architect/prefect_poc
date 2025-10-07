from prefect import flow, task
import pandas as pd
from datetime import datetime
import os

@task
def extract_raw_data() -> pd.DataFrame:
    print("Extracting raw sales data...")
    df = pd.read_csv("data/sales.csv")
    return df

@task
def transform_sales(df: pd.DataFrame) -> pd.DataFrame:
    print("Transforming data...")
    df["sale_amount"] = df["quantity"] * df["unit_price"]
    summary = df.groupby("product_id")["sale_amount"].sum().reset_index()
    summary["processed_at"] = datetime.utcnow()
    return summary

@task
def load_to_storage(df: pd.DataFrame) -> str:
    os.makedirs("output", exist_ok=True)
    output_path = f"output/sales_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(output_path, index=False)
    print(f"Data loaded to {output_path}")
    return output_path

@flow(name="sales-etl-flow")
def sales_etl_flow():
    raw_df = extract_raw_data()
    transformed = transform_sales(raw_df)
    load_to_storage(transformed)

if __name__ == "__main__":
    sales_etl_flow.serve(
        name="sales-etl-deployment",
        cron="0 * * * *"  # Run every hour
    )
