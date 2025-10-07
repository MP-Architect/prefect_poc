from prefect import flow, task
import random

@task
def get_customer_ids() -> list[str]:
    # Fetch customer IDs from a database or API
    return [f"customer{n}" for n in random.choices(range(100), k=10)]

@task
def process_customer(customer_id: str) -> str:
    # Process a single customer
    return f"Processed {customer_id}"

@flow
def main() -> list[str]:
    customer_ids = get_customer_ids()
    print(f"Fetched customer IDs: {customer_ids}")
    # Map the process_customer task across all customer IDs
    futures = process_customer.map(customer_ids)    
    # Resolve the futures to get actual results
    results = [future.result() for future in futures]
    print(f"Processed customer results: {results}")

    return results


if __name__ == "__main__":
    main.serve(
        name="my-first-deployment",
        cron="* * * * *"  # Run every minute
    )
