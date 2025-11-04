import dlt
import pyarrow as pa
import pyarrow.compute as pc
from typing import Iterator

@dlt.resource(name="customers", write_disposition="replace", primary_key="customer_id")
def stripe_customers() -> Iterator[pa.Table]:
    """Clean Stripe customers"""
    pipeline = dlt.pipeline(
        pipeline_name="stripe_raw",
        destination="duckdb",
        dataset_name="stripe_raw"
    )
    
    raw_customers = pipeline.dataset().table("customers")
    
    for batch in raw_customers.iter_arrow(chunk_size=10000):
        cols = {
            "customer_id": batch.column("id"),
            "email": pc.utf8_lower(pc.utf8_trim(batch.column("email"))),
            "name": batch.column("name") if "name" in batch.schema.names else None,
            "created": pc.cast(batch.column("created"), pa.timestamp('s')),
            "currency": batch.column("currency") if "currency" in batch.schema.names else None,
            "delinquent": pc.cast(batch.column("delinquent"), pa.bool_()) if "delinquent" in batch.schema.names else None,
            "balance": pc.cast(batch.column("balance"), pa.float64()) / 100.0 if "balance" in batch.schema.names else None
        }
        
        # Filter out None values
        cols = {k: v for k, v in cols.items() if v is not None}
        
        yield pa.table(cols)


@dlt.resource(name="subscriptions", write_disposition="replace", primary_key="subscription_id")
def stripe_subscriptions() -> Iterator[pa.Table]:
    """Clean Stripe subscriptions with MRR calculation"""
    pipeline = dlt.pipeline(
        pipeline_name="stripe_raw",
        destination="duckdb",
        dataset_name="stripe_raw"
    )
    
    raw_subs = pipeline.dataset().table("subscriptions")
    
    for batch in raw_subs.iter_arrow(chunk_size=10000):
        # Extract plan details (may be nested in items)
        plan_amount = batch.column("plan__amount") if "plan__amount" in batch.schema.names else batch.column("items__data__0__plan__amount")
        plan_interval = batch.column("plan__interval") if "plan__interval" in batch.schema.names else batch.column("items__data__0__plan__interval")
        
        cols = {
            "subscription_id": batch.column("id"),
            "customer_id": batch.column("customer"),
            "status": batch.column("status"),
            "created": pc.cast(batch.column("created"), pa.timestamp('s')),
            "current_period_start": pc.cast(batch.column("current_period_start"), pa.timestamp('s')),
            "current_period_end": pc.cast(batch.column("current_period_end"), pa.timestamp('s')),
            "cancel_at": pc.cast(batch.column("cancel_at"), pa.timestamp('s')) if "cancel_at" in batch.schema.names else None,
            "canceled_at": pc.cast(batch.column("canceled_at"), pa.timestamp('s')) if "canceled_at" in batch.schema.names else None,
            "ended_at": pc.cast(batch.column("ended_at"), pa.timestamp('s')) if "ended_at" in batch.schema.names else None,
            "trial_start": pc.cast(batch.column("trial_start"), pa.timestamp('s')) if "trial_start" in batch.schema.names else None,
            "trial_end": pc.cast(batch.column("trial_end"), pa.timestamp('s')) if "trial_end" in batch.schema.names else None,
            "plan_amount": pc.cast(plan_amount, pa.float64()) / 100.0,
            "plan_interval": plan_interval,
            "currency": batch.column("currency") if "currency" in batch.schema.names else None
        }
        
        # Calculate MRR based on interval
        interval_col = cols["plan_interval"]
        amount_col = cols["plan_amount"]
        
        # Convert to MRR
        mrr = pc.case_when(
            pc.equal(interval_col, pa.scalar("year")),
            pc.divide(amount_col, pa.scalar(12.0)),
            pc.case_when(
                pc.equal(interval_col, pa.scalar("month")),
                amount_col,
                pc.case_when(
                    pc.equal(interval_col, pa.scalar("week")),
                    pc.multiply(amount_col, pa.scalar(4.33)),
                    pc.multiply(amount_col, pa.scalar(30.0))  # day
                )
            )
        )
        cols["mrr"] = mrr
        
        # Filter Nones
        cols = {k: v for k, v in cols.items() if v is not None}
        
        yield pa.table(cols)


@dlt.resource(name="invoices", write_disposition="replace", primary_key="invoice_id")
def stripe_invoices() -> Iterator[pa.Table]:
    """Clean Stripe invoices"""
    pipeline = dlt.pipeline(
        pipeline_name="stripe_raw",
        destination="duckdb",
        dataset_name="stripe_raw"
    )
    
    raw_invoices = pipeline.dataset().table("invoices")
    
    for batch in raw_invoices.iter_arrow(chunk_size=10000):
        cols = {
            "invoice_id": batch.column("id"),
            "customer_id": batch.column("customer"),
            "subscription_id": batch.column("subscription") if "subscription" in batch.schema.names else None,
            "status": batch.column("status"),
            "created": pc.cast(batch.column("created"), pa.timestamp('s')),
            "due_date": pc.cast(batch.column("due_date"), pa.timestamp('s')) if "due_date" in batch.schema.names else None,
            "amount_due": pc.cast(batch.column("amount_due"), pa.float64()) / 100.0,
            "amount_paid": pc.cast(batch.column("amount_paid"), pa.float64()) / 100.0,
            "amount_remaining": pc.cast(batch.column("amount_remaining"), pa.float64()) / 100.0 if "amount_remaining" in batch.schema.names else None,
            "currency": batch.column("currency"),
            "paid": pc.cast(batch.column("paid"), pa.bool_())
        }
        
        cols = {k: v for k, v in cols.items() if v is not None}
        
        yield pa.table(cols)


@dlt.resource(name="charges", write_disposition="replace", primary_key="charge_id")
def stripe_charges() -> Iterator[pa.Table]:
    """Clean Stripe charges"""
    pipeline = dlt.pipeline(
        pipeline_name="stripe_raw",
        destination="duckdb",
        dataset_name="stripe_raw"
    )
    
    raw_charges = pipeline.dataset().table("charges")
    
    for batch in raw_charges.iter_arrow(chunk_size=10000):
        cols = {
            "charge_id": batch.column("id"),
            "customer_id": batch.column("customer") if "customer" in batch.schema.names else None,
            "invoice_id": batch.column("invoice") if "invoice" in batch.schema.names else None,
            "amount": pc.cast(batch.column("amount"), pa.float64()) / 100.0,
            "currency": batch.column("currency"),
            "status": batch.column("status"),
            "created": pc.cast(batch.column("created"), pa.timestamp('s')),
            "refunded": pc.cast(batch.column("refunded"), pa.bool_()),
            "payment_method_type": batch.column("payment_method_details__type") if "payment_method_details__type" in batch.schema.names else None
        }
        
        cols = {k: v for k, v in cols.items() if v is not None}
        
        yield pa.table(cols)


@dlt.resource(name="payment_intents", write_disposition="replace", primary_key="payment_intent_id")
def stripe_payment_intents() -> Iterator[pa.Table]:
    """Clean Stripe payment intents for PLG self-serve tracking"""
    pipeline = dlt.pipeline(
        pipeline_name="stripe_raw",
        destination="duckdb",
        dataset_name="stripe_raw"
    )
    
    try:
        raw_intents = pipeline.dataset().table("payment_intents")
        
        for batch in raw_intents.iter_arrow(chunk_size=10000):
            cols = {
                "payment_intent_id": batch.column("id"),
                "customer_id": batch.column("customer") if "customer" in batch.schema.names else None,
                "amount": pc.cast(batch.column("amount"), pa.float64()) / 100.0,
                "currency": batch.column("currency"),
                "status": batch.column("status"),
                "created": pc.cast(batch.column("created"), pa.timestamp('s')),
                "setup_future_usage": batch.column("setup_future_usage") if "setup_future_usage" in batch.schema.names else None
            }
            
            cols = {k: v for k, v in cols.items() if v is not None}
            
            yield pa.table(cols)
    except Exception:
        pass


def run_stripe_source():
    """Execute Stripe source pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="stripe_source",
        destination="filesystem",
        dataset_name="stripe_staging",
        destination_kwargs={
            "layout": "{table_name}/{load_id}.parquet"
        }
    )
    
    pipeline.run([
        stripe_customers(),
        stripe_subscriptions(),
        stripe_invoices(),
        stripe_charges(),
        stripe_payment_intents()
    ])
    
    print("✓ Stripe source tables created")


if __name__ == "__main__":
    run_stripe_source()
