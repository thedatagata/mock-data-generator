import dlt
import pyarrow as pa
import pyarrow.compute as pc
from typing import Iterator

@dlt.resource(name="contacts", write_disposition="replace", primary_key="contact_id")
def crm_contacts() -> Iterator[pa.Table]:
    """Clean CRM contacts"""
    pipeline = dlt.pipeline(
        pipeline_name="crm_raw",
        destination="duckdb",
        dataset_name="crm_raw"
    )
    
    raw_contacts = pipeline.dataset().table("contacts")
    
    for batch in raw_contacts.iter_arrow(chunk_size=10000):
        cols = {}
        
        for name in batch.schema.names:
            if name.startswith("_dlt"):
                if name == "_dlt_id":
                    cols["contact_id"] = batch.column(name)
                continue
            
            # Standardize naming
            clean_name = name.lower().replace(" ", "_")
            col = batch.column(name)
            
            # Type casting based on common CRM fields
            if "email" in clean_name:
                cols[clean_name] = pc.utf8_lower(pc.utf8_trim(col))
            elif "phone" in clean_name:
                cols[clean_name] = pc.utf8_trim(col)
            elif clean_name in ["created_at", "updated_at", "last_activity_date"]:
                cols[clean_name] = pc.cast(col, pa.timestamp('us'))
            elif clean_name in ["annual_revenue", "deal_amount"]:
                cols[clean_name] = pc.cast(col, pa.float64())
            else:
                cols[clean_name] = col
        
        yield pa.table(cols)


@dlt.resource(name="accounts", write_disposition="replace", primary_key="account_id")
def crm_accounts() -> Iterator[pa.Table]:
    """Clean CRM accounts/companies"""
    pipeline = dlt.pipeline(
        pipeline_name="crm_raw",
        destination="duckdb",
        dataset_name="crm_raw"
    )
    
    raw_accounts = pipeline.dataset().table("accounts")
    
    for batch in raw_accounts.iter_arrow(chunk_size=10000):
        cols = {}
        
        for name in batch.schema.names:
            if name.startswith("_dlt"):
                if name == "_dlt_id":
                    cols["account_id"] = batch.column(name)
                continue
            
            clean_name = name.lower().replace(" ", "_")
            col = batch.column(name)
            
            if clean_name in ["created_at", "updated_at"]:
                cols[clean_name] = pc.cast(col, pa.timestamp('us'))
            elif clean_name in ["annual_revenue", "number_of_employees"]:
                cols[clean_name] = pc.cast(col, pa.float64())
            else:
                cols[clean_name] = col
        
        yield pa.table(cols)


@dlt.resource(name="opportunities", write_disposition="replace", primary_key="opportunity_id")
def crm_opportunities() -> Iterator[pa.Table]:
    """Clean CRM opportunities with proper stage typing"""
    pipeline = dlt.pipeline(
        pipeline_name="crm_raw",
        destination="duckdb",
        dataset_name="crm_raw"
    )
    
    raw_opps = pipeline.dataset().table("opportunities")
    
    for batch in raw_opps.iter_arrow(chunk_size=10000):
        cols = {}
        
        for name in batch.schema.names:
            if name.startswith("_dlt"):
                if name == "_dlt_id":
                    cols["opportunity_id"] = batch.column(name)
                continue
            
            clean_name = name.lower().replace(" ", "_")
            col = batch.column(name)
            
            # Type conversions
            if clean_name in ["created_at", "updated_at", "close_date", "closed_at"]:
                cols[clean_name] = pc.cast(col, pa.timestamp('us'))
            elif clean_name in ["amount", "probability"]:
                cols[clean_name] = pc.cast(col, pa.float64())
            elif clean_name in ["is_won", "is_closed"]:
                cols[clean_name] = pc.cast(col, pa.bool_())
            else:
                cols[clean_name] = col
        
        yield pa.table(cols)


@dlt.resource(name="activities", write_disposition="replace")
def crm_activities() -> Iterator[pa.Table]:
    """Clean CRM activities (calls, emails, meetings)"""
    pipeline = dlt.pipeline(
        pipeline_name="crm_raw",
        destination="duckdb",
        dataset_name="crm_raw"
    )
    
    try:
        raw_activities = pipeline.dataset().table("activities")
        
        for batch in raw_activities.iter_arrow(chunk_size=10000):
            cols = {}
            
            for name in batch.schema.names:
                if name.startswith("_dlt"):
                    if name == "_dlt_id":
                        cols["activity_id"] = batch.column(name)
                    continue
                
                clean_name = name.lower().replace(" ", "_")
                col = batch.column(name)
                
                if "date" in clean_name or "time" in clean_name:
                    cols[clean_name] = pc.cast(col, pa.timestamp('us'))
                else:
                    cols[clean_name] = col
            
            yield pa.table(cols)
    except Exception:
        pass


def run_crm_source():
    """Execute CRM source pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="crm_source",
        destination="filesystem",
        dataset_name="crm_staging",
        destination_kwargs={
            "layout": "{table_name}/{load_id}.parquet"
        }
    )
    
    pipeline.run([
        crm_contacts(),
        crm_accounts(),
        crm_opportunities(),
        crm_activities()
    ])
    
    print("✓ CRM source tables created")


if __name__ == "__main__":
    run_crm_source()
