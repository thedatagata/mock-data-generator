import dlt
import pyarrow as pa
import pyarrow.compute as pc
from typing import Iterator

@dlt.resource(name="events", write_disposition="replace", primary_key="event_id")
def ga4_events() -> Iterator[pa.Table]:
    """Clean GA4 events with standardized naming"""
    pipeline = dlt.pipeline(
        pipeline_name="ga4_raw",
        destination="duckdb",
        dataset_name="ga4_raw"
    )
    
    raw_events = pipeline.dataset().table("events")
    
    for batch in raw_events.iter_arrow(chunk_size=10000):
        # Rename _dlt_id to event_id
        cols = {}
        for name in batch.schema.names:
            if name == "_dlt_id":
                cols["event_id"] = batch.column(name)
            elif name.startswith("_dlt"):
                continue  # Skip other DLT metadata
            # Handle double underscore nesting from DLT
            elif "__" in name:
                # device__category -> device_category
                clean_name = name.replace("__", "_")
                cols[clean_name] = batch.column(name)
            else:
                cols[name] = batch.column(name)
        
        # Cast timestamps to proper types
        if "event_timestamp" in cols:
            cols["event_timestamp"] = pc.cast(cols["event_timestamp"], pa.timestamp('us'))
        if "user_first_touch_timestamp" in cols:
            cols["user_first_touch_timestamp"] = pc.cast(
                cols["user_first_touch_timestamp"], 
                pa.timestamp('us')
            )
        
        # Cast numeric fields
        if "event_bundle_sequence_id" in cols:
            cols["event_bundle_sequence_id"] = pc.cast(
                cols["event_bundle_sequence_id"], 
                pa.int64()
            )
        
        yield pa.table(cols)


@dlt.resource(name="event_params", write_disposition="replace")
def ga4_event_params() -> Iterator[pa.Table]:
    """Clean event parameters with proper typing"""
    pipeline = dlt.pipeline(
        pipeline_name="ga4_raw",
        destination="duckdb",
        dataset_name="ga4_raw"
    )
    
    raw_params = pipeline.dataset().table("events__event_params")
    
    for batch in raw_params.iter_arrow(chunk_size=10000):
        cols = {
            "event_id": batch.column("_dlt_parent_id"),
            "param_key": batch.column("key")
        }
        
        # Handle value types
        if "value__int_value" in batch.schema.names:
            cols["value_int"] = batch.column("value__int_value")
        if "value__string_value" in batch.schema.names:
            cols["value_string"] = batch.column("value__string_value")
        if "value__float_value" in batch.schema.names:
            cols["value_float"] = batch.column("value__float_value")
        if "value__double_value" in batch.schema.names:
            cols["value_double"] = batch.column("value__double_value")
        
        # Coalesced value for convenience
        cols["value"] = pc.coalesce(
            pc.cast(cols.get("value_int", pa.array([None])), pa.string()),
            cols.get("value_string", pa.array([None])),
            pc.cast(cols.get("value_float", pa.array([None])), pa.string()),
            pc.cast(cols.get("value_double", pa.array([None])), pa.string())
        )
        
        yield pa.table(cols)


@dlt.resource(name="user_properties", write_disposition="replace")
def ga4_user_properties() -> Iterator[pa.Table]:
    """Clean user properties"""
    pipeline = dlt.pipeline(
        pipeline_name="ga4_raw",
        destination="duckdb",
        dataset_name="ga4_raw"
    )
    
    try:
        raw_props = pipeline.dataset().table("events__user_properties")
        
        for batch in raw_props.iter_arrow(chunk_size=10000):
            cols = {
                "event_id": batch.column("_dlt_parent_id"),
                "property_key": batch.column("key")
            }
            
            # Handle nested value structure
            if "value__string_value" in batch.schema.names:
                cols["property_value"] = batch.column("value__string_value")
            
            yield pa.table(cols)
    except Exception:
        # User properties may not exist in all GA4 exports
        pass


def run_ga4_source():
    """Execute GA4 source pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="ga4_source",
        destination="filesystem",
        dataset_name="ga4_staging",
        destination_kwargs={
            "layout": "{table_name}/{load_id}.parquet"
        }
    )
    
    pipeline.run([
        ga4_events(),
        ga4_event_params(),
        ga4_user_properties()
    ])
    
    print("✓ GA4 source tables created")


if __name__ == "__main__":
    run_ga4_source()
