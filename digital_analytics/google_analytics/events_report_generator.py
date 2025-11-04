"""
GA4 Events Report - Date-partitioned
"""
import dlt
import polars as pl
from datetime import datetime, timedelta
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from shared_config import *
from faker import Faker

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

print("Loading leads with polars...")
leads_df = pl.read_parquet('gs://mock-source-data/customer_data_population/unified_leads_enriched.parquet')
print(f"Loaded {len(leads_df)} leads")

EVENTS = [
    {'name': 'page_view', 'is_conversion': False, 'value': 0},
    {'name': 'sign_up', 'is_conversion': True, 'value': 0},
    {'name': 'add_to_cart', 'is_conversion': False, 'value': 0},
    {'name': 'begin_checkout', 'is_conversion': False, 'value': 0},
    {'name': 'purchase', 'is_conversion': True, 'value': AVERAGE_TRANSACTION_VALUE},
]

@dlt.resource(
    write_disposition="append",
    table_name="events_report",
    parallelized=True
)
def events_report():
    """Generate daily event aggregates"""
    
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        date_str = current_date.strftime('%Y%m%d')
        daily_metrics = get_daily_metrics(day)
        
        daily_records = []
        
        for source in TRAFFIC_SOURCES:
            source_users = int(daily_metrics['new_users'] * source['weight'])
            
            for event in EVENTS:
                # Calculate event counts based on user behavior
                if event['name'] == 'page_view':
                    event_count = source_users * random.randint(3, 8)
                elif event['name'] == 'sign_up':
                    event_count = int(source_users * 0.15)  # 15% sign up
                elif event['name'] == 'purchase':
                    event_count = int(source_users * 0.02)  # 2% purchase
                else:
                    event_count = int(source_users * random.uniform(0.1, 0.3))
                
                total_users = int(event_count / max(1, random.randint(1, 3)))
                
                daily_records.append({
                    'event_date': date_str,
                    'event_month': current_date.strftime('%Y-%m'),
                    'event_name': event['name'],
                    'source_medium': f"{source['source']}/{source['medium']}",
                    'event_count': event_count,
                    'total_users': total_users,
                    'event_count_per_user': round(event_count / max(1, total_users), 2),
                    'total_revenue': event['value'] * event_count if event['is_conversion'] else 0,
                })
        
        if day % 10 == 0:
            print(f"Day {day}/{DAYS_OF_DATA}")
        
        yield daily_records

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="ga4_events",
        destination="filesystem",
        dataset_name="google_analytics"
    )
    
    pipeline.run(events_report(), loader_file_format="parquet")
    print("✓ Events report generated")
