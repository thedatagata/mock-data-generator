"""
Google Analytics 4 Events Report Data Generator
Generates event-level data for conversion funnel tracking
"""
import dlt
import pandas as pd
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

print("Loading lead data...")
byd_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_bookyourdata/bookyourdata/1762095548.474671.701ad8b601.parquet')
uplead_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_upleads/uplead/1762095612.4264941.2fb362f699.parquet')

leads_df = pd.concat([byd_df, uplead_df], ignore_index=True)
print(f"Analyzed {len(leads_df)} leads")


@dlt.resource(write_disposition="append", table_name="events_report")
def events_report():
    """Generate GA4 events data aligned with Amplitude"""
    
    # Event types with conversion flags
    events = [
        {'name': 'page_view', 'is_conversion': False, 'value': 0},
        {'name': 'sign_up', 'is_conversion': True, 'value': 0},
        {'name': 'add_to_cart', 'is_conversion': False, 'value': 0},
        {'name': 'begin_checkout', 'is_conversion': False, 'value': 0},
        {'name': 'purchase', 'is_conversion': True, 'value': AVERAGE_TRANSACTION_VALUE},
    ]
    
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        date_str = current_date.strftime('%Y%m%d')
        
        # Get aligned daily metrics
        daily_metrics = get_daily_metrics(day)
        
        for source in TRAFFIC_SOURCES:
            # Distribute metrics by source weight
            source_sessions = int(daily_metrics['sessions'] * source['weight'])
            source_users = int(daily_metrics['active_users'] * source['weight'])
            
            for event in events:
                # Calculate event counts
                if event['name'] == 'page_view':
                    count = int(source_sessions * random.uniform(2.5, 3.5))
                elif event['name'] == 'sign_up':
                    count = int(source_users * CONVERSION_RATES['sign_up'] * random.uniform(0.9, 1.1))
                elif event['name'] == 'add_to_cart':
                    count = int(source_sessions * CONVERSION_RATES['add_to_cart'] * random.uniform(0.9, 1.1))
                elif event['name'] == 'begin_checkout':
                    count = int(source_sessions * CONVERSION_RATES['add_to_cart'] * 
                              CONVERSION_RATES['checkout_start'] * random.uniform(0.9, 1.1))
                elif event['name'] == 'purchase':
                    count = int(daily_metrics['transactions'] * source['weight'] * random.uniform(0.95, 1.05))
                else:
                    count = int(source_sessions * random.uniform(0.1, 0.3))
                
                users = int(count * random.uniform(0.6, 0.9))
                conversions = count if event['is_conversion'] else 0
                event_value = count * event['value'] if event['value'] > 0 else 0
                
                yield {
                    'date': date_str,
                    'eventName': event['name'],
                    'sessionSource': source['source'],
                    'sessionMedium': source['medium'],
                    'sessionSourceMedium': f"{source['source']} / {source['medium']}",
                    'sessionCampaignName': fake.catch_phrase() if source['medium'] == 'cpc' else '(not set)',
                    
                    'eventCount': count,
                    'eventValue': round(event_value, 2),
                    'totalUsers': users,
                    'conversions': conversions,
                    'eventCountPerUser': round(count / users, 2) if users > 0 else 0,
                    
                    '_generated_at': datetime.now().isoformat(),
                    '_lead_count': len(leads_df),
                    '_is_conversion_event': event['is_conversion'],
                }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="ga4_events",
        destination="filesystem",
        dataset_name="google_analytics"
    )
    
    load_info = pipeline.run(events_report(), loader_file_format="parquet")
    
    print(f"\n✓ Events data generated: {DAYS_OF_DATA} days × {len(TRAFFIC_SOURCES)} sources × 5 events")
