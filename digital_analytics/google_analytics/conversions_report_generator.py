"""GA4 Conversions - Date-partitioned"""
import dlt
from datetime import datetime, timedelta
import random, sys, os
from faker import Faker
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from shared_config import *

Faker.seed(SEED)
random.seed(SEED)

KEY_EVENTS = ['sign_up', 'trial_started', 'purchase', 'demo_requested']

@dlt.resource(write_disposition="append", table_name="conversions_report", parallelized=True)
def conversions_report():
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        daily_metrics = get_daily_metrics(day)
        daily_records = []
        
        for source in TRAFFIC_SOURCES:
            source_users = int(daily_metrics['new_users'] * source['weight'])
            
            for event_name in KEY_EVENTS:
                conversion_rate = {'sign_up': 0.15, 'trial_started': 0.12, 'purchase': 0.02, 'demo_requested': 0.08}.get(event_name, 0.05)
                key_events = int(source_users * conversion_rate)
                revenue = key_events * AVERAGE_TRANSACTION_VALUE if event_name == 'purchase' else 0
                
                daily_records.append({
                    'event_date': current_date.strftime('%Y%m%d'),
                    'event_month': current_date.strftime('%Y-%m'),
                    'event_name': event_name,
                    'source_medium': f"{source['source']}/{source['medium']}",
                    'key_events': key_events,
                    'total_revenue': revenue,
                    'total_users': int(key_events * random.uniform(0.8, 1.0)),
                })
        
        if day % 10 == 0:
            print(f"Day {day}/{DAYS_OF_DATA}")
        yield daily_records

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="ga4_conversions", destination="filesystem", dataset_name="google_analytics")
    pipeline.run(conversions_report(), loader_file_format="parquet")
    print("✓ Conversions report generated")
