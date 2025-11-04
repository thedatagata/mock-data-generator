"""GA4 Traffic Acquisition - Date-partitioned"""
import dlt
import polars as pl
from datetime import datetime, timedelta
import random, sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from shared_config import *
from faker import Faker

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

@dlt.resource(write_disposition="append", table_name="traffic_acquisition", parallelized=True)
def traffic_acquisition():
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        daily_metrics = get_daily_metrics(day)
        daily_records = []
        
        for source in TRAFFIC_SOURCES:
            sessions = int(daily_metrics['new_users'] * source['weight'])
            engaged_sessions = int(sessions * random.uniform(0.4, 0.7))
            
            daily_records.append({
                'event_date': current_date.strftime('%Y%m%d'),
                'event_month': current_date.strftime('%Y-%m'),
                'session_source': source['source'],
                'session_medium': source['medium'],
                'total_sessions': sessions,
                'engaged_sessions': engaged_sessions,
                'engagement_rate': round(engaged_sessions / sessions, 2),
                'event_count': sessions * random.randint(5, 15),
                'events_per_session': round(random.uniform(4, 12), 2),
                'total_users': int(sessions * random.uniform(0.85, 0.95)),
                'new_users': int(sessions * 0.3),
            })
        
        if day % 10 == 0:
            print(f"Day {day}/{DAYS_OF_DATA}")
        yield daily_records

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="ga4_traffic", destination="filesystem", dataset_name="google_analytics")
    pipeline.run(traffic_acquisition(), loader_file_format="parquet")
    print("✓ Traffic acquisition generated")
