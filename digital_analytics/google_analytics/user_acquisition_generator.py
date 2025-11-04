"""GA4 User Acquisition - Date-partitioned"""
import dlt
from datetime import datetime, timedelta
import random, sys, os
from faker import Faker
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from shared_config import *

Faker.seed(SEED)
random.seed(SEED)

@dlt.resource(write_disposition="append", table_name="user_acquisition", parallelized=True)
def user_acquisition():
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        daily_metrics = get_daily_metrics(day)
        daily_records = []
        
        for source in TRAFFIC_SOURCES:
            new_users = int(daily_metrics['new_users'] * source['weight'])
            
            daily_records.append({
                'event_date': current_date.strftime('%Y%m%d'),
                'event_month': current_date.strftime('%Y-%m'),
                'first_user_source': source['source'],
                'first_user_medium': source['medium'],
                'new_users': new_users,
                'total_users': new_users,
                'engaged_sessions': int(new_users * random.uniform(0.5, 0.8)),
                'engagement_rate': round(random.uniform(0.5, 0.8), 2),
                'event_count': new_users * random.randint(5, 12),
                'total_revenue': new_users * AVERAGE_TRANSACTION_VALUE * random.uniform(0.01, 0.03),
            })
        
        if day % 10 == 0:
            print(f"Day {day}/{DAYS_OF_DATA}")
        yield daily_records

if __name__ == "__main__":
    pipeline = dlt.pipeline(pipeline_name="ga4_users", destination="filesystem", dataset_name="google_analytics")
    pipeline.run(user_acquisition(), loader_file_format="parquet")
    print("✓ User acquisition generated")
