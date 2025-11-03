"""
Google Analytics 4 User Acquisition Data Generator
Generates first user source/medium data aligned with Amplitude
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


@dlt.resource(write_disposition="append", table_name="user_acquisition_first_user_source_medium")
def user_acquisition():
    """Generate GA4 user acquisition data"""
    
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        date_str = current_date.strftime('%Y%m%d')
        
        daily_metrics = get_daily_metrics(day)
        
        for source in TRAFFIC_SOURCES:
            new_users = int(daily_metrics['new_users'] * source['weight'] * random.uniform(0.95, 1.05))
            total_users = int(new_users * random.uniform(1.2, 1.5))
            sessions = int(new_users * random.uniform(1.3, 2.0))
            page_views = int(sessions * random.uniform(2.5, 5.0))
            
            conversions = int(new_users * CONVERSION_RATES['sign_up'] * random.uniform(0.9, 1.1))
            revenue = round(conversions * AVERAGE_TRANSACTION_VALUE * 0.3, 2)
            
            yield {
                'date': date_str,
                'firstUserSource': source['source'],
                'firstUserMedium': source['medium'],
                'firstUserSourceMedium': f"{source['source']} / {source['medium']}",
                'firstUserCampaignName': fake.catch_phrase() if source['medium'] == 'cpc' else '(not set)',
                
                'newUsers': new_users,
                'totalUsers': total_users,
                'sessions': sessions,
                'screenPageViews': page_views,
                'conversions': conversions,
                'totalRevenue': revenue,
                
                'averageSessionDuration': round(random.uniform(120, 480), 2),
                'engagementRate': round(random.uniform(0.5, 0.8), 4),
                
                '_generated_at': datetime.now().isoformat(),
            }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="ga4_user_acquisition",
        destination="filesystem",
        dataset_name="google_analytics"
    )
    
    load_info = pipeline.run(user_acquisition(), loader_file_format="parquet")
    
    print(f"\n✓ User acquisition generated: {DAYS_OF_DATA} days aligned with Amplitude")
