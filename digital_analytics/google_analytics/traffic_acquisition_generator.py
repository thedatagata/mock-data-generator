"""
Google Analytics 4 Traffic Acquisition Data Generator
Generates session source/medium data aligned with Amplitude
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


@dlt.resource(write_disposition="append", table_name="traffic_acquisition_session_source_medium")
def traffic_acquisition():
    """Generate GA4 traffic acquisition session data"""
    
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        date_str = current_date.strftime('%Y%m%d')
        
        # Get aligned daily metrics
        daily_metrics = get_daily_metrics(day)
        
        for source in TRAFFIC_SOURCES:
            # Distribute metrics by source weight
            sessions = int(daily_metrics['sessions'] * source['weight'] * random.uniform(0.95, 1.05))
            engaged_sessions = int(sessions * random.uniform(0.6, 0.85))
            total_users = int(daily_metrics['active_users'] * source['weight'] * random.uniform(0.95, 1.05))
            new_users = int(daily_metrics['new_users'] * source['weight'] * random.uniform(0.95, 1.05))
            page_views = int(sessions * random.uniform(2.5, 6.0))
            
            # Conversions aligned with shared config
            conversions = int(sessions * CONVERSION_RATES['add_to_cart'] * 
                            CONVERSION_RATES['checkout_start'] * 
                            CONVERSION_RATES['purchase'] * random.uniform(0.9, 1.1))
            revenue = round(conversions * AVERAGE_TRANSACTION_VALUE * random.uniform(0.95, 1.05), 2)
            
            channel_group = source['campaign_type'].title()
            
            yield {
                'date': date_str,
                'sessionSource': source['source'],
                'sessionMedium': source['medium'],
                'sessionSourceMedium': f"{source['source']} / {source['medium']}",
                'sessionDefaultChannelGroup': channel_group,
                'sessionCampaignName': fake.catch_phrase() if source['medium'] == 'cpc' else '(not set)',
                
                'sessions': sessions,
                'engagedSessions': engaged_sessions,
                'totalUsers': total_users,
                'newUsers': new_users,
                'screenPageViews': page_views,
                'conversions': conversions,
                'totalRevenue': revenue,
                
                'averageSessionDuration': round(random.uniform(120, 480), 2),
                'bounceRate': round(random.uniform(0.3, 0.6), 4),
                'engagementRate': round(engaged_sessions / sessions, 4) if sessions > 0 else 0,
                
                '_generated_at': datetime.now().isoformat(),
                '_lead_count': len(leads_df),
            }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="ga4_traffic",
        destination="filesystem",
        dataset_name="google_analytics"
    )
    
    load_info = pipeline.run(traffic_acquisition(), loader_file_format="parquet")
    
    print(f"\n✓ Traffic acquisition generated: {DAYS_OF_DATA} days aligned with Amplitude")
