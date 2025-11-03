"""
Google Analytics 4 Conversions Report Data Generator
Generates conversion event data aligned with Amplitude
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

CONVERSION_EVENTS = [
    {'name': 'sign_up', 'rate': CONVERSION_RATES['sign_up'], 'value': 0},
    {'name': 'purchase', 'rate': CONVERSION_RATES['purchase'], 'value': AVERAGE_TRANSACTION_VALUE},
    {'name': 'generate_lead', 'rate': 0.08, 'value': 25.00},
]


@dlt.resource(write_disposition="append", table_name="conversions_report")
def conversions_report():
    """Generate GA4 conversions data"""
    
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        date_str = current_date.strftime('%Y%m%d')
        
        daily_metrics = get_daily_metrics(day)
        
        for source in TRAFFIC_SOURCES:
            for conversion in CONVERSION_EVENTS:
                # Calculate conversions from daily metrics
                if conversion['name'] == 'sign_up':
                    base_count = daily_metrics['new_users']
                elif conversion['name'] == 'purchase':
                    base_count = daily_metrics['transactions']
                else:
                    base_count = daily_metrics['sessions']
                
                conversions = int(base_count * source['weight'] * conversion['rate'] * random.uniform(0.9, 1.1))
                users = int(conversions * random.uniform(0.8, 0.95))
                total_value = round(conversions * conversion['value'], 2)
                
                yield {
                    'date': date_str,
                    'conversionName': conversion['name'],
                    'sessionSource': source['source'],
                    'sessionMedium': source['medium'],
                    'sessionSourceMedium': f"{source['source']} / {source['medium']}",
                    'sessionCampaignName': fake.catch_phrase() if source['medium'] == 'cpc' else '(not set)',
                    
                    'conversions': conversions,
                    'totalUsers': users,
                    'conversionValue': total_value,
                    'conversionRate': round(conversion['rate'], 4),
                    
                    '_generated_at': datetime.now().isoformat(),
                }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="ga4_conversions",
        destination="filesystem",
        dataset_name="google_analytics"
    )
    
    load_info = pipeline.run(conversions_report(), loader_file_format="parquet")
    
    print(f"\n✓ Conversions data generated: {DAYS_OF_DATA} days aligned with Amplitude")
