"""
Amplitude Retention Analysis Generator
Generates retention data matching official schema with aligned metrics
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


@dlt.resource(write_disposition="append", table_name="retention_analysis")
def retention_analysis():
    """Generate Amplitude retention analysis with aligned new user cohorts"""
    
    # Last 90 days of cohorts
    cohort_days = 90
    cohort_dates = []
    formatted_dates = []
    cohort_start = START_DATE + timedelta(days=DAYS_OF_DATA - cohort_days)
    
    for day in range(cohort_days):
        cohort_date = cohort_start + timedelta(days=day)
        cohort_dates.append(cohort_date.strftime('%Y-%m-%d'))
        formatted_dates.append(cohort_date.strftime('%b %d'))
    
    values = {}
    max_retention_days = cohort_days
    combined_counts = [0] * max_retention_days
    combined_outof = [0] * max_retention_days
    
    for idx, (date_key, formatted_date) in enumerate(zip(cohort_dates, formatted_dates)):
        # Use aligned new user metrics for cohort size
        day_idx = DAYS_OF_DATA - cohort_days + idx
        daily_metrics = get_daily_metrics(day_idx)
        cohort_size = daily_metrics['new_users']
        
        days_available = cohort_days - idx
        cohort_retention = []
        
        for retention_day in range(max_retention_days):
            if retention_day == 0:
                retained = cohort_size
                incomplete = False
            elif retention_day < days_available:
                base_retention = 0.70 - (retention_day * 0.015)
                retention_rate = max(0.10, base_retention * random.uniform(0.9, 1.1))
                retained = int(cohort_size * retention_rate)
                incomplete = False
            else:
                retained = int(cohort_size * 0.12)
                incomplete = True
            
            cohort_retention.append({
                'count': retained,
                'outof': cohort_size,
                'incomplete': incomplete
            })
            
            combined_counts[retention_day] += retained
            if retention_day < days_available:
                combined_outof[retention_day] += cohort_size
        
        values[formatted_date] = cohort_retention
    
    combined = []
    for retention_day in range(max_retention_days):
        incomplete = retention_day >= 1
        combined.append({
            'count': combined_counts[retention_day],
            'outof': combined_outof[retention_day] if combined_outof[retention_day] > 0 else combined_outof[0],
            'retainedSetId': None,
            'incomplete': incomplete
        })
    
    yield {
        'series': [{
            'dates': formatted_dates,
            'values': values,
            'combined': combined
        }],
        'seriesMeta': [{'segmentIndex': 0, 'eventIndex': 0}],
        '_generated_at': datetime.now().isoformat(),
    }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="amplitude_retention",
        destination="filesystem",
        dataset_name="amplitude"
    )
    
    load_info = pipeline.run(retention_analysis(), loader_file_format="parquet")
    
    print(f"\n✓ Retention analysis generated: 90-day cohorts aligned with GA4")
