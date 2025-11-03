"""
Amplitude Revenue LTV Report Generator
Only 5% of leads become paying customers and appear in revenue data
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
print(f"Loaded {len(leads_df)} leads")


@dlt.resource(write_disposition="append", table_name="revenue_ltv_report")
def revenue_ltv_report():
    """LTV analysis: only paying leads (5%) contribute to revenue"""
    
    cohort_days = 90
    cohort_dates = []
    cohort_start = START_DATE + timedelta(days=DAYS_OF_DATA - cohort_days)
    
    for day in range(cohort_days):
        cohort_date = cohort_start + timedelta(days=day)
        cohort_dates.append(cohort_date.strftime('%Y-%m-%d'))
    
    analysis_dates = []
    for day in range(30):
        analysis_date = datetime.now() - timedelta(days=29 - day)
        analysis_dates.append(analysis_date.strftime('%Y-%m-%d'))
    
    values = {}
    
    for idx, cohort_date_str in enumerate(cohort_dates):
        day_idx = DAYS_OF_DATA - cohort_days + idx
        daily_metrics = get_daily_metrics(day_idx)
        
        # Cohort = new lead users this day
        lead_cohort_size = int(daily_metrics['new_users'] * LEAD_PERCENTAGE_OF_TRAFFIC)
        identified = int(lead_cohort_size * LEAD_IDENTIFICATION_RATE)
        paying = daily_metrics['paying_leads']  # 5% of total leads
        
        cohort_data = {
            'count': identified,  # Identified users in cohort
            'paid': paying,       # Paying customers (5% of all leads)
        }
        
        cohort_date = datetime.strptime(cohort_date_str, '%Y-%m-%d')
        days_since = (datetime.now() - cohort_date).days
        
        cumulative_revenue = 0
        for day in range(1, min(91, days_since + 1)):
            if day == 1:
                daily_revenue = paying * AVERAGE_TRANSACTION_VALUE
            else:
                decay = 1.0 / (1 + (day * 0.12))
                daily_revenue = paying * AVERAGE_TRANSACTION_VALUE * 0.15 * decay
            
            cumulative_revenue += daily_revenue
            cohort_data[f'r{day}d'] = round(cumulative_revenue, 2)
        
        cohort_data['total_amount'] = round(cumulative_revenue, 2)
        values[cohort_date_str] = cohort_data
    
    yield {
        'seriesLabels': ['Identified Leads'],
        'analysis_dates': analysis_dates,
        'series': [{
            'dates': analysis_dates,
            'values': values
        }],
        '_generated_at': datetime.now().isoformat(),
        '_note': 'Revenue from 5% of leads who become paying customers',
    }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="amplitude_ltv",
        destination="filesystem",
        dataset_name="amplitude"
    )
    
    load_info = pipeline.run(revenue_ltv_report(), loader_file_format="parquet")
    
    print(f"\n✓ LTV report: 90-day cohorts showing only 5% paying lead customers")
