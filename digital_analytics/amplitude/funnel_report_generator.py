"""
Amplitude Funnel Analysis Generator
Generates funnel conversion data aligned with GA4
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

FUNNELS = [
    {
        'name': 'Purchase Funnel',
        'events': ['page_view', 'add_to_cart', 'checkout_start', 'purchase'],
        'conversion_rates': [1.0, CONVERSION_RATES['add_to_cart'], 
                           CONVERSION_RATES['add_to_cart'] * CONVERSION_RATES['checkout_start'],
                           CONVERSION_RATES['add_to_cart'] * CONVERSION_RATES['checkout_start'] * CONVERSION_RATES['purchase']]
    },
    {
        'name': 'Sign Up Funnel',
        'events': ['landing_page', 'form_view', 'form_submit', 'sign_up'],
        'conversion_rates': [1.0, 0.65, 0.52, CONVERSION_RATES['sign_up']]
    },
]


def generate_histogram_bins(step_idx, num_bins=10):
    bins = []
    base_time = step_idx * 60000
    for i in range(num_bins):
        start = base_time + (i * 10000)
        end = start + 10000
        bins.append({
            'start': start,
            'end': end,
            'bin_dist': {
                'users': random.randint(100, 5000),
                'count': random.randint(100, 5000),
                'propsum': round(random.uniform(0.01, 0.15), 4)
            }
        })
    return bins


@dlt.resource(write_disposition="append", table_name="funnel_report")
def funnel_report():
    """Generate Amplitude funnel analysis aligned with GA4"""
    
    for funnel in FUNNELS:
        num_steps = len(funnel['events'])
        
        # Use aligned metrics for base users
        avg_daily_sessions = sum(get_daily_metrics(d)['sessions'] for d in range(30)) // 30
        base_users = avg_daily_sessions * 30
        
        # Calculate conversions using shared rates
        cumulative = funnel['conversion_rates']
        cumulative_raw = [int(base_users * rate * random.uniform(0.95, 1.05)) for rate in cumulative]
        
        step_by_step = [1.0] + [cumulative[i] / cumulative[i-1] for i in range(1, num_steps)]
        
        median_trans_times = [0] + [random.randint(30000, 600000) for _ in range(1, num_steps)]
        avg_trans_times = [0] + [random.randint(100000, 3600000) for _ in range(1, num_steps)]
        
        date_series = []
        formatted_dates = []
        median_series = []
        avg_series = []
        funnel_series = []
        
        for day in range(30):
            current_date = START_DATE + timedelta(days=DAYS_OF_DATA - 30 + day)
            date_str = current_date.strftime('%Y-%m-%d')
            formatted_date = current_date.strftime('%b %d')
            
            date_series.append(date_str)
            formatted_dates.append(formatted_date)
            
            daily_median = [0] + [int(median_trans_times[i] * random.uniform(0.9, 1.1)) for i in range(1, num_steps)]
            median_series.append(daily_median)
            
            daily_avg = [0] + [int(avg_trans_times[i] * random.uniform(0.85, 1.15)) for i in range(1, num_steps)]
            avg_series.append(daily_avg)
            
            daily_funnel = [int(cumulative_raw[i] / 30 * random.uniform(0.8, 1.2)) for i in range(num_steps)]
            funnel_series.append(daily_funnel)
        
        step_trans_time_dist = [{'stepIndex': i, 'bins': generate_histogram_bins(i)} for i in range(num_steps)]
        step_prev_step_dist = [{'stepIndex': i, 'bins': generate_histogram_bins(i, num_bins=5)} for i in range(num_steps)]
        
        yield {
            'funnel_name': funnel['name'],
            'meta': {'segmentIndex': 0},
            'dayMedianTransTimes': {'series': median_series, 'xValues': date_series, 'formattedXValues': formatted_dates},
            'dayAvgTransTimes': {'series': avg_series, 'xValues': date_series, 'formattedXValues': formatted_dates},
            'dayFunnels': {'series': funnel_series, 'xValues': date_series, 'formattedXValues': formatted_dates},
            'stepByStep': step_by_step,
            'medianTransTimes': median_trans_times,
            'cumulative': cumulative,
            'cumulativeRaw': cumulative_raw,
            'avgTransTimes': avg_trans_times,
            'stepTransTimeDistribution': step_trans_time_dist,
            'stepPrevStepCountDistribution': step_prev_step_dist,
            'events': funnel['events'],
            '_generated_at': datetime.now().isoformat(),
        }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="amplitude_funnels",
        destination="filesystem",
        dataset_name="amplitude"
    )
    
    load_info = pipeline.run(funnel_report(), loader_file_format="parquet")
    
    print(f"\n✓ Funnel analysis generated with aligned conversion rates")
