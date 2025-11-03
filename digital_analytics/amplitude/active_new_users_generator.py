"""
Amplitude Active and New Users Report Generator
Generates DAU/WAU/MAU aligned with GA4 metrics
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


@dlt.resource(write_disposition="append", table_name="active_users_daily")
def active_users_daily():
    """Generate daily active users by country"""
    
    x_values = []
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        x_values.append(current_date.strftime('%Y-%m-%d'))
    
    series = []
    series_meta = [geo['country'] for geo in GEO_DISTRIBUTION]
    
    for geo in GEO_DISTRIBUTION:
        country_series = []
        for day in range(DAYS_OF_DATA):
            daily_metrics = get_daily_metrics(day)
            country_dau = int(daily_metrics['active_users'] * geo['weight'] * random.uniform(0.95, 1.05))
            country_series.append(country_dau)
        series.append(country_series)
    
    yield {
        'metric_type': 'active',
        'interval': 'daily',
        'group_by': 'country',
        'series': series,
        'seriesMeta': series_meta,
        'xValues': x_values,
        '_generated_at': datetime.now().isoformat(),
    }


@dlt.resource(write_disposition="append", table_name="new_users_daily")
def new_users_daily():
    """Generate daily new users by platform"""
    
    x_values = []
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        x_values.append(current_date.strftime('%Y-%m-%d'))
    
    series = []
    series_meta = ['iOS', 'Android', 'Web']
    
    for platform, weight in DEVICE_DISTRIBUTION.items():
        platform_series = []
        for day in range(DAYS_OF_DATA):
            daily_metrics = get_daily_metrics(day)
            platform_new = int(daily_metrics['new_users'] * weight * random.uniform(0.95, 1.05))
            platform_series.append(platform_new)
        series.append(platform_series)
    
    yield {
        'metric_type': 'new',
        'interval': 'daily',
        'group_by': 'platform',
        'series': series,
        'seriesMeta': series_meta,
        'xValues': x_values,
        '_generated_at': datetime.now().isoformat(),
    }


@dlt.resource(write_disposition="append", table_name="active_users_weekly")
def active_users_weekly():
    """Generate weekly active users"""
    
    weeks = DAYS_OF_DATA // 7
    x_values = []
    for week in range(weeks):
        week_date = START_DATE + timedelta(weeks=week)
        x_values.append(week_date.strftime('%Y-%m-%d'))
    
    series = []
    series_meta = [s['source'].title() for s in TRAFFIC_SOURCES]
    
    for source in TRAFFIC_SOURCES:
        source_series = []
        for week in range(weeks):
            # Aggregate 7 days of metrics
            week_wau = 0
            for day in range(7):
                day_idx = week * 7 + day
                if day_idx < DAYS_OF_DATA:
                    daily_metrics = get_daily_metrics(day_idx)
                    week_wau += daily_metrics['active_users'] * source['weight']
            # WAU is unique users (not sum), so apply deduplication factor
            week_wau = int(week_wau * 0.7 * random.uniform(0.95, 1.05))
            source_series.append(week_wau)
        series.append(source_series)
    
    yield {
        'metric_type': 'active',
        'interval': 'weekly',
        'group_by': 'source',
        'series': series,
        'seriesMeta': series_meta,
        'xValues': x_values,
        '_generated_at': datetime.now().isoformat(),
    }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="amplitude_users",
        destination="filesystem",
        dataset_name="amplitude"
    )
    
    load_info = pipeline.run([
        active_users_daily(),
        new_users_daily(),
        active_users_weekly()
    ], loader_file_format="parquet")
    
    print(f"\n✓ Active/new users generated: {DAYS_OF_DATA} days aligned with GA4")
