"""
Comprehensive Analysis: Amplitude Events → GA4 Reports + User Funnel State
Generates GA4 reports matching exact schemas AND user-level funnel progression for CRM
"""
import polars as pl
from datetime import datetime
import sys
import os

sys.path.append(os.path.dirname(__file__))
from shared_config import KEY_EVENTS

AVERAGE_TRANSACTION_VALUE = 99.0  # For revenue calculations

print("="*80)
print("LOADING AMPLITUDE EVENT DATA")
print("="*80)

# Read all months
dfs = []
for month in range(12):
    df = pl.read_parquet(f'data/events_month_{month:02d}.parquet')
    dfs.append(df)
    print(f"  Month {month}: {len(df):,} events")

events = pl.concat(dfs)
print(f"\nTotal events: {len(events):,}")
print(f"Date range: {events.select('event_date').min()[0,0]} to {events.select('event_date').max()[0,0]}")

# Parse nested properties
events = events.with_columns([
    pl.col('event_properties').struct.field('source').alias('source'),
    pl.col('event_properties').struct.field('medium').alias('medium'),
    pl.col('event_properties').struct.field('campaign_name').alias('campaign_name'),
    pl.col('user_properties').struct.field('engagement_tier').alias('engagement_tier'),
])

print("\n" + "="*80)
print("GA4 EVENTS REPORT")
print("="*80)

# Events report - exact schema match
ga4_events_report = (events
    .with_columns([
        pl.col('event_date').str.replace_all('-', '').alias('event_date'),
        pl.col('event_date').str.slice(0, 7).alias('event_month'),
        (pl.col('source') + '/' + pl.col('medium')).alias('source_medium')
    ])
    .group_by(['event_date', 'event_month', pl.col('event_type').alias('event_name'), 'source_medium'])
    .agg([
        pl.count().alias('event_count'),
        pl.col('device_id').n_unique().alias('total_users'),
        (pl.when(pl.col('event_type').is_in(['payment_completed', 'subscription_created', 'trial_converted']))
         .then(pl.lit(AVERAGE_TRANSACTION_VALUE))
         .otherwise(pl.lit(0.0))).sum().alias('total_revenue')
    ])
    .with_columns([
        (pl.col('event_count') / pl.col('total_users')).round(2).alias('event_count_per_user')
    ])
    .sort('event_date')
)

print(ga4_events_report.head(10))
print(f"\nTotal records: {len(ga4_events_report):,}")

print("\n" + "="*80)
print("GA4 CONVERSIONS REPORT")
print("="*80)

# Conversions report - key events only (exact schema)
ga4_conversions_report = (events
    .filter(pl.col('event_type').is_in(KEY_EVENTS))
    .with_columns([
        pl.col('event_date').str.replace_all('-', '').alias('event_date'),
        pl.col('event_date').str.slice(0, 7).alias('event_month'),
        (pl.col('source') + '/' + pl.col('medium')).alias('source_medium')
    ])
    .group_by(['event_date', 'event_month', pl.col('event_type').alias('event_name'), 'source_medium'])
    .agg([
        pl.count().alias('key_events'),
        pl.col('device_id').n_unique().alias('total_users'),
        (pl.when(pl.col('event_type').is_in(['payment_completed', 'subscription_created', 'trial_converted']))
         .then(pl.lit(AVERAGE_TRANSACTION_VALUE))
         .otherwise(pl.lit(0.0))).sum().alias('total_revenue')
    ])
    .sort('event_date')
)

print(ga4_conversions_report.head(10))
print(f"\nTotal records: {len(ga4_conversions_report):,}")

print("\n" + "="*80)
print("GA4 TRAFFIC ACQUISITION REPORT")
print("="*80)

# Traffic acquisition - session-based metrics (exact schema)
# Step 1: Session-level aggregations
session_aggs = (events
    .with_columns([
        pl.col('event_date').str.replace_all('-', '').alias('event_date'),
        pl.col('event_date').str.slice(0, 7).alias('event_month'),
        pl.col('source').alias('session_source'),
        pl.col('medium').alias('session_medium'),
    ])
    .group_by(['event_date', 'event_month', 'session_source', 'session_medium', 'session_id', 'device_id'])
    .agg([
        pl.count().alias('events_in_session'),
        pl.col('event_type').is_in(KEY_EVENTS).any().alias('has_conversion')
    ])
)

# Step 2: Daily aggregations
ga4_traffic_acquisition = (session_aggs
    .group_by(['event_date', 'event_month', 'session_source', 'session_medium'])
    .agg([
        pl.col('session_id').n_unique().alias('total_sessions'),
        pl.col('has_conversion').sum().alias('engaged_sessions'),
        pl.col('events_in_session').sum().alias('event_count'),
        pl.col('device_id').n_unique().alias('total_users'),
        # Approximate new users as 30% of total for this report
        (pl.col('device_id').n_unique() * 0.3).cast(pl.Int64).alias('new_users'),
    ])
    .with_columns([
        (pl.col('engaged_sessions') / pl.col('total_sessions')).round(2).alias('engagement_rate'),
        (pl.col('event_count') / pl.col('total_sessions')).round(2).alias('events_per_session')
    ])
    .sort('event_date')
)

print(ga4_traffic_acquisition.head(10))
print(f"\nTotal records: {len(ga4_traffic_acquisition):,}")

print("\n" + "="*80)
print("GA4 USER ACQUISITION REPORT")  
print("="*80)

# User acquisition - first touch attribution (exact schema)
first_touch = (events
    .sort('event_time')
    .group_by('device_id')
    .agg([
        pl.col('event_date').first().alias('first_date'),
        pl.col('source').first().alias('first_user_source'),
        pl.col('medium').first().alias('first_user_medium'),
    ])
)

# Calculate new users (those whose first_date is on that day)
new_users_by_day = (first_touch
    .with_columns([
        pl.col('first_date').str.replace_all('-', '').alias('event_date'),
        pl.col('first_date').str.slice(0, 7).alias('event_month'),
    ])
    .group_by(['event_date', 'event_month', 'first_user_source', 'first_user_medium'])
    .agg([
        pl.count().alias('new_users')
    ])
)

# Join events with first touch, then aggregate
user_acquisition_events = (events
    .join(first_touch, on='device_id')
    .with_columns([
        pl.col('event_date').str.replace_all('-', '').alias('event_date'),
        pl.col('event_date').str.slice(0, 7).alias('event_month'),
    ])
)

# Session-level for engaged sessions
user_acq_sessions = (user_acquisition_events
    .group_by(['event_date', 'event_month', 'first_user_source', 'first_user_medium', 'session_id'])
    .agg([
        pl.col('event_type').is_in(KEY_EVENTS).any().alias('has_conversion')
    ])
    .group_by(['event_date', 'event_month', 'first_user_source', 'first_user_medium'])
    .agg([
        pl.col('has_conversion').sum().alias('engaged_sessions'),
        pl.count().alias('total_sessions')
    ])
)

# Event and revenue aggregations
user_acq_metrics = (user_acquisition_events
    .group_by(['event_date', 'event_month', 'first_user_source', 'first_user_medium'])
    .agg([
        pl.count().alias('event_count'),
        pl.col('device_id').n_unique().alias('total_users'),
        (pl.when(pl.col('event_type').is_in(['payment_completed', 'subscription_created', 'trial_converted']))
         .then(pl.lit(AVERAGE_TRANSACTION_VALUE))
         .otherwise(pl.lit(0.0))).sum().alias('total_revenue')
    ])
)

# Combine everything
ga4_user_acquisition = (new_users_by_day
    .join(user_acq_sessions, on=['event_date', 'event_month', 'first_user_source', 'first_user_medium'], how='left')
    .join(user_acq_metrics, on=['event_date', 'event_month', 'first_user_source', 'first_user_medium'], how='left')
    .with_columns([
        pl.col('engaged_sessions').fill_null(0),
        pl.col('total_sessions').fill_null(0),
        pl.col('event_count').fill_null(0),
        pl.col('total_users').fill_null(pl.col('new_users')),
        pl.col('total_revenue').fill_null(0.0),
    ])
    .with_columns([
        (pl.col('engaged_sessions') / pl.when(pl.col('total_sessions') > 0).then(pl.col('total_sessions')).otherwise(1)).round(2).alias('engagement_rate')
    ])
    .sort('event_date')
)

print(ga4_user_acquisition.head(10))
print(f"\nTotal records: {len(ga4_user_acquisition):,}")

print("\n" + "="*80)
print("USER FUNNEL STATE ANALYSIS (FOR CRM)")
print("="*80)

# Step 1: Create identity graph - resolve all identities per device
# This is critical for stitching anonymous → identified journeys
identity_graph = (events
    .group_by('device_id')
    .agg([
        # Get the UUID if user ever identified on this device
        pl.col('uuid').filter(pl.col('uuid') != '').first().alias('resolved_uuid'),
        pl.col('email').filter(pl.col('email') != '').first().alias('resolved_email'),
        
        # Track when they identified
        pl.col('identified_at_date').filter(pl.col('identified_at_date') != '').first().alias('identification_date'),
        pl.col('identified_at_session').filter(pl.col('identified_at_session') > 0).first().alias('identification_session'),
        
        # Track all cookie IDs used by this device
        pl.col('all_cookie_ids').first().alias('cookie_history'),
    ])
)

# Step 2: Create user-level funnel progression table with identity resolution
funnel_milestones = (events
    .group_by('device_id')
    .agg([
        pl.col('event_date').min().alias('first_visit_date'),
        pl.col('event_date').filter(pl.col('event_type') == 'trial_started').min().alias('trial_started_date'),
        pl.col('event_date').filter(pl.col('event_type').is_in(['trial_converted', 'payment_completed', 'subscription_created'])).min().alias('trial_converted_date'),
        pl.col('event_date').filter(pl.col('event_type') == 'demo_requested').min().alias('demo_requested_date'),
        pl.col('event_date').filter(pl.col('event_type') == 'subscription_cancelled').min().alias('churned_date'),
        pl.col('event_date').max().alias('last_active_date'),
    ])
)

# Step 3: Get engagement metrics per user
user_engagement = (events
    .group_by('device_id')
    .agg([
        pl.col('session_id').n_unique().alias('total_sessions'),
        pl.count().alias('total_events'),
        pl.col('engagement_tier').last().alias('engagement_tier'),
    ])
)

# Step 4: Join identity graph to get resolved identities for all users
# This ensures anonymous events are linked to identified users

# Build comprehensive user funnel state with identity resolution
user_funnel_state = (funnel_milestones
    .join(first_touch, on='device_id')
    .join(user_engagement, on='device_id')
    .join(identity_graph, on='device_id', how='left')  # Add identity resolution
    .with_columns([
        # Use resolved identity (works for both anonymous and identified users)
        pl.coalesce('resolved_uuid', 'device_id').alias('user_id'),
        pl.col('resolved_email').alias('email'),
        
        # Determine current funnel stage
        pl.when(pl.col('churned_date').is_not_null())
            .then(pl.lit('churned'))
        .when(pl.col('trial_converted_date').is_not_null())
            .then(pl.lit('customer'))
        .when(pl.col('demo_requested_date').is_not_null())
            .then(pl.lit('demo_requested'))
        .when(pl.col('trial_started_date').is_not_null())
            .then(pl.lit('trial_active'))
        .otherwise(pl.lit('visitor'))
        .alias('current_stage'),
        
        # Calculate days in current stage
        pl.when(pl.col('churned_date').is_not_null())
            .then((pl.col('last_active_date').str.to_date() - pl.col('churned_date').str.to_date()).dt.total_days())
        .when(pl.col('trial_converted_date').is_not_null())
            .then((pl.col('last_active_date').str.to_date() - pl.col('trial_converted_date').str.to_date()).dt.total_days())
        .when(pl.col('demo_requested_date').is_not_null())
            .then((pl.col('last_active_date').str.to_date() - pl.col('demo_requested_date').str.to_date()).dt.total_days())
        .when(pl.col('trial_started_date').is_not_null())
            .then((pl.col('last_active_date').str.to_date() - pl.col('trial_started_date').str.to_date()).dt.total_days())
        .otherwise((pl.col('last_active_date').str.to_date() - pl.col('first_visit_date').str.to_date()).dt.total_days())
        .alias('days_in_current_stage'),
        
        # Days since first visit
        (pl.col('last_active_date').str.to_date() - pl.col('first_visit_date').str.to_date()).dt.total_days().alias('days_since_first_visit'),
        
        # Create source/medium combined field
        (pl.col('first_user_source') + '/' + pl.col('first_user_medium')).alias('acquisition_channel'),
    ])
    .sort('last_active_date', descending=True)
)

print(f"\nTotal users tracked: {len(user_funnel_state):,}")

# Show identity resolution effectiveness
identified_users = user_funnel_state.filter(pl.col('email').is_not_null()).height
print(f"\n🔗 Identity Resolution:")
print(f"   Total devices tracked: {len(user_funnel_state):,}")
print(f"   Identified users (with email): {identified_users:,} ({identified_users/len(user_funnel_state)*100:.1f}%)")
print(f"   Anonymous users: {len(user_funnel_state) - identified_users:,}")

print("\nFunnel stage distribution:")
stage_counts = user_funnel_state.group_by('current_stage').agg(pl.count().alias('count')).sort('count', descending=True)
print(stage_counts)

print("\nTop 10 users (most recent activity):")
print(user_funnel_state.head(10).select([
    'device_id', 'user_id', 'email', 'current_stage', 'days_in_current_stage',
    'total_sessions', 'engagement_tier', 'acquisition_channel', 'last_active_date'
]))

# Identify high-value leads for CRM prioritization
high_value_leads = (user_funnel_state
    .filter(
        (pl.col('current_stage').is_in(['trial_active', 'demo_requested'])) &
        (pl.col('engagement_tier').is_in(['high_engagement', 'medium_engagement', 'very_high_engagement'])) &
        (pl.col('total_sessions') >= 3)
    )
    .sort('total_events', descending=True)
)

print(f"\n🎯 High-value leads (trial/demo + engaged): {len(high_value_leads):,}")
if len(high_value_leads) > 0:
    print(high_value_leads.head(10).select([
        'device_id', 'user_id', 'email', 'current_stage', 'total_sessions', 
        'engagement_tier', 'acquisition_channel'
    ]))

# Save user funnel state for CRM
os.makedirs('output', exist_ok=True)
user_funnel_state.write_parquet('output/user_funnel_state.parquet')
print("\n✓ Saved user funnel state to output/user_funnel_state.parquet")

# Also save high-value leads separately
if len(high_value_leads) > 0:
    high_value_leads.write_parquet('output/high_value_leads.parquet')
    print("✓ Saved high-value leads to output/high_value_leads.parquet")

# Save GA4 reports
print("\n" + "="*80)
print("SAVING GA4 REPORTS")
print("="*80)

os.makedirs('output/ga4_reports', exist_ok=True)
ga4_events_report.write_parquet('output/ga4_reports/events_report.parquet')
ga4_conversions_report.write_parquet('output/ga4_reports/conversions_report.parquet')
ga4_traffic_acquisition.write_parquet('output/ga4_reports/traffic_acquisition.parquet')
ga4_user_acquisition.write_parquet('output/ga4_reports/user_acquisition.parquet')

print("✓ GA4 Events Report")
print("✓ GA4 Conversions Report")  
print("✓ GA4 Traffic Acquisition Report")
print("✓ GA4 User Acquisition Report")

print("\n" + "="*80)
print("SUMMARY")
print("="*80)
print(f"Total events processed: {len(events):,}")
print(f"Total users tracked: {len(user_funnel_state):,}")
print(f"High-value leads: {len(high_value_leads):,}")
print(f"GA4 report records: {len(ga4_events_report) + len(ga4_conversions_report) + len(ga4_traffic_acquisition) + len(ga4_user_acquisition):,}")
print("\nNext steps:")
print("1. Load user_funnel_state.parquet to CRM (Pipedrive)")
print("2. Load GA4 reports to BigQuery/DuckDB")
print("3. Generate Stripe subscriptions for customers")
print("4. Set up automated lead scoring based on funnel stage + engagement")
