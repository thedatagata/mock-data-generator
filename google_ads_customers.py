"""
Google Ads Customer Data Generator
Generates customer-level account data following Google Ads customer schema
"""
import dlt
import pandas as pd
from datetime import datetime


# Load BookYourData and UpLead leads for context
print("Loading lead data...")
byd_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_bookyourdata/bookyourdata/1762095548.474671.701ad8b601.parquet')
uplead_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_upleads/uplead/1762095612.4264941.2fb362f699.parquet')

# Combine both sources
leads_df = pd.concat([byd_df, uplead_df], ignore_index=True)

# Analyze lead data for customer settings
top_industries = leads_df['byd_industries'].value_counts().head(5).index.tolist() if 'byd_industries' in leads_df.columns else []
avg_company_size = int(leads_df['employees'].mean()) if 'employees' in leads_df.columns else 0

print(f"Analyzed {len(leads_df)} leads ({len(byd_df)} BookYourData + {len(uplead_df)} UpLead)")
print(f"Top industries: {', '.join(top_industries[:3])}")


@dlt.resource(write_disposition="append", table_name="customer")
def customers():
    """Generate Google Ads customer account data"""
    yield {
        # Core identifiers
        'customer_id': 1234567890,
        'resource_name': 'customers/1234567890',
        
        # Account settings
        'descriptive_name': 'B2B Tech Solutions Marketing',
        'currency_code': 'USD',
        'time_zone': 'America/New_York',
        'auto_tagging_enabled': True,
        'test_account': False,
        'manager': False,
        'status': 'ENABLED',
        
        # Tracking configuration
        'tracking_url_template': 'https://example.com/?utm_source=google&utm_medium=cpc&utm_campaign={campaignid}&utm_content={adgroupid}&utm_term={keyword}',
        'final_url_suffix': 'gclid={gclid}',
        
        # Conversion tracking
        'conversion_tracking_setting_conversion_tracking_id': 987654321,
        'conversion_tracking_setting_conversion_tracking_status': 'CONVERSION_TRACKING_ENABLED',
        'conversion_tracking_setting_enhanced_conversions_for_leads_enabled': True,
        'conversion_tracking_setting_google_ads_conversion_customer': 'customers/1234567890',
        
        # Call reporting
        'call_reporting_setting_call_reporting_enabled': True,
        'call_reporting_setting_call_conversion_reporting_enabled': True,
        'call_reporting_setting_call_conversion_action': 'customers/1234567890/conversionActions/123456',
        
        # Remarketing
        'remarketing_setting_google_global_site_tag': 'gtag(\'config\', \'AW-1234567890\');',
        
        # Performance metrics
        'optimization_score': 0.87,
        'optimization_score_weight': 1.0,
        
        # Customer agreement settings
        'customer_agreement_setting_accepted_lead_form_terms': True,
        'has_partners_badge': False,
        'pay_per_conversion_eligibility_failure_reasons': [],
        
        # Asset migration status
        'image_asset_auto_migration_done': True,
        'image_asset_auto_migration_done_date_time': '2024-01-15 10:30:00',
        'location_asset_auto_migration_done': True,
        'location_asset_auto_migration_done_date_time': '2024-01-15 10:30:00',
        
        # Brand safety
        'video_brand_safety_suitability': 'STANDARD_INVENTORY',
        
        # Metadata
        '_generated_at': datetime.now().isoformat(),
        '_lead_count': len(leads_df),
        '_target_industries': '|'.join(top_industries)
    }


if __name__ == "__main__":
    # Configure pipeline for GCS
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_customers",
        destination="filesystem",
        dataset_name="google_ads"
    )
    
    # Run
    load_info = pipeline.run(
        customers(),
        loader_file_format="parquet"
    )
    
    print("\n✓ Customer data generated")
    print(f"Pipeline: {load_info.pipeline.pipeline_name}")
    print(f"Dataset: {load_info.dataset_name}")
    print(f"Loads: {len(load_info.loads_ids)}")
