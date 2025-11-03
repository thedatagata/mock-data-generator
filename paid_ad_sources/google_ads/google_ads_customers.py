# google_ads_customers.py
"""
Google Ads Customer Data Generator
"""
import dlt
from datetime import datetime

@dlt.resource(write_disposition="replace", table_name="customer")
def customers():
    """Generate single Google Ads customer account"""
    
    yield {
        'customer_id': 1234567890,
        'resource_name': 'customers/1234567890',
        'descriptive_name': 'B2B Tech Solutions Marketing',
        'currency_code': 'USD',
        'time_zone': 'America/New_York',
        'auto_tagging_enabled': True,
        'test_account': False,
        'manager': False,
        'status': 'ENABLED',
        'tracking_url_template': 'https://example.com/?utm_source=google&utm_medium=cpc&utm_campaign={campaignid}&utm_content={adgroupid}&utm_term={keyword}',
        'final_url_suffix': 'gclid={gclid}',
        'conversion_tracking_setting_conversion_tracking_id': 987654321,
        'conversion_tracking_setting_conversion_tracking_status': 'CONVERSION_TRACKING_ENABLED',
        'conversion_tracking_setting_enhanced_conversions_for_leads_enabled': True,
        'conversion_tracking_setting_google_ads_conversion_customer': 'customers/1234567890',
        'call_reporting_setting_call_reporting_enabled': True,
        'call_reporting_setting_call_conversion_reporting_enabled': True,
        'call_reporting_setting_call_conversion_action': 'customers/1234567890/conversionActions/123456',
        'remarketing_setting_google_global_site_tag': 'gtag(\'config\', \'AW-1234567890\');',
        'optimization_score': 0.87,
        'optimization_score_weight': 1.0,
        'customer_agreement_setting_accepted_lead_form_terms': True,
        'has_partners_badge': False,
        'pay_per_conversion_eligibility_failure_reasons': [],
        'image_asset_auto_migration_done': True,
        'image_asset_auto_migration_done_date_time': '2024-01-15 10:30:00',
        'location_asset_auto_migration_done': True,
        'location_asset_auto_migration_done_date_time': '2024-01-15 10:30:00',
        'video_brand_safety_suitability': 'STANDARD_INVENTORY',
        '_generated_at': datetime.now().isoformat(),
    }

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_customers",
        destination="filesystem",
        dataset_name="google_ads"
    )
    load_info = pipeline.run(customers(), loader_file_format="parquet")
    print("✓ Customer generated")
