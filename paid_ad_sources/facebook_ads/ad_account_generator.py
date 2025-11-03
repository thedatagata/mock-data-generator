# ad_account_generator.py
"""
Facebook Ads Ad Account Data Generator
Generates ad account data following Facebook Ads ad_account schema
"""
import dlt
from datetime import datetime

@dlt.resource(write_disposition="replace", table_name="ad_account")
def ad_accounts():
    """Generate single Facebook Ads ad account"""
    
    yield {
        'account_id': "act_1234567890",
        'id': "act_1234567890",
        'account_status': 1,  # ACTIVE
        'age': 730,
        'name': 'Primary Ad Account',
        'amount_spent': '250000.00',
        'balance': '5000.00',
        'spend_cap': '500000.00',
        'currency': 'USD',
        'business': {'id': 'business_001', 'name': 'Primary Business'},
        'business_name': 'Primary Business Company',
        'business_city': 'San Francisco',
        'business_state': 'CA',
        'business_country_code': 'US',
        'business_street': '123 Market St',
        'business_street2': 'Suite 500',
        'business_zip': '94103',
        'can_create_brand_lift_study': True,
        'capabilities': ['INSIGHTS', 'CAN_USE_REACH_AND_FREQUENCY', 'DIRECT_SALES'],
        'is_direct_deals_enabled': False,
        'is_notifications_enabled': True,
        'is_personal': 0,
        'is_prepay_account': False,
        'is_tax_id_required': False,
        'is_attribution_spec_system_default': True,
        'is_in_3ds_authorization_enabled_market': True,
        'created_time': '2023-01-15T10:30:00+0000',
        'has_migrated_permissions': True,
        'offsite_pixels_tos_accepted': True,
        'owner': 1234567,
        'fb_entity': 2345678,
        'funding_source': 3456789,
        'partner': None,
        'media_agency': None,
        'end_advertiser': None,
        'end_advertiser_name': None,
        'funding_source_details': {
            'id': 'funding_001',
            'display_string': '****1234',
            'type': 1
        },
        'extended_credit_invoice_group': None,
        'rf_spec': {},
        'tos_accepted': {'web': 1, 'mobile': 1},
        'user_tos_accepted': {'web': 1, 'mobile': 1},
        'min_campaign_group_spend_cap': 100,
        'min_daily_budget': 1,
        'disable_reason': None,
        'failed_delivery_checks': [],
        'user_tasks': [],
        'tax_id': None,
        'tax_id_status': None,
        'tax_id_type': None,
        'timezone_id': 1,
        'timezone_name': 'America/Los_Angeles',
        'timezone_offset_hours_utc': -8,
        'io_number': None,
        'line_numbers': None,
        '_generated_at': datetime.now().isoformat(),
    }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads_accounts",
        destination="filesystem",
        dataset_name="facebook_ads"
    )
    
    load_info = pipeline.run(ad_accounts(), loader_file_format="parquet")
    print("✓ Ad account generated")