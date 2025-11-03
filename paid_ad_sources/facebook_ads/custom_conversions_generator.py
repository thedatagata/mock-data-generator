# custom_conversions_generator.py
"""
Facebook Ads Custom Conversions Data Generator
Generates custom conversion definitions following Facebook Ads custom_conversions schema
"""
import dlt
from datetime import datetime, timedelta
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

@dlt.resource(write_disposition="replace", table_name="custom_conversions")
def custom_conversions():
    """Generate Facebook Ads custom conversion definitions"""
    
    account_id = "act_1234567890"
    business_id = "business_001"
    pixel_id = "238571234567890"
    
    conversions = [
        {
            'name': 'Purchase Completed',
            'custom_event_type': 'PURCHASE',
            'description': 'User completed a purchase transaction',
            'rule': '{"type":"url","rule":"url_contains","value":"checkout/confirmation"}',
            'default_conversion_value': 99.99,
        },
        {
            'name': 'Sign Up Completed',
            'custom_event_type': 'COMPLETE_REGISTRATION',
            'description': 'User successfully registered for an account',
            'rule': '{"type":"url","rule":"url_contains","value":"signup/success"}',
            'default_conversion_value': 0,
        },
        {
            'name': 'Add to Cart',
            'custom_event_type': 'ADD_TO_CART',
            'description': 'User added product to shopping cart',
            'rule': '{"type":"event","rule":"event_equals","value":"AddToCart"}',
            'default_conversion_value': 0,
        },
        {
            'name': 'Lead Form Submitted',
            'custom_event_type': 'LEAD',
            'description': 'User submitted contact form',
            'rule': '{"type":"url","rule":"url_contains","value":"contact/thank-you"}',
            'default_conversion_value': 25.00,
        },
        {
            'name': 'Free Trial Started',
            'custom_event_type': 'START_TRIAL',
            'description': 'User initiated free trial',
            'rule': '{"type":"url","rule":"url_contains","value":"trial/started"}',
            'default_conversion_value': 0,
        },
    ]
    
    for i, conv in enumerate(conversions):
        conversion_id = f"23857{70000000 + i}"
        created = datetime.now() - timedelta(days=365)
        first_fired = datetime.now() - timedelta(days=300)
        last_fired = datetime.now() - timedelta(days=1)
        
        yield {
            'id': conversion_id,
            'account_id': account_id,
            'business': business_id,
            'name': conv['name'],
            'description': conv['description'],
            'custom_event_type': conv['custom_event_type'],
            'rule': conv['rule'],
            'default_conversion_value': conv['default_conversion_value'],
            'event_source_type': 'WEB',
            'data_sources': [{'id': pixel_id, 'source_type': 'PIXEL'}],
            'creation_time': created.strftime('%Y-%m-%dT%H:%M:%S+0000'),
            'first_fired_time': first_fired.strftime('%Y-%m-%dT%H:%M:%S+0000'),
            'last_fired_time': last_fired.strftime('%Y-%m-%dT%H:%M:%S+0000'),
            'is_archived': False,
            'is_unavailable': False,
            'retention_days': 90,
            'offline_conversion_data_set': None,
            '_generated_at': datetime.now().isoformat(),
        }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads_custom_conversions",
        destination="filesystem",
        dataset_name="facebook_ads"
    )
    
    load_info = pipeline.run(custom_conversions(), loader_file_format="parquet")
    print("✓ Generated 5 custom conversions")
