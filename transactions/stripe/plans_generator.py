"""
Stripe Plans Generator
Subscription plans matching product tiers
"""
import dlt
import sys
import os

# Debug the path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'digital_analytics'))
from shared_config import *
print(f"Looking for: {config_path}")
print(f"Exists: {os.path.exists(config_path)}")

from shared_config import *
from datetime import datetime

from faker import Faker
fake = Faker()
Faker.seed(SEED)


@dlt.resource(write_disposition="append", table_name="plans")
def plans():
    """Generate subscription plans"""
    
    for product in STRIPE_PRODUCTS:
        # Monthly plan
        yield {
            'id': f"plan_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'plan',
            'active': True,
            'amount': product['price_monthly'],
            'amount_decimal': str(product['price_monthly']),
            'billing_scheme': 'per_unit',
            'created': int(START_DATE.timestamp()),
            'currency': 'usd',
            'interval': 'month',
            'interval_count': 1,
            'livemode': False,
            'metadata': {'tier': product['name']},
            'nickname': f"{product['name']} Monthly",
            'product': product['id'],
            'tiers_mode': None,
            'transform_usage': None,
            'trial_period_days': product['trial_days'],
            'usage_type': 'licensed',
            '_generated_at': datetime.now().isoformat(),
        }
        
        # Annual plan
        yield {
            'id': f"plan_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'plan',
            'active': True,
            'amount': product['price_annual'],
            'amount_decimal': str(product['price_annual']),
            'billing_scheme': 'per_unit',
            'created': int(START_DATE.timestamp()),
            'currency': 'usd',
            'interval': 'year',
            'interval_count': 1,
            'livemode': False,
            'metadata': {'tier': product['name'], 'discount': '17%'},
            'nickname': f"{product['name']} Annual",
            'product': product['id'],
            'tiers_mode': None,
            'transform_usage': None,
            'trial_period_days': product['trial_days'],
            'usage_type': 'licensed',
            '_generated_at': datetime.now().isoformat(),
        }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_plans",
        destination="filesystem",
        dataset_name="stripe"
    )
    
    load_info = pipeline.run(plans(), loader_file_format="parquet")
    
    print("\n✓ Stripe plans: 4 tiers × 2 billing periods = 8 plans")
