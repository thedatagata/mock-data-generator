"""
Stripe Plans Generator
Subscription plans matching product tiers
"""
import dlt
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared_config import *
from datetime import datetime

from faker import Faker
fake = Faker()
Faker.seed(SEED)


@dlt.resource(write_disposition="append", table_name="plans")
def plans():
    """Generate subscription plans"""
    
    # Plan tiers with monthly pricing
    tiers = [
        {'name': 'Starter', 'monthly': 2900, 'annual': 29000},
        {'name': 'Professional', 'monthly': 9900, 'annual': 99000},
        {'name': 'Business', 'monthly': 19900, 'annual': 199000},
        {'name': 'Enterprise', 'monthly': 49900, 'annual': 499000},
    ]
    
    for tier in tiers:
        # Monthly plan
        yield {
            'id': f"plan_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'plan',
            'active': True,
            'amount': tier['monthly'],
            'amount_decimal': str(tier['monthly']),
            'billing_scheme': 'per_unit',
            'created': int(START_DATE.timestamp()),
            'currency': 'usd',
            'interval': 'month',
            'interval_count': 1,
            'livemode': False,
            'metadata': {'tier': tier['name']},
            'nickname': f"{tier['name']} Monthly",
            'product': f"prod_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'tiers_mode': None,
            'transform_usage': None,
            'trial_period_days': 14,
            'usage_type': 'licensed',
            '_generated_at': datetime.now().isoformat(),
        }
        
        # Annual plan (discounted)
        yield {
            'id': f"plan_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'plan',
            'active': True,
            'amount': tier['annual'],
            'amount_decimal': str(tier['annual']),
            'billing_scheme': 'per_unit',
            'created': int(START_DATE.timestamp()),
            'currency': 'usd',
            'interval': 'year',
            'interval_count': 1,
            'livemode': False,
            'metadata': {'tier': tier['name'], 'discount': '17%'},
            'nickname': f"{tier['name']} Annual",
            'product': f"prod_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'tiers_mode': None,
            'transform_usage': None,
            'trial_period_days': 14,
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
