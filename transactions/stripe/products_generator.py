"""
Stripe Products Generator
Product offerings for subscription tiers
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


@dlt.resource(write_disposition="append", table_name="products")
def products():
    """Generate product offerings"""
    
    tiers = [
        {
            'name': 'Starter',
            'description': 'Perfect for individuals and small teams getting started',
            'statement_descriptor': 'STARTER PLAN',
        },
        {
            'name': 'Professional',
            'description': 'Advanced features for growing teams and businesses',
            'statement_descriptor': 'PRO PLAN',
        },
        {
            'name': 'Business',
            'description': 'Comprehensive solution for established businesses',
            'statement_descriptor': 'BUSINESS PLAN',
        },
        {
            'name': 'Enterprise',
            'description': 'Custom solutions with dedicated support for large organizations',
            'statement_descriptor': 'ENTERPRISE',
        },
    ]
    
    for tier in tiers:
        yield {
            'id': f"prod_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'product',
            'active': True,
            'created': int(START_DATE.timestamp()),
            'default_price': f"price_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'description': tier['description'],
            'images': [],
            'marketing_features': [],
            'livemode': False,
            'metadata': {'tier': tier['name']},
            'name': f"{tier['name']} Plan",
            'package_dimensions': None,
            'shippable': None,
            'statement_descriptor': tier['statement_descriptor'],
            'tax_code': None,
            'unit_label': None,
            'updated': int(START_DATE.timestamp()),
            'url': None,
            '_generated_at': datetime.now().isoformat(),
        }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_products",
        destination="filesystem",
        dataset_name="stripe"
    )
    
    load_info = pipeline.run(products(), loader_file_format="parquet")
    
    print("\n✓ Stripe products: 4 subscription tiers")
