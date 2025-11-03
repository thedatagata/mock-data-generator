"""
Stripe Customers Generator
~97K total: 5% identified leads + anonymous purchasers
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared_config import *

from faker import Faker

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

print("Loading lead data...")
byd_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_bookyourdata/bookyourdata/1762095548.474671.701ad8b601.parquet')
uplead_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_upleads/uplead/1762095612.4264941.2fb362f699.parquet')

purchased_leads = pd.concat([byd_df, uplead_df], ignore_index=True)
print(f"Loaded {len(purchased_leads)} leads")


@dlt.resource(write_disposition="append", table_name="customers")
def customers():
    """Generate Stripe customers: identified leads + anonymous purchasers"""
    
    total_leads = len(purchased_leads) + sum(get_daily_metrics(d)['identified_leads'] for d in range(DAYS_OF_DATA))
    lead_customers = int(total_leads * LEAD_CONVERSION_RATE)
    
    # Anonymous purchases: 1.8% of sessions over 365 days
    total_sessions = sum(get_daily_metrics(d)['sessions'] for d in range(DAYS_OF_DATA))
    anonymous_purchase_rate = (ANONYMOUS_CONVERSION_RATES['add_to_cart'] * 
                               ANONYMOUS_CONVERSION_RATES['checkout_start'] * 
                               ANONYMOUS_CONVERSION_RATES['purchase'])
    anonymous_customers = int(total_sessions * (1 - LEAD_PERCENTAGE_OF_TRAFFIC) * anonymous_purchase_rate)
    
    total_customers = lead_customers + anonymous_customers
    print(f"Generating {lead_customers} lead customers + {anonymous_customers} anonymous = {total_customers} total")
    
    # 1. Lead customers (from purchased lists)
    paying_leads = purchased_leads.sample(min(int(len(purchased_leads) * LEAD_CONVERSION_RATE), len(purchased_leads)))
    
    for idx, lead in paying_leads.iterrows():
        created_time = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 30))
        
        yield {
            'id': f"cus_{fake.bothify(text='??????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')}",
            'object': 'customer',
            'address': {
                'line1': lead.get('address', fake.street_address()),
                'city': lead.get('city', fake.city()),
                'state': lead.get('state', fake.state_abbr()),
                'postal_code': fake.postcode(),
                'country': lead.get('country', 'US'),
            },
            'balance': 0,
            'created': int(created_time.timestamp()),
            'currency': 'usd',
            'default_source': None,
            'delinquent': False,
            'description': f"Customer from {lead.get('company', fake.company())}",
            'email': lead.get('email', fake.email()),
            'invoice_prefix': fake.bothify(text='????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'),
            'invoice_settings': {
                'custom_fields': None,
                'default_payment_method': f"pm_{fake.bothify(text='????????????????????', letters='abcdefghijklmnopqrstuvwxyz0123456789')}",
                'footer': None,
                'rendering_options': None,
            },
            'livemode': False,
            'metadata': {
                'company': lead.get('company', fake.company()),
                'industry': lead.get('industry', fake.bs()),
                'source': 'identified_lead',
            },
            'name': f"{lead.get('first_name', fake.first_name())} {lead.get('last_name', fake.last_name())}",
            'next_invoice_sequence': random.randint(1, 24),
            'phone': lead.get('phone', fake.phone_number()),
            'preferred_locales': ['en-US'],
            'shipping': None,
            'tax_exempt': 'none',
            'test_clock': None,
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'identified_lead',
        }
    
    # 2. Form-fill lead customers
    form_fill_lead_customers = lead_customers - len(paying_leads)
    
    for _ in range(form_fill_lead_customers):
        created_time = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA))
        
        yield {
            'id': f"cus_{fake.bothify(text='??????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')}",
            'object': 'customer',
            'address': {
                'line1': fake.street_address(),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'postal_code': fake.postcode(),
                'country': random.choices([g['country'][:2] for g in GEO_DISTRIBUTION], 
                                        weights=[g['weight'] for g in GEO_DISTRIBUTION])[0],
            },
            'balance': 0,
            'created': int(created_time.timestamp()),
            'currency': 'usd',
            'default_source': None,
            'delinquent': False,
            'description': f"Customer from {fake.company()}",
            'email': fake.email(),
            'invoice_prefix': fake.bothify(text='????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'),
            'invoice_settings': {
                'custom_fields': None,
                'default_payment_method': f"pm_{fake.bothify(text='????????????????????', letters='abcdefghijklmnopqrstuvwxyz0123456789')}",
                'footer': None,
                'rendering_options': None,
            },
            'livemode': False,
            'metadata': {
                'company': fake.company(),
                'industry': fake.bs(),
                'source': random.choice(['google_ads', 'facebook_ads', 'organic_web']),
            },
            'name': fake.name(),
            'next_invoice_sequence': random.randint(1, 24),
            'phone': fake.phone_number(),
            'preferred_locales': ['en-US'],
            'shipping': None,
            'tax_exempt': 'none',
            'test_clock': None,
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'form_fill_lead',
        }
    
    # 3. Anonymous purchasers (no lead trail)
    for _ in range(anonymous_customers):
        created_time = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA))
        
        yield {
            'id': f"cus_{fake.bothify(text='??????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')}",
            'object': 'customer',
            'address': {
                'line1': fake.street_address(),
                'city': fake.city(),
                'state': fake.state_abbr(),
                'postal_code': fake.postcode(),
                'country': random.choices([g['country'][:2] for g in GEO_DISTRIBUTION], 
                                        weights=[g['weight'] for g in GEO_DISTRIBUTION])[0],
            },
            'balance': 0,
            'created': int(created_time.timestamp()),
            'currency': 'usd',
            'default_source': None,
            'delinquent': False,
            'description': None,
            'email': fake.email(),
            'invoice_prefix': fake.bothify(text='????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'),
            'invoice_settings': {
                'custom_fields': None,
                'default_payment_method': f"pm_{fake.bothify(text='????????????????????', letters='abcdefghijklmnopqrstuvwxyz0123456789')}",
                'footer': None,
                'rendering_options': None,
            },
            'livemode': False,
            'metadata': {
                'source': 'anonymous_purchase',
            },
            'name': fake.name(),
            'next_invoice_sequence': random.randint(1, 12),
            'phone': fake.phone_number() if random.random() < 0.7 else None,
            'preferred_locales': ['en-US'],
            'shipping': None,
            'tax_exempt': 'none',
            'test_clock': None,
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'anonymous_purchase',
        }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_customers",
        destination="filesystem",
        dataset_name="stripe"
    )
    
    load_info = pipeline.run(customers(), loader_file_format="parquet")
    
    print(f"\n✓ Stripe customers: ~97K total (leads + anonymous)")
