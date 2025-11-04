"""
Pipedrive Organizations Generator
Companies from purchased leads + form-fill leads (deduplicated)
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

print("Loading purchased lead data...")
byd_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_bookyourdata/bookyourdata/1762095548.474671.701ad8b601.parquet')
uplead_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_upleads/uplead/1762095612.4264941.2fb362f699.parquet')

purchased_leads = pd.concat([byd_df, uplead_df], ignore_index=True)
print(f"Loaded {len(purchased_leads)} purchased leads")

SALES_REPS = [1, 2, 3, 4, 5]


@dlt.resource(write_disposition="append", table_name="organizations")
def organizations():
    """Generate unique organizations from leads + anonymous customers"""
    
    seen_companies = set()
    org_id = 1
    
    # Calculate anonymous purchasers
    total_sessions = sum(get_daily_metrics(d)['sessions'] for d in range(DAYS_OF_DATA))
    anonymous_rate = (ANONYMOUS_CONVERSION_RATES['add_to_cart'] * 
                     ANONYMOUS_CONVERSION_RATES['checkout_start'] * 
                     ANONYMOUS_CONVERSION_RATES['purchase'])
    anonymous_customers = int(total_sessions * (1 - LEAD_PERCENTAGE_OF_TRAFFIC) * anonymous_rate)
    
    # 1. Organizations from purchased leads
    for idx, lead in purchased_leads.iterrows():
        company_name = lead.get('company', fake.company())
        
        if company_name in seen_companies:
            continue
        seen_companies.add(company_name)
        
        days_offset = random.randint(0, DAYS_OF_DATA - 30)
        add_time = START_DATE + timedelta(days=days_offset, hours=random.randint(8, 18))
        update_time = add_time + timedelta(days=random.randint(0, 60))
        
        owner = random.choice(SALES_REPS)
        is_customer = random.random() < LEAD_CONVERSION_RATE
        
        yield {
            'id': org_id,
            'name': company_name,
            'owner_id': owner,
            'org_id': org_id,
            'add_time': add_time.isoformat(),
            'update_time': update_time.isoformat(),
            
            'address': lead.get('address', fake.street_address()),
            'country': lead.get('country', fake.country()),
            'admin_area_level_1': lead.get('state', fake.state()),
            'locality': lead.get('city', fake.city()),
            'route': fake.street_name(),
            'street_number': str(random.randint(1, 9999)),
            'postal_code': fake.postcode(),
            
            'is_deleted': False,
            'visible_to': 7,
            'label_ids': [1, 2] if is_customer else [1],
            
            # Activity metrics
            'next_activity_id': None if is_customer else random.randint(1, 1000),
            'last_activity_id': random.randint(1, 1000) if random.random() < 0.8 else None,
            'activities_count': random.randint(0, 20),
            'done_activities_count': random.randint(0, 15),
            'undone_activities_count': random.randint(0, 5),
            
            # Deal metrics
            'open_deals_count': 0 if is_customer else random.randint(0, 3),
            'related_open_deals_count': 0 if is_customer else random.randint(0, 2),
            'closed_deals_count': 1 if is_customer else random.randint(0, 2),
            'related_closed_deals_count': 1 if is_customer else 0,
            'won_deals_count': 1 if is_customer else 0,
            'related_won_deals_count': 1 if is_customer else 0,
            'lost_deals_count': random.randint(0, 1),
            'related_lost_deals_count': 0,
            
            # Engagement metrics
            'people_count': random.randint(1, 5),
            'email_messages_count': random.randint(0, 50),
            'files_count': random.randint(0, 10),
            'notes_count': random.randint(0, 15),
            'followers_count': random.randint(1, 3),
            
            # Custom fields
            'industry': lead.get('industry', fake.bs()),
            'company_size': lead.get('employees', random.choice(['1-10', '11-50', '51-200', '201-500', '500+'])),
            'website': f"https://{company_name.lower().replace(' ', '')}.com",
            'annual_revenue': random.randint(100000, 50000000),
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'purchased_list',
            '_is_customer': is_customer,
        }
        
        org_id += 1
    
    # 2. Organizations from form fills (Faker companies)
    form_fill_count = sum(get_daily_metrics(d)['identified_leads'] for d in range(DAYS_OF_DATA))
    faker_orgs = int(form_fill_count * 0.7)  # ~70% unique companies from form fills
    
    for _ in range(faker_orgs):
        company_name = fake.company()
        
        if company_name in seen_companies:
            continue
        seen_companies.add(company_name)
        
        days_offset = random.randint(0, DAYS_OF_DATA)
        add_time = START_DATE + timedelta(days=days_offset, hours=random.randint(8, 22))
        update_time = add_time + timedelta(hours=random.randint(1, 720))
        
        owner = random.choice(SALES_REPS)
        is_customer = random.random() < (LEAD_CONVERSION_RATE * 0.8)  # Slightly lower for web leads
        
        yield {
            'id': org_id,
            'name': company_name,
            'owner_id': owner,
            'org_id': org_id,
            'add_time': add_time.isoformat(),
            'update_time': update_time.isoformat(),
            
            'address': fake.street_address(),
            'country': random.choices([g['country'] for g in GEO_DISTRIBUTION], 
                                    weights=[g['weight'] for g in GEO_DISTRIBUTION])[0],
            'admin_area_level_1': fake.state(),
            'locality': fake.city(),
            'route': fake.street_name(),
            'street_number': str(random.randint(1, 9999)),
            'postal_code': fake.postcode(),
            
            'is_deleted': False,
            'visible_to': 7,
            'label_ids': [1, 2] if is_customer else [1],
            
            'next_activity_id': None if is_customer else random.randint(1, 1000),
            'last_activity_id': random.randint(1, 1000) if random.random() < 0.7 else None,
            'activities_count': random.randint(0, 15),
            'done_activities_count': random.randint(0, 10),
            'undone_activities_count': random.randint(0, 5),
            
            'open_deals_count': 0 if is_customer else random.randint(0, 2),
            'related_open_deals_count': 0 if is_customer else random.randint(0, 1),
            'closed_deals_count': 1 if is_customer else random.randint(0, 1),
            'related_closed_deals_count': 1 if is_customer else 0,
            'won_deals_count': 1 if is_customer else 0,
            'related_won_deals_count': 1 if is_customer else 0,
            'lost_deals_count': random.randint(0, 1),
            'related_lost_deals_count': 0,
            
            'people_count': random.randint(1, 4),
            'email_messages_count': random.randint(0, 30),
            'files_count': random.randint(0, 8),
            'notes_count': random.randint(0, 12),
            'followers_count': random.randint(1, 3),
            
            'industry': fake.bs(),
            'company_size': random.choice(['1-10', '11-50', '51-200', '201-500', '500+']),
            'website': f"https://{company_name.lower().replace(' ', '')}.com",
            'annual_revenue': random.randint(100000, 50000000),
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'form_fill',
            '_is_customer': is_customer,
        }
        
        org_id += 1
    
    # 3. Organizations from anonymous purchasers (post-purchase)
    anonymous_orgs = int(anonymous_customers * 0.7)
    
    for _ in range(anonymous_orgs):
        company_name = fake.company()
        if company_name in seen_companies:
            continue
        seen_companies.add(company_name)
        
        purchase_time = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA))
        
        yield {
            'id': org_id,
            'name': company_name,
            'owner_id': random.choice(SALES_REPS),
            'org_id': org_id,
            'add_time': purchase_time.isoformat(),
            'update_time': purchase_time.isoformat(),
            
            'address': fake.street_address(),
            'country': random.choices([g['country'] for g in GEO_DISTRIBUTION], 
                                    weights=[g['weight'] for g in GEO_DISTRIBUTION])[0],
            'admin_area_level_1': fake.state(),
            'locality': fake.city(),
            'route': fake.street_name(),
            'street_number': str(random.randint(1, 9999)),
            'postal_code': fake.postcode(),
            
            'is_deleted': False,
            'visible_to': 7,
            'label_ids': [1, 2],
            
            'next_activity_id': None,
            'last_activity_id': None,
            'activities_count': 0,
            'done_activities_count': 0,
            'undone_activities_count': 0,
            
            'open_deals_count': 0,
            'related_open_deals_count': 0,
            'closed_deals_count': 1,
            'related_closed_deals_count': 1,
            'won_deals_count': 1,
            'related_won_deals_count': 1,
            'lost_deals_count': 0,
            'related_lost_deals_count': 0,
            
            'people_count': 1,
            'email_messages_count': random.randint(0, 5),
            'files_count': 0,
            'notes_count': 0,
            'followers_count': 1,
            
            'industry': fake.bs(),
            'company_size': random.choice(['1-10', '11-50', '51-200']),
            'website': f"https://{company_name.lower().replace(' ', '')}.com",
            'annual_revenue': random.randint(100000, 10000000),
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'anonymous_purchase',
            '_is_customer': True,
        }
        
        org_id += 1
    
    print(f"\nGenerated {org_id - 1} unique organizations")


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive_orgs",
        destination="filesystem",
        dataset_name="pipedrive"
    )
    
    load_info = pipeline.run(organizations(), loader_file_format="parquet")
    
    print(f"\n✓ Pipedrive organizations: purchased + form-fill companies")