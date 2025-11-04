"""
Pipedrive Persons Generator
Individual contacts from purchased leads + form fills
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


@dlt.resource(write_disposition="append", table_name="persons")
def persons():
    """Generate persons: leads + form fills + anonymous purchasers"""
    
    person_id = 1
    
    # Calculate anonymous purchasers
    total_sessions = sum(get_daily_metrics(d)['sessions'] for d in range(DAYS_OF_DATA))
    anonymous_rate = (ANONYMOUS_CONVERSION_RATES['add_to_cart'] * 
                     ANONYMOUS_CONVERSION_RATES['checkout_start'] * 
                     ANONYMOUS_CONVERSION_RATES['purchase'])
    anonymous_customers = int(total_sessions * (1 - LEAD_PERCENTAGE_OF_TRAFFIC) * anonymous_rate)
    
    # 1. Persons from purchased leads
    for idx, lead in purchased_leads.iterrows():
        days_offset = random.randint(0, DAYS_OF_DATA - 30)
        add_time = START_DATE + timedelta(days=days_offset, hours=random.randint(8, 18))
        update_time = add_time + timedelta(days=random.randint(0, 60))
        
        owner = random.choice(SALES_REPS)
        is_customer = random.random() < LEAD_CONVERSION_RATE
        
        yield {
            'id': person_id,
            'name': f"{lead.get('first_name', fake.first_name())} {lead.get('last_name', fake.last_name())}",
            'first_name': lead.get('first_name', fake.first_name()),
            'last_name': lead.get('last_name', fake.last_name()),
            'owner_id': owner,
            'org_id': random.randint(1, 100000),
            'add_time': add_time.isoformat(),
            'update_time': update_time.isoformat(),
            
            'email': [
                {'value': lead.get('email', fake.email()), 'primary': True, 'label': 'work'}
            ],
            'phone': [
                {'value': lead.get('phone', fake.phone_number()), 'primary': True, 'label': 'work'}
            ],
            
            'is_deleted': False,
            'visible_to': 7,
            'label_ids': [1, 2] if is_customer else [1],
            'picture_id': None,
            'notes': None,
            
            'im': [],
            'birthday': None,
            'job_title': lead.get('title', fake.job()),
            
            'address': {
                'value': lead.get('address', fake.street_address()),
                'country': lead.get('country', fake.country()),
                'admin_area_level_1': lead.get('state', fake.state()),
                'locality': lead.get('city', fake.city()),
                'route': fake.street_name(),
                'street_number': str(random.randint(1, 9999)),
                'postal_code': fake.postcode(),
            },
            
            # Activity metrics
            'next_activity_id': None if is_customer else random.randint(1, 1000),
            'last_activity_id': random.randint(1, 1000) if random.random() < 0.8 else None,
            'activities_count': random.randint(0, 25),
            'done_activities_count': random.randint(0, 20),
            'undone_activities_count': random.randint(0, 5),
            
            # Deal metrics
            'open_deals_count': 0 if is_customer else random.randint(0, 2),
            'related_open_deals_count': 0 if is_customer else random.randint(0, 1),
            'closed_deals_count': 1 if is_customer else random.randint(0, 1),
            'related_closed_deals_count': 1 if is_customer else 0,
            'participant_open_deals_count': 0 if is_customer else random.randint(0, 1),
            'participant_closed_deals_count': 1 if is_customer else 0,
            'won_deals_count': 1 if is_customer else 0,
            'related_won_deals_count': 1 if is_customer else 0,
            'lost_deals_count': random.randint(0, 1),
            'related_lost_deals_count': 0,
            
            # Engagement
            'email_messages_count': random.randint(0, 60),
            'files_count': random.randint(0, 10),
            'notes_count': random.randint(0, 15),
            'followers_count': random.randint(1, 3),
            'last_incoming_mail_time': (update_time - timedelta(days=random.randint(1, 30))).isoformat() if random.random() < 0.7 else None,
            'last_outgoing_mail_time': update_time.isoformat() if random.random() < 0.8 else None,
            
            # Custom fields
            'linkedin_url': lead.get('linkedin', f"https://linkedin.com/in/{fake.user_name()}"),
            'lead_source': 'Purchased List',
            'company': lead.get('company', fake.company()),
            'industry': lead.get('industry', fake.bs()),
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'purchased_list',
            '_is_customer': is_customer,
        }
        
        person_id += 1
    
    # 2. Persons from form fills
    form_fill_count = sum(get_daily_metrics(d)['identified_leads'] for d in range(DAYS_OF_DATA))
    
    for _ in range(form_fill_count):
        days_offset = random.randint(0, DAYS_OF_DATA)
        add_time = START_DATE + timedelta(days=days_offset, hours=random.randint(8, 22))
        update_time = add_time + timedelta(hours=random.randint(1, 720))
        
        owner = random.choice(SALES_REPS)
        is_customer = random.random() < (LEAD_CONVERSION_RATE * 0.8)
        
        first = fake.first_name()
        last = fake.last_name()
        email = fake.email()
        
        yield {
            'id': person_id,
            'name': f"{first} {last}",
            'first_name': first,
            'last_name': last,
            'owner_id': owner,
            'org_id': random.randint(1, 150000),
            'add_time': add_time.isoformat(),
            'update_time': update_time.isoformat(),
            
            'email': [
                {'value': email, 'primary': True, 'label': 'work'}
            ],
            'phone': [
                {'value': fake.phone_number(), 'primary': True, 'label': 'work'}
            ],
            
            'is_deleted': False,
            'visible_to': 7,
            'label_ids': [1, 2] if is_customer else [1],
            'picture_id': None,
            'notes': 'Contact from web form',
            
            'im': [],
            'birthday': None,
            'job_title': fake.job(),
            
            'address': {
                'value': fake.street_address(),
                'country': random.choices([g['country'] for g in GEO_DISTRIBUTION], 
                                        weights=[g['weight'] for g in GEO_DISTRIBUTION])[0],
                'admin_area_level_1': fake.state(),
                'locality': fake.city(),
                'route': fake.street_name(),
                'street_number': str(random.randint(1, 9999)),
                'postal_code': fake.postcode(),
            },
            
            'next_activity_id': None if is_customer else random.randint(1, 1000),
            'last_activity_id': random.randint(1, 1000) if random.random() < 0.7 else None,
            'activities_count': random.randint(0, 20),
            'done_activities_count': random.randint(0, 15),
            'undone_activities_count': random.randint(0, 5),
            
            'open_deals_count': 0 if is_customer else random.randint(0, 2),
            'related_open_deals_count': 0 if is_customer else random.randint(0, 1),
            'closed_deals_count': 1 if is_customer else random.randint(0, 1),
            'related_closed_deals_count': 1 if is_customer else 0,
            'participant_open_deals_count': 0 if is_customer else random.randint(0, 1),
            'participant_closed_deals_count': 1 if is_customer else 0,
            'won_deals_count': 1 if is_customer else 0,
            'related_won_deals_count': 1 if is_customer else 0,
            'lost_deals_count': random.randint(0, 1),
            'related_lost_deals_count': 0,
            
            'email_messages_count': random.randint(0, 40),
            'files_count': random.randint(0, 8),
            'notes_count': random.randint(0, 12),
            'followers_count': random.randint(1, 3),
            'last_incoming_mail_time': (update_time - timedelta(hours=random.randint(1, 72))).isoformat() if random.random() < 0.6 else None,
            'last_outgoing_mail_time': update_time.isoformat() if random.random() < 0.7 else None,
            
            'linkedin_url': f"https://linkedin.com/in/{fake.user_name()}",
            'lead_source': random.choice(['Google Ads Form', 'Facebook Ads Form', 'Website Form']),
            'company': fake.company(),
            'industry': fake.bs(),
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'form_fill',
            '_is_customer': is_customer,
        }
        
        person_id += 1
    
    # 3. Anonymous purchasers (added to CRM post-purchase)
    for _ in range(anonymous_customers):
        purchase_time = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA))
        
        yield {
            'id': person_id,
            'name': fake.name(),
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'owner_id': random.choice(SALES_REPS),
            'org_id': random.randint(1, 240000),
            'add_time': purchase_time.isoformat(),
            'update_time': purchase_time.isoformat(),
            
            'email': [{'value': fake.email(), 'primary': True, 'label': 'work'}],
            'phone': [{'value': fake.phone_number(), 'primary': True, 'label': 'work'}] if random.random() < 0.7 else [],
            
            'is_deleted': False,
            'visible_to': 7,
            'label_ids': [1, 2],
            'picture_id': None,
            'notes': 'Customer from self-serve purchase',
            'im': [],
            'birthday': None,
            'job_title': fake.job(),
            
            'address': {
                'value': fake.street_address(),
                'country': random.choices([g['country'] for g in GEO_DISTRIBUTION], 
                                        weights=[g['weight'] for g in GEO_DISTRIBUTION])[0],
                'admin_area_level_1': fake.state(),
                'locality': fake.city(),
                'route': fake.street_name(),
                'street_number': str(random.randint(1, 9999)),
                'postal_code': fake.postcode(),
            },
            
            'next_activity_id': None,
            'last_activity_id': None,
            'activities_count': 0,
            'done_activities_count': 0,
            'undone_activities_count': 0,
            
            'open_deals_count': 0,
            'related_open_deals_count': 0,
            'closed_deals_count': 1,
            'related_closed_deals_count': 1,
            'participant_open_deals_count': 0,
            'participant_closed_deals_count': 1,
            'won_deals_count': 1,
            'related_won_deals_count': 1,
            'lost_deals_count': 0,
            'related_lost_deals_count': 0,
            
            'email_messages_count': random.randint(0, 5),
            'files_count': 0,
            'notes_count': 0,
            'followers_count': 1,
            'last_incoming_mail_time': None,
            'last_outgoing_mail_time': None,
            
            'linkedin_url': None,
            'lead_source': 'Self-Serve Purchase',
            'company': fake.company(),
            'industry': fake.bs(),
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'anonymous_purchase',
            '_is_customer': True,
        }
        
        person_id += 1
    
    print(f"\nGenerated {person_id - 1} persons")


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive_persons",
        destination="filesystem",
        dataset_name="pipedrive"
    )
    
    load_info = pipeline.run(persons(), loader_file_format="parquet")
    
    print(f"\n✓ Pipedrive persons: ~{len(purchased_leads) + 73000} contacts")