"""
Pipedrive Leads Generator
Creates leads from purchased lists + all form fills (trial, demo, pricing, contact, whitepaper, newsletter)
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
import uuid
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

# Pipedrive sales team
SALES_REPS = [
    {'id': 1, 'name': 'Sarah Johnson'},
    {'id': 2, 'name': 'Mike Chen'},
    {'id': 3, 'name': 'Emily Rodriguez'},
    {'id': 4, 'name': 'David Kim'},
    {'id': 5, 'name': 'Jessica Taylor'},
]

# Lead source mapping by form type
FORM_SOURCE_MAPPING = {
    'trial_signup': 'Trial Signup Form',
    'demo_request': 'Demo Request Form',
    'pricing_inquiry': 'Pricing Inquiry Form',
    'contact_us': 'Contact Us Form',
    'whitepaper_download': 'Content Download',
    'newsletter_signup': 'Newsletter Signup',
}


@dlt.resource(write_disposition="append", table_name="leads")
def leads():
    """Generate Pipedrive leads from purchased lists + all form types"""
    
    # ==========================================
    # 1. PURCHASED LEADS (from list providers)
    # ==========================================
    
    print(f"\nGenerating {len(purchased_leads)} purchased leads...")
    
    for idx, lead in purchased_leads.iterrows():
        # Stagger lead import across the year
        days_offset = random.randint(0, DAYS_OF_DATA - 30)
        add_time = START_DATE + timedelta(days=days_offset, hours=random.randint(8, 18))
        
        # Purchased leads are lower priority
        owner = random.choice(SALES_REPS)
        
        # Low conversion rate for purchased leads (2%)
        is_customer = random.random() < 0.02
        amount = random.randint(5000, 50000)
        
        yield {
            'id': str(uuid.uuid4()),
            'title': f"{lead.get('first_name', fake.first_name())} {lead.get('last_name', fake.last_name())} - {lead.get('company', fake.company())}",
            'owner_id': owner['id'],
            'creator_id': 1,  # Marketing ops imported
            'person_id': random.randint(1000, 999999),
            'organization_id': random.randint(1000, 999999),
            
            'source_name': 'Purchased List - BookYourData' if 'bookyourdata' in lead.get('source', '').lower() else 'Purchased List - UpLead',
            'origin': 'List Purchase',
            'origin_id': None,
            'channel': 'Outbound',
            'channel_id': 'List Import',
            
            'is_archived': is_customer or random.random() < 0.4,  # Many get archived (low quality)
            'was_seen': random.random() < 0.6,  # Sales team doesn't always review
            'value': amount,
            'currency': 'USD',
            'expected_close_date': (add_time + timedelta(days=random.randint(60, 120))).strftime('%Y-%m-%d') if not is_customer else None,
            'next_activity_id': None if is_customer else random.randint(1, 1000),
            
            'add_time': add_time.isoformat(),
            'update_time': (add_time + timedelta(days=random.randint(0, 30))).isoformat(),
            'visible_to': '3',
            'cc_email': f"company+{random.randint(1000, 9999)}@pipedrivemail.com",
            
            # Contact details
            'email': lead.get('email', fake.email()),
            'phone': lead.get('phone', fake.phone_number()),
            'job_title': lead.get('title', fake.job()),
            'company': lead.get('company', fake.company()),
            'industry': lead.get('industry', fake.bs()),
            'company_size': lead.get('employees', random.choice(['1-10', '11-50', '51-200', '201-500', '500+'])),
            'country': lead.get('country', fake.country()),
            'state': lead.get('state', fake.state()),
            'city': lead.get('city', fake.city()),
            
            # Lead tracking fields
            'lifecycle_stage': 'Lead',
            'form_type': 'purchased_lead',
            'trial_path': None,
            'trial_start_date': None,
            'sales_priority': 'low',
            'expected_activities': 2,  # Just cold outreach
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'purchased_list',
            '_converted_to_customer': is_customer,
        }
    
    # ==========================================
    # 2. FORM FILL LEADS (all types)
    # ==========================================
    
    # Calculate total form fills across all days
    total_form_fills = 0
    for day in range(DAYS_OF_DATA):
        daily_metrics = get_daily_metrics(day)
        total_form_fills += daily_metrics['identified_leads']
    
    print(f"Generating ~{total_form_fills} form-fill leads across {DAYS_OF_DATA} days...")
    
    # Distribute form fills across form types
    form_type_counts = {
        'trial_signup': int(total_form_fills * 0.35),
        'demo_request': int(total_form_fills * 0.25),
        'pricing_inquiry': int(total_form_fills * 0.15),
        'contact_us': int(total_form_fills * 0.15),
        'whitepaper_download': int(total_form_fills * 0.07),
        'newsletter_signup': int(total_form_fills * 0.03),
    }
    
    print(f"Form type distribution:")
    for form_type, count in form_type_counts.items():
        print(f"  - {form_type}: {count}")
    
    # Generate leads for each form type
    for form_type, count in form_type_counts.items():
        form_config = FORM_TYPES[form_type]
        
        for _ in range(count):
            # Random day across the year
            day = random.randint(0, DAYS_OF_DATA - 1)
            current_date = START_DATE + timedelta(days=day)
            add_time = current_date + timedelta(hours=random.randint(8, 22), minutes=random.randint(0, 59))
            
            # Determine traffic source
            if random.random() < 0.55:  # Paid traffic (google + facebook)
                if random.random() < 0.636:  # 35/(35+20) = Google Ads weight
                    source_name = 'Google Ads Form'
                    channel = 'Paid'
                    origin = 'Google Ads'
                else:
                    source_name = 'Facebook Ads Form'
                    channel = 'Paid'
                    origin = 'Facebook Ads'
            else:
                source_name = 'Website Form'
                channel = 'Inbound'
                origin = 'Organic'
            
            # Assign sales rep
            owner = random.choice(SALES_REPS)
            
            # Determine trial path if trial signup
            trial_path = None
            trial_start_date = None
            if form_type == 'trial_signup':
                trial_path = get_trial_path()
                trial_start_date = add_time.strftime('%Y-%m-%d')
            
            # Get conversion rate for this form type
            if form_type == 'trial_signup':
                # Trial conversion varies by path
                path_config = TRIAL_CONVERSION_PATHS[trial_path]
                conversion_rate = path_config['conversion_rate_to_paid']
            else:
                conversion_rate = form_config.get('conversion_rate_to_paid', 0.05)
            
            is_customer = random.random() < conversion_rate
            
            # Calculate deal value (higher for sales-assisted and enterprise forms)
            if form_type == 'trial_signup' and trial_path == 'sales_assisted':
                amount = random.randint(10000, 50000)
            elif form_type in ['demo_request', 'pricing_inquiry']:
                amount = random.randint(8000, 40000)
            else:
                amount = random.randint(3000, 15000)
            
            # Sales priority
            sales_priority = form_config.get('sales_priority', 'medium')
            
            # Expected activities
            if form_type == 'trial_signup':
                expected_activities = TRIAL_CONVERSION_PATHS[trial_path]['crm_activities']
            else:
                expected_activities = form_config.get('crm_activities', 3)
            
            # Determine if lead gets archived
            archive_rate = {
                'trial_signup': 0.20,  # 20% abandon trial
                'demo_request': 0.30,
                'pricing_inquiry': 0.35,
                'contact_us': 0.50,
                'whitepaper_download': 0.70,  # Most don't convert
                'newsletter_signup': 0.80,
            }
            is_archived = is_customer or random.random() < archive_rate.get(form_type, 0.5)
            
            # Was seen rate (high priority leads get seen quickly)
            was_seen_rate = {
                'high': 0.95,
                'medium': 0.75,
                'low': 0.40,
            }
            was_seen = random.random() < was_seen_rate.get(sales_priority, 0.7)
            
            yield {
                'id': str(uuid.uuid4()),
                'title': f"{fake.name()} - {fake.company()}",
                'owner_id': owner['id'],
                'creator_id': 99,  # Web form automation
                'person_id': random.randint(1000, 999999),
                'organization_id': random.randint(1000, 999999),
                
                'source_name': source_name,
                'origin': origin,
                'origin_id': None,
                'channel': channel,
                'channel_id': FORM_SOURCE_MAPPING[form_type],
                
                'is_archived': is_archived,
                'was_seen': was_seen,
                'value': amount,
                'currency': 'USD',
                'expected_close_date': (add_time + timedelta(days=random.randint(15, 60))).strftime('%Y-%m-%d') if not is_customer else None,
                'next_activity_id': None if is_customer else random.randint(1, 1000),
                
                'add_time': add_time.isoformat(),
                'update_time': (add_time + timedelta(hours=random.randint(1, 72))).isoformat(),
                'visible_to': '3',
                'cc_email': f"company+{random.randint(1000, 9999)}@pipedrivemail.com",
                
                # Contact details
                'email': fake.email(),
                'phone': fake.phone_number(),
                'job_title': fake.job(),
                'company': fake.company(),
                'industry': fake.bs(),
                'company_size': random.choice(['1-10', '11-50', '51-200', '201-500', '500+']),
                'country': random.choices([g['country'] for g in GEO_DISTRIBUTION], 
                                        weights=[g['weight'] for g in GEO_DISTRIBUTION])[0],
                'state': fake.state(),
                'city': fake.city(),
                
                # Lead tracking fields
                'lifecycle_stage': form_config['lifecycle_stage'],
                'form_type': form_type,
                'trial_path': trial_path,
                'trial_start_date': trial_start_date,
                'sales_priority': sales_priority,
                'expected_activities': expected_activities,
                
                '_generated_at': datetime.now().isoformat(),
                '_source': 'form_fill',
                '_form_type': form_type,
                '_converted_to_customer': is_customer,
            }
    
    total_generated = len(purchased_leads) + total_form_fills
    print(f"\n✓ Total leads generated: {total_generated}")
    print(f"  - Purchased leads: {len(purchased_leads)}")
    print(f"  - Form fill leads: {total_form_fills}")
    print(f"    - Trial signups: {form_type_counts['trial_signup']}")
    print(f"    - Demo requests: {form_type_counts['demo_request']}")
    print(f"    - Pricing inquiries: {form_type_counts['pricing_inquiry']}")
    print(f"    - Contact forms: {form_type_counts['contact_us']}")
    print(f"    - Whitepaper downloads: {form_type_counts['whitepaper_download']}")
    print(f"    - Newsletter signups: {form_type_counts['newsletter_signup']}")


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive_leads",
        destination="filesystem",
        dataset_name="pipedrive"
    )
    
    load_info = pipeline.run(leads(), loader_file_format="parquet")
    
    print(f"\n✓ Pipedrive leads: purchased lists + {DAYS_OF_DATA} days of form fills")