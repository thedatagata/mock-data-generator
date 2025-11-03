"""
Pipedrive Deals Generator
Creates deals linked to product SKUs with proper timing based on trial paths
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

SALES_REPS = [1, 2, 3, 4, 5]

STAGES = {
    'qualification': {'id': 1, 'probability': 20},
    'needs_analysis': {'id': 2, 'probability': 40},
    'proposal': {'id': 3, 'probability': 60},
    'negotiation': {'id': 4, 'probability': 80},
    'closed_won': {'id': 5, 'probability': 100},
    'closed_lost': {'id': 6, 'probability': 0},
}

LOST_REASONS = ['Budget constraints', 'Chose competitor', 'No response', 'Timeline mismatch', 'Not a good fit']


def calculate_deal_value(product, billing_interval='monthly'):
    """Calculate deal value from product and billing interval"""
    if billing_interval == 'annual':
        return product['price_annual'] / 100  # Convert cents to dollars
    else:
        return product['price_monthly'] / 100


@dlt.resource(write_disposition="append", table_name="deals")
def deals():
    """Generate deals from trial conversions and other form conversions"""
    
    deal_id = 1
    
    # Calculate expected conversions
    total_form_fills = sum(get_daily_metrics(d)['identified_leads'] for d in range(DAYS_OF_DATA))
    
    # Trial conversions
    trial_signups = int(total_form_fills * 0.35)
    trial_self_service = int(trial_signups * 0.65)
    trial_sales_assisted = int(trial_signups * 0.35)
    
    trial_ss_conversions = int(trial_self_service * 0.28)
    trial_sa_conversions = int(trial_sales_assisted * 0.45)
    total_trial_conversions = trial_ss_conversions + trial_sa_conversions
    
    # Other form conversions
    demo_conversions = int(total_form_fills * 0.25 * 0.12)
    pricing_conversions = int(total_form_fills * 0.15 * 0.15)
    contact_conversions = int(total_form_fills * 0.15 * 0.08)
    whitepaper_conversions = int(total_form_fills * 0.07 * 0.03)
    
    total_conversions = (total_trial_conversions + demo_conversions + 
                        pricing_conversions + contact_conversions + whitepaper_conversions)
    
    # Also add some lost deals
    lost_deals_count = int(total_conversions * 0.30)  # 30% lost rate
    
    print(f"\nGenerating {total_conversions} won deals + {lost_deals_count} lost deals:")
    print(f"  - Trial self-service conversions: {trial_ss_conversions}")
    print(f"  - Trial sales-assisted conversions: {trial_sa_conversions}")
    print(f"  - Demo request conversions: {demo_conversions}")
    print(f"  - Pricing inquiry conversions: {pricing_conversions}")
    print(f"  - Contact form conversions: {contact_conversions}")
    print(f"  - Whitepaper conversions: {whitepaper_conversions}")
    
    # ==========================================
    # 1. TRIAL SELF-SERVICE CONVERSIONS
    # ==========================================
    
    for _ in range(trial_ss_conversions):
        # Trial starts randomly across year
        trial_start = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 20))
        
        # Self-service converts on average day 12
        conversion_day = int(random.gauss(12, 2))
        conversion_day = max(10, min(16, conversion_day))  # Between 10-16 days
        
        conversion_time = trial_start + timedelta(days=conversion_day)
        
        # Deal is created POST-CONVERSION (immediately won)
        add_time = conversion_time
        won_time = conversion_time
        
        # Select product tier (self-service leans lower)
        product = select_product_tier('self_service')
        
        # Billing interval (85% monthly, 15% annual)
        billing_interval = 'annual' if random.random() < 0.15 else 'monthly'
        value = calculate_deal_value(product, billing_interval)
        
        owner = random.choice(SALES_REPS)
        
        yield {
            'id': deal_id,
            'title': f"{fake.company()} - {product['name']} Plan ({billing_interval.title()})",
            'creator_user_id': 99,  # System/automated
            'owner_id': owner,
            'value': round(value, 2),
            'person_id': random.randint(1000, 999999),
            'org_id': random.randint(1000, 999999),
            'stage_id': STAGES['closed_won']['id'],
            'pipeline_id': 1,
            'currency': 'USD',
            'archive_time': None,
            'add_time': add_time.isoformat(),
            'update_time': won_time.isoformat(),
            'stage_change_time': won_time.isoformat(),
            'status': 'won',
            'is_archived': False,
            'is_deleted': False,
            'probability': 100,
            'lost_reason': None,
            'visible_to': 7,
            'close_time': won_time.isoformat(),
            'won_time': won_time.isoformat(),
            'lost_time': None,
            'local_won_date': won_time.strftime('%Y-%m-%d'),
            'local_lost_date': None,
            'local_close_date': won_time.strftime('%Y-%m-%d'),
            'expected_close_date': None,
            'label_ids': [1, 2],
            'origin': 'API',
            'origin_id': None,
            'channel': random.choice([52, 53, 54]),
            'channel_id': 'Trial Self-Service',
            
            # Revenue metrics
            'acv': round(value, 2),
            'arr': round(value * 12, 2) if billing_interval == 'monthly' else round(value, 2),
            'mrr': round(value, 2) if billing_interval == 'monthly' else round(value / 12, 2),
            
            'next_activity_id': None,
            'last_activity_id': random.randint(1, 500000),
            'first_won_time': won_time.isoformat(),
            'products_count': 1,
            'files_count': random.randint(0, 3),
            'notes_count': random.randint(1, 5),
            'followers_count': 1,
            'email_messages_count': random.randint(3, 8),  # Just automated emails
            'activities_count': 1,  # Just trial_started
            'done_activities_count': 1,
            'undone_activities_count': 0,
            'participants_count': 1,
            'last_incoming_mail_time': None,
            'last_outgoing_mail_time': won_time.isoformat(),
            
            # Custom fields
            'product_sku': product['sku'],
            'product_name': product['name'],
            'billing_interval': billing_interval,
            'trial_path': 'self_service',
            'trial_start_date': trial_start.strftime('%Y-%m-%d'),
            'conversion_day': conversion_day,
            'source_form_type': 'trial_signup',
            
            '_generated_at': datetime.now().isoformat(),
            '_deal_type': 'trial_self_service',
        }
        deal_id += 1
    
    # ==========================================
    # 2. TRIAL SALES-ASSISTED CONVERSIONS
    # ==========================================
    
    for _ in range(trial_sa_conversions):
        # Trial starts randomly across year
        trial_start = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 20))
        
        # Deal is created ON TRIAL START (progresses through stages)
        add_time = trial_start
        
        # Sales-assisted converts on average day 14
        conversion_day = int(random.gauss(14, 1))
        conversion_day = max(12, min(16, conversion_day))
        
        won_time = trial_start + timedelta(days=conversion_day)
        
        # Select product tier (sales-assisted leans higher)
        product = select_product_tier('sales_assisted')
        
        # Billing interval (70% monthly, 30% annual for sales-assisted)
        billing_interval = 'annual' if random.random() < 0.30 else 'monthly'
        value = calculate_deal_value(product, billing_interval)
        
        owner = random.choice(SALES_REPS)
        
        yield {
            'id': deal_id,
            'title': f"{fake.company()} - {product['name']} Plan ({billing_interval.title()})",
            'creator_user_id': owner,
            'owner_id': owner,
            'value': round(value, 2),
            'person_id': random.randint(1000, 999999),
            'org_id': random.randint(1000, 999999),
            'stage_id': STAGES['closed_won']['id'],
            'pipeline_id': 1,
            'currency': 'USD',
            'archive_time': None,
            'add_time': add_time.isoformat(),
            'update_time': won_time.isoformat(),
            'stage_change_time': won_time.isoformat(),
            'status': 'won',
            'is_archived': False,
            'is_deleted': False,
            'probability': 100,
            'lost_reason': None,
            'visible_to': 7,
            'close_time': won_time.isoformat(),
            'won_time': won_time.isoformat(),
            'lost_time': None,
            'local_won_date': won_time.strftime('%Y-%m-%d'),
            'local_lost_date': None,
            'local_close_date': won_time.strftime('%Y-%m-%d'),
            'expected_close_date': None,
            'label_ids': [1, 2, 3],
            'origin': 'API',
            'origin_id': None,
            'channel': random.choice([52, 53, 54]),
            'channel_id': 'Trial Sales-Assisted',
            
            # Revenue metrics
            'acv': round(value, 2),
            'arr': round(value * 12, 2) if billing_interval == 'monthly' else round(value, 2),
            'mrr': round(value, 2) if billing_interval == 'monthly' else round(value / 12, 2),
            
            'next_activity_id': None,
            'last_activity_id': random.randint(1, 500000),
            'first_won_time': won_time.isoformat(),
            'products_count': 1,
            'files_count': random.randint(2, 10),
            'notes_count': random.randint(8, 20),
            'followers_count': random.randint(1, 3),
            'email_messages_count': random.randint(15, 40),
            'activities_count': random.randint(10, 15),
            'done_activities_count': random.randint(10, 15),
            'undone_activities_count': 0,
            'participants_count': random.randint(2, 4),
            'last_incoming_mail_time': (won_time - timedelta(days=1)).isoformat(),
            'last_outgoing_mail_time': won_time.isoformat(),
            
            # Custom fields
            'product_sku': product['sku'],
            'product_name': product['name'],
            'billing_interval': billing_interval,
            'trial_path': 'sales_assisted',
            'trial_start_date': trial_start.strftime('%Y-%m-%d'),
            'conversion_day': conversion_day,
            'source_form_type': 'trial_signup',
            
            '_generated_at': datetime.now().isoformat(),
            '_deal_type': 'trial_sales_assisted',
        }
        deal_id += 1
    
    # ==========================================
    # 3. OTHER FORM TYPE CONVERSIONS
    # ==========================================
    
    other_conversions = [
        ('demo_request', demo_conversions, 'Demo Request'),
        ('pricing_inquiry', pricing_conversions, 'Pricing Inquiry'),
        ('contact_us', contact_conversions, 'Contact Form'),
        ('whitepaper_download', whitepaper_conversions, 'Whitepaper Download'),
    ]
    
    for form_type, count, channel_name in other_conversions:
        for _ in range(count):
            # Form submission date
            form_date = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 60))
            
            # Deal created shortly after form submission
            add_time = form_date + timedelta(days=random.randint(0, 3))
            
            # Won after 20-45 days
            won_time = add_time + timedelta(days=random.randint(20, 45))
            
            # Product selection (mix of tiers)
            product = select_product_tier()
            
            # Billing interval
            billing_interval = 'annual' if random.random() < 0.20 else 'monthly'
            value = calculate_deal_value(product, billing_interval)
            
            owner = random.choice(SALES_REPS)
            
            yield {
                'id': deal_id,
                'title': f"{fake.company()} - {product['name']} Plan ({billing_interval.title()})",
                'creator_user_id': owner,
                'owner_id': owner,
                'value': round(value, 2),
                'person_id': random.randint(1000, 999999),
                'org_id': random.randint(1000, 999999),
                'stage_id': STAGES['closed_won']['id'],
                'pipeline_id': 1,
                'currency': 'USD',
                'archive_time': None,
                'add_time': add_time.isoformat(),
                'update_time': won_time.isoformat(),
                'stage_change_time': won_time.isoformat(),
                'status': 'won',
                'is_archived': False,
                'is_deleted': False,
                'probability': 100,
                'lost_reason': None,
                'visible_to': 7,
                'close_time': won_time.isoformat(),
                'won_time': won_time.isoformat(),
                'lost_time': None,
                'local_won_date': won_time.strftime('%Y-%m-%d'),
                'local_lost_date': None,
                'local_close_date': won_time.strftime('%Y-%m-%d'),
                'expected_close_date': None,
                'label_ids': [1, 2],
                'origin': 'API',
                'origin_id': None,
                'channel': random.choice([52, 53, 54]),
                'channel_id': channel_name,
                
                # Revenue metrics
                'acv': round(value, 2),
                'arr': round(value * 12, 2) if billing_interval == 'monthly' else round(value, 2),
                'mrr': round(value, 2) if billing_interval == 'monthly' else round(value / 12, 2),
                
                'next_activity_id': None,
                'last_activity_id': random.randint(1, 500000),
                'first_won_time': won_time.isoformat(),
                'products_count': 1,
                'files_count': random.randint(1, 8),
                'notes_count': random.randint(5, 15),
                'followers_count': random.randint(1, 2),
                'email_messages_count': random.randint(10, 30),
                'activities_count': random.randint(6, 12),
                'done_activities_count': random.randint(6, 12),
                'undone_activities_count': 0,
                'participants_count': random.randint(1, 3),
                'last_incoming_mail_time': (won_time - timedelta(days=random.randint(1, 3))).isoformat(),
                'last_outgoing_mail_time': won_time.isoformat(),
                
                # Custom fields
                'product_sku': product['sku'],
                'product_name': product['name'],
                'billing_interval': billing_interval,
                'trial_path': None,
                'trial_start_date': None,
                'conversion_day': None,
                'source_form_type': form_type,
                
                '_generated_at': datetime.now().isoformat(),
                '_deal_type': f'form_{form_type}',
            }
            deal_id += 1
    
    # ==========================================
    # 4. LOST DEALS
    # ==========================================
    
    for _ in range(lost_deals_count):
        # Deal started
        add_time = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 60))
        
        # Lost after 15-45 days
        lost_time = add_time + timedelta(days=random.randint(15, 45))
        
        # Product selection
        product = select_product_tier()
        billing_interval = 'monthly'
        value = calculate_deal_value(product, billing_interval)
        
        owner = random.choice(SALES_REPS)
        lost_reason = random.choice(LOST_REASONS)
        
        yield {
            'id': deal_id,
            'title': f"{fake.company()} - {product['name']} Plan",
            'creator_user_id': owner,
            'owner_id': owner,
            'value': round(value, 2),
            'person_id': random.randint(1000, 999999),
            'org_id': random.randint(1000, 999999),
            'stage_id': STAGES['closed_lost']['id'],
            'pipeline_id': 1,
            'currency': 'USD',
            'archive_time': lost_time.isoformat(),
            'add_time': add_time.isoformat(),
            'update_time': lost_time.isoformat(),
            'stage_change_time': lost_time.isoformat(),
            'status': 'lost',
            'is_archived': True,
            'is_deleted': False,
            'probability': 0,
            'lost_reason': lost_reason,
            'visible_to': 7,
            'close_time': lost_time.isoformat(),
            'won_time': None,
            'lost_time': lost_time.isoformat(),
            'local_won_date': None,
            'local_lost_date': lost_time.strftime('%Y-%m-%d'),
            'local_close_date': lost_time.strftime('%Y-%m-%d'),
            'expected_close_date': None,
            'label_ids': [1],
            'origin': 'API',
            'origin_id': None,
            'channel': random.choice([52, 53, 54]),
            'channel_id': random.choice(['Trial', 'Demo Request', 'Pricing Inquiry']),
            
            # Revenue metrics (lost = 0)
            'acv': 0,
            'arr': 0,
            'mrr': 0,
            
            'next_activity_id': None,
            'last_activity_id': random.randint(1, 500000),
            'first_won_time': None,
            'products_count': 0,
            'files_count': random.randint(0, 5),
            'notes_count': random.randint(3, 10),
            'followers_count': 1,
            'email_messages_count': random.randint(5, 15),
            'activities_count': random.randint(4, 10),
            'done_activities_count': random.randint(4, 10),
            'undone_activities_count': 0,
            'participants_count': random.randint(1, 2),
            'last_incoming_mail_time': (lost_time - timedelta(days=random.randint(3, 10))).isoformat(),
            'last_outgoing_mail_time': lost_time.isoformat(),
            
            # Custom fields
            'product_sku': None,
            'product_name': None,
            'billing_interval': None,
            'trial_path': None,
            'trial_start_date': None,
            'conversion_day': None,
            'source_form_type': random.choice(['trial_signup', 'demo_request', 'pricing_inquiry']),
            
            '_generated_at': datetime.now().isoformat(),
            '_deal_type': 'lost',
        }
        deal_id += 1
    
    print(f"\n✓ Total deals generated: {deal_id - 1}")
    print(f"  - Won deals: {total_conversions}")
    print(f"  - Lost deals: {lost_deals_count}")


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive_deals",
        destination="filesystem",
        dataset_name="pipedrive"
    )
    
    load_info = pipeline.run(deals(), loader_file_format="parquet")
    
    print(f"\n✓ Deals: linked to product SKUs with trial path tracking")
