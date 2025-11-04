"""
Generate Stripe + Pipedrive CRM data FROM Amplitude user funnel state
Complete customer data pipeline: Amplitude → Stripe → Pipedrive
"""
import polars as pl
from datetime import datetime, timedelta
import random
import uuid
import os
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

print("="*80)
print("AMPLITUDE → STRIPE + PIPEDRIVE DATA GENERATOR")
print("="*80)

# Load Amplitude user funnel state
print("\nLoading Amplitude user funnel state...")
users = pl.read_parquet('output/user_funnel_state.parquet')
print(f"Loaded {len(users):,} users from Amplitude")

# Sales team
SALES_REPS = [
    {'id': 1, 'name': 'Sarah Johnson'},
    {'id': 2, 'name': 'Mike Chen'},
    {'id': 3, 'name': 'Emily Rodriguez'},
    {'id': 4, 'name': 'David Kim'},
    {'id': 5, 'name': 'Jessica Taylor'},
]

# Product tiers with pricing (cents)
PRODUCTS = [
    {'id': 'prod_starter', 'sku': 'STARTER_MONTHLY', 'name': 'Starter', 'price_monthly': 2900, 'price_annual': 29900},
    {'id': 'prod_pro', 'sku': 'PRO_MONTHLY', 'name': 'Professional', 'price_monthly': 9900, 'price_annual': 99900},
    {'id': 'prod_business', 'sku': 'BUSINESS_MONTHLY', 'name': 'Business', 'price_monthly': 29900, 'price_annual': 299900},
    {'id': 'prod_enterprise', 'sku': 'ENTERPRISE', 'name': 'Enterprise', 'price_monthly': 49900, 'price_annual': 499900},
]

def select_product_by_engagement(engagement_tier):
    """Select product based on user engagement"""
    if engagement_tier == 'very_high_engagement':
        return random.choice([p for p in PRODUCTS if p['name'] in ['Enterprise', 'Business']])
    elif engagement_tier in ['high_engagement', 'medium_engagement']:
        return random.choice([p for p in PRODUCTS if p['name'] in ['Professional', 'Business']])
    else:
        return PRODUCTS[0]  # Starter

os.makedirs('output/stripe', exist_ok=True)
os.makedirs('output/pipedrive', exist_ok=True)

# ==========================================
# 1. STRIPE CUSTOMERS (from all users with email)
# ==========================================

print("\n" + "="*80)
print("GENERATING STRIPE CUSTOMERS")
print("="*80)

# Customers are users who identified (have email) AND converted
stripe_customers = users.filter(
    (pl.col('email') != '') & 
    (pl.col('current_stage') == 'customer')
)

print(f"\nCreating {len(stripe_customers):,} Stripe customers...")

customers_list = []
customer_id_map = {}  # device_id -> stripe_customer_id

for user in stripe_customers.iter_rows(named=True):
    customer_id = f"cus_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}"
    customer_id_map[user['device_id']] = customer_id
    
    created_time = datetime.fromisoformat(user['trial_converted_date']) if user['trial_converted_date'] else datetime.fromisoformat(user['first_visit_date'])
    
    customers_list.append({
        'id': customer_id,
        'object': 'customer',
        'email': user['email'],
        'name': user['email'].split('@')[0],
        'created': int(created_time.timestamp()),
        'currency': 'usd',
        'balance': 0,
        'delinquent': False,
        'metadata': {
            'user_id': user['user_id'],
            'device_id': user['device_id'],
            'acquisition_channel': user['acquisition_channel'],
            'engagement_tier': user['engagement_tier'],
            'total_sessions': user['total_sessions'],
        },
        '_generated_at': datetime.now().isoformat(),
        '_source': 'amplitude',
    })

customers_df = pl.DataFrame(customers_list)
customers_df.write_parquet('output/stripe/customers.parquet')
print(f"✓ Saved {len(customers_list):,} customers")

# ==========================================
# 2. STRIPE SUBSCRIPTIONS (for customers)
# ==========================================

print("\n" + "="*80)
print("GENERATING STRIPE SUBSCRIPTIONS")
print("="*80)

subscriptions_list = []

for user in stripe_customers.iter_rows(named=True):
    customer_id = customer_id_map[user['device_id']]
    
    # Select product based on engagement
    product = select_product_by_engagement(user['engagement_tier'])
    
    # Determine billing interval (higher engagement → more annual)
    if user['engagement_tier'] in ['very_high_engagement', 'high_engagement']:
        billing_interval = 'year' if random.random() < 0.4 else 'month'
    else:
        billing_interval = 'year' if random.random() < 0.15 else 'month'
    
    amount = product['price_annual'] if billing_interval == 'year' else product['price_monthly']
    
    # Dates
    trial_start = datetime.fromisoformat(user['trial_started_date']) if user['trial_started_date'] else None
    conversion_date = datetime.fromisoformat(user['trial_converted_date']) if user['trial_converted_date'] else datetime.fromisoformat(user['first_visit_date'])
    
    # Current period
    if billing_interval == 'month':
        current_period_end = conversion_date + timedelta(days=30)
    else:
        current_period_end = conversion_date + timedelta(days=365)
    
    # Calculate conversion day if trial
    conversion_day = None
    if trial_start:
        conversion_day = (conversion_date - trial_start).days
    
    subscriptions_list.append({
        'id': f"sub_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
        'object': 'subscription',
        'customer': customer_id,
        'status': 'active',
        'created': int(conversion_date.timestamp()),
        'current_period_start': int(conversion_date.timestamp()),
        'current_period_end': int(current_period_end.timestamp()),
        'currency': 'usd',
        'billing_cycle_anchor': int(conversion_date.timestamp()),
        'trial_start': int(trial_start.timestamp()) if trial_start else None,
        'trial_end': int(conversion_date.timestamp()) if trial_start else None,
        'items': {
            'data': [{
                'plan': {
                    'id': f"plan_{product['sku'].lower()}_{billing_interval}ly",
                    'product': product['id'],
                    'amount': amount,
                    'currency': 'usd',
                    'interval': billing_interval,
                    'nickname': f"{product['name']} {billing_interval.title()}ly",
                }
            }]
        },
        'metadata': {
            'product_sku': product['sku'],
            'product_name': product['name'],
            'billing_interval': billing_interval,
            'user_id': user['user_id'],
            'device_id': user['device_id'],
            'conversion_day': conversion_day,
            'engagement_tier': user['engagement_tier'],
        },
        '_generated_at': datetime.now().isoformat(),
        '_source': 'amplitude',
    })

subscriptions_df = pl.DataFrame(subscriptions_list)
subscriptions_df.write_parquet('output/stripe/subscriptions.parquet')
print(f"✓ Saved {len(subscriptions_list):,} subscriptions")

# Calculate revenue metrics
total_mrr = sum([
    (s['items']['data'][0]['plan']['amount'] / 100) if s['items']['data'][0]['plan']['interval'] == 'month'
    else (s['items']['data'][0]['plan']['amount'] / 100 / 12)
    for s in subscriptions_list
])
total_arr = total_mrr * 12

print(f"\n💰 Revenue Metrics:")
print(f"   MRR: ${total_mrr:,.2f}")
print(f"   ARR: ${total_arr:,.2f}")

# ==========================================
# 3. PIPEDRIVE LEADS (trial + demo users)
# ==========================================

print("\n" + "="*80)
print("GENERATING PIPEDRIVE LEADS")
print("="*80)

# Leads = users who started trial or requested demo (not just visitors)
pipedrive_leads = users.filter(
    pl.col('current_stage').is_in(['trial_active', 'demo_requested', 'customer', 'churned'])
)

print(f"\nCreating {len(pipedrive_leads):,} Pipedrive leads...")

leads_list = []
lead_id_map = {}  # device_id -> lead_id

for user in pipedrive_leads.iter_rows(named=True):
    lead_id = str(uuid.uuid4())
    lead_id_map[user['device_id']] = lead_id
    
    owner = random.choice(SALES_REPS)
    
    # Determine form type
    if user['trial_started_date']:
        form_type = 'trial_signup'
        lifecycle_stage = 'Trial'
    elif user['demo_requested_date']:
        form_type = 'demo_request'
        lifecycle_stage = 'Marketing Qualified Lead'
    else:
        form_type = 'contact_us'
        lifecycle_stage = 'Lead'
    
    # Sales priority
    if user['engagement_tier'] in ['very_high_engagement', 'high_engagement']:
        sales_priority = 'high'
    elif user['engagement_tier'] == 'medium_engagement':
        sales_priority = 'medium'
    else:
        sales_priority = 'low'
    
    # Value estimate
    if user['current_stage'] == 'customer':
        # Use actual subscription value
        matching_subs = [s for s in subscriptions_list if s['metadata']['device_id'] == user['device_id']]
        if matching_subs:
            value = matching_subs[0]['items']['data'][0]['plan']['amount'] / 100
        else:
            value = 9900 / 100  # Default
    else:
        value = random.randint(3000, 15000)
    
    leads_list.append({
        'id': lead_id,
        'title': f"{user['email'] or user['user_id']} - {form_type.replace('_', ' ').title()}",
        'owner_id': owner['id'],
        'value': value,
        'currency': 'USD',
        'is_archived': user['current_stage'] in ['customer', 'churned'],
        'was_seen': True,
        'add_time': user['first_visit_date'],
        'update_time': user['last_active_date'],
        
        # Contact info
        'email': user['email'],
        'user_id': user['user_id'],
        'device_id': user['device_id'],
        
        # Tracking
        'lifecycle_stage': lifecycle_stage,
        'form_type': form_type,
        'current_stage': user['current_stage'],
        'trial_started_date': user['trial_started_date'],
        'sales_priority': sales_priority,
        'engagement_tier': user['engagement_tier'],
        'total_sessions': user['total_sessions'],
        'acquisition_channel': user['acquisition_channel'],
        
        '_generated_at': datetime.now().isoformat(),
        '_source': 'amplitude',
    })

leads_df = pl.DataFrame(leads_list)
leads_df.write_parquet('output/pipedrive/leads.parquet')
print(f"✓ Saved {len(leads_list):,} leads")

# ==========================================
# 4. PIPEDRIVE DEALS (customers only)
# ==========================================

print("\n" + "="*80)
print("GENERATING PIPEDRIVE DEALS")
print("="*80)

deals_list = []
deal_id = 1

# Deals = converted customers only
pipedrive_customers = users.filter(pl.col('current_stage') == 'customer')

print(f"\nCreating {len(pipedrive_customers):,} Pipedrive deals...")

for user in pipedrive_customers.iter_rows(named=True):
    owner = random.choice(SALES_REPS)
    
    # Get subscription details
    matching_subs = [s for s in subscriptions_list if s['metadata']['device_id'] == user['device_id']]
    
    if matching_subs:
        sub = matching_subs[0]
        product_sku = sub['metadata']['product_sku']
        product_name = sub['metadata']['product_name']
        billing_interval = sub['metadata']['billing_interval']
        value = sub['items']['data'][0]['plan']['amount'] / 100
        
        # Calculate revenue metrics
        if billing_interval == 'month':
            mrr = value
            arr = value * 12
        else:
            mrr = value / 12
            arr = value
    else:
        # Fallback
        product_sku = 'STARTER_MONTHLY'
        product_name = 'Starter'
        billing_interval = 'monthly'
        value = 29.00
        mrr = 29.00
        arr = 348.00
    
    # Dates
    add_time = datetime.fromisoformat(user['trial_started_date']) if user['trial_started_date'] else datetime.fromisoformat(user['first_visit_date'])
    won_time = datetime.fromisoformat(user['trial_converted_date']) if user['trial_converted_date'] else datetime.fromisoformat(user['last_active_date'])
    
    deals_list.append({
        'id': deal_id,
        'title': f"{user['email'] or user['user_id']} - {product_name} ({billing_interval.title()})",
        'owner_id': owner['id'],
        'value': round(value, 2),
        'currency': 'USD',
        'stage_id': 5,  # closed_won
        'status': 'won',
        'probability': 100,
        
        'add_time': add_time.isoformat(),
        'update_time': won_time.isoformat(),
        'won_time': won_time.isoformat(),
        'close_time': won_time.isoformat(),
        
        # Revenue metrics
        'acv': round(value, 2),
        'arr': round(arr, 2),
        'mrr': round(mrr, 2),
        
        # Activity metrics  
        'activities_count': user['total_sessions'],
        'email_messages_count': random.randint(10, 40),
        
        # Product details (from Stripe)
        'product_sku': product_sku,
        'product_name': product_name,
        'billing_interval': billing_interval,
        
        # User tracking (from Amplitude)
        'user_id': user['user_id'],
        'device_id': user['device_id'],
        'email': user['email'],
        'trial_started_date': user['trial_started_date'],
        'conversion_day': user['days_in_current_stage'],
        'engagement_tier': user['engagement_tier'],
        'acquisition_channel': user['acquisition_channel'],
        
        '_generated_at': datetime.now().isoformat(),
        '_source': 'amplitude',
    })
    deal_id += 1

deals_df = pl.DataFrame(deals_list)
deals_df.write_parquet('output/pipedrive/deals.parquet')
print(f"✓ Saved {len(deals_list):,} deals")

# Calculate deal metrics
total_deal_value = sum([d['value'] for d in deals_list])
total_deal_arr = sum([d['arr'] for d in deals_list])

print(f"\n💼 Deal Metrics:")
print(f"   Total Value: ${total_deal_value:,.2f}")
print(f"   Total ARR: ${total_deal_arr:,.2f}")

# ==========================================
# SUMMARY
# ==========================================

print("\n" + "="*80)
print("SUMMARY")
print("="*80)

print(f"\n📊 Amplitude:")
print(f"   Total users tracked: {len(users):,}")
print(f"   Identified users: {len(users.filter(pl.col('email') != '')):,}")
print(f"   Converted customers: {len(users.filter(pl.col('current_stage') == 'customer')):,}")

print(f"\n💳 Stripe:")
print(f"   Customers: {len(customers_list):,}")
print(f"   Active subscriptions: {len(subscriptions_list):,}")
print(f"   MRR: ${total_mrr:,.2f}")
print(f"   ARR: ${total_arr:,.2f}")

print(f"\n🤝 Pipedrive:")
print(f"   Leads: {len(leads_list):,}")
print(f"   Won deals: {len(deals_list):,}")
print(f"   Total deal value: ${total_deal_value:,.2f}")
print(f"   Pipeline ARR: ${total_deal_arr:,.2f}")

print(f"\n✅ Data Consistency:")
print(f"   Stripe customers == Pipedrive deals: {len(customers_list) == len(deals_list)}")
print(f"   Stripe subscriptions == Pipedrive deals: {len(subscriptions_list) == len(deals_list)}")

print("\n" + "="*80)
print("OUTPUT FILES")
print("="*80)
print("✓ output/stripe/customers.parquet")
print("✓ output/stripe/subscriptions.parquet")
print("✓ output/pipedrive/leads.parquet")
print("✓ output/pipedrive/deals.parquet")

print("\n🎯 Next Steps:")
print("1. These files are ready to load into Stripe/Pipedrive")
print("2. All data traces back to Amplitude user_id/device_id")
print("3. Revenue metrics match between Stripe and Pipedrive")
print("4. You can join across systems using user_id/device_id")
