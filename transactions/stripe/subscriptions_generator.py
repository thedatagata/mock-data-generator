"""
Stripe Subscriptions Generator
Creates subscriptions linked to product SKUs with proper trial tracking
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

# Calculate total conversions (same as deals)
total_form_fills = sum(get_daily_metrics(d)['identified_leads'] for d in range(DAYS_OF_DATA))

# Trial conversions
trial_signups = int(total_form_fills * 0.35)
trial_self_service = int(trial_signups * 0.65)
trial_sales_assisted = int(trial_signups * 0.35)

trial_ss_conversions = int(trial_self_service * 0.28)
trial_sa_conversions = int(trial_sales_assisted * 0.45)

# Other form conversions
demo_conversions = int(total_form_fills * 0.25 * 0.12)
pricing_conversions = int(total_form_fills * 0.15 * 0.15)
contact_conversions = int(total_form_fills * 0.15 * 0.08)
whitepaper_conversions = int(total_form_fills * 0.07 * 0.03)

total_paid_conversions = (trial_ss_conversions + trial_sa_conversions + 
                          demo_conversions + pricing_conversions + 
                          contact_conversions + whitepaper_conversions)

print(f"Total paid subscriptions: {total_paid_conversions}")
print(f"  - Trial conversions: {trial_ss_conversions + trial_sa_conversions}")
print(f"  - Other form conversions: {demo_conversions + pricing_conversions + contact_conversions + whitepaper_conversions}")

# Also need trialing subscriptions (those who haven't converted yet)
trial_non_converted = trial_signups - (trial_ss_conversions + trial_sa_conversions)
active_trials = int(trial_non_converted * 0.20)  # 20% still in trial period

print(f"Active trials (not yet converted): {active_trials}")


@dlt.resource(write_disposition="append", table_name="subscriptions")
def subscriptions():
    """Generate Stripe subscriptions linked to product SKUs"""
    
    subscription_count = 0
    
    # ==========================================
    # 1. TRIAL SELF-SERVICE CONVERSIONS
    # ==========================================
    
    for _ in range(trial_ss_conversions):
        # Trial started
        trial_start = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 20))
        
        # Self-service converts on day 12 average
        conversion_day = int(random.gauss(12, 2))
        conversion_day = max(10, min(16, conversion_day))
        
        # Subscription created ON TRIAL START
        created_date = trial_start
        
        # Trial end is 14 days after start
        trial_end = trial_start + timedelta(days=14)
        
        # But user converted early
        conversion_date = trial_start + timedelta(days=conversion_day)
        
        # Select product tier
        product = select_product_tier('self_service')
        
        # Billing interval
        billing_interval = 'year' if random.random() < 0.15 else 'month'
        amount = product['price_annual'] if billing_interval == 'year' else product['price_monthly']
        
        # Current period
        if billing_interval == 'month':
            current_period_start = conversion_date
            current_period_end = conversion_date + timedelta(days=30)
        else:
            current_period_start = conversion_date
            current_period_end = conversion_date + timedelta(days=365)
        
        yield {
            'id': f"sub_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'subscription',
            'application': None,
            'application_fee_percent': None,
            'automatic_tax': {'enabled': False, 'liability': None},
            'billing_cycle_anchor': int(conversion_date.timestamp()),
            'billing_cycle_anchor_config': None,
            'billing_mode': 'recurring',
            'billing_thresholds': None,
            'cancel_at': None,
            'cancel_at_period_end': False,
            'canceled_at': None,
            'cancellation_details': {'comment': None, 'feedback': None, 'reason': None},
            'collection_method': 'charge_automatically',
            'created': int(created_date.timestamp()),
            'currency': 'usd',
            'customer': f"cus_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'days_until_due': None,
            'default_payment_method': f"pm_{fake.bothify(text='????????????????????', letters='abcdefghijklmnopqrstuvwxyz0123456789')}",
            'default_source': None,
            'default_tax_rates': [],
            'description': None,
            'discounts': [],
            'ended_at': None,
            'invoice_settings': {'issuer': {'type': 'self'}},
            'items': {
                'object': 'list',
                'data': [{
                    'id': f"si_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
                    'object': 'subscription_item',
                    'created': int(created_date.timestamp()),
                    'metadata': {},
                    'plan': {
                        'id': f"plan_{product['sku'].lower()}_{billing_interval}ly",
                        'object': 'plan',
                        'active': True,
                        'amount': amount,
                        'amount_decimal': str(amount),
                        'billing_scheme': 'per_unit',
                        'created': int(START_DATE.timestamp()),
                        'currency': 'usd',
                        'interval': billing_interval,
                        'interval_count': 1,
                        'livemode': False,
                        'metadata': {'tier': product['name']},
                        'nickname': f"{product['name']} {billing_interval.title()}ly",
                        'product': product['id'],
                        'tiers_mode': None,
                        'transform_usage': None,
                        'trial_period_days': 14,
                        'usage_type': 'licensed',
                    },
                    'quantity': 1,
                }],
                'has_more': False,
                'total_count': 1,
            },
            'latest_invoice': f"in_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'livemode': False,
            'metadata': {
                'trial_path': 'self_service',
                'product_sku': product['sku'],
                'conversion_day': conversion_day,
            },
            'next_pending_invoice_item_invoice': None,
            'on_behalf_of': None,
            'pause_collection': None,
            'payment_settings': {
                'payment_method_options': None,
                'payment_method_types': None,
                'save_default_payment_method': 'off',
            },
            'pending_invoice_item_interval': None,
            'pending_setup_intent': None,
            'pending_update': None,
            'schedule': None,
            'start_date': int(created_date.timestamp()),
            'status': 'active',
            'test_clock': None,
            'transfer_data': None,
            'trial_end': int(conversion_date.timestamp()),  # Trial ended when they converted
            'trial_settings': {'end_behavior': {'missing_payment_method': 'create_invoice'}},
            'trial_start': int(trial_start.timestamp()),
            'current_period_start': int(current_period_start.timestamp()),
            'current_period_end': int(current_period_end.timestamp()),
            
            '_generated_at': datetime.now().isoformat(),
            '_subscription_type': 'trial_self_service_converted',
        }
        subscription_count += 1
    
    # ==========================================
    # 2. TRIAL SALES-ASSISTED CONVERSIONS
    # ==========================================
    
    for _ in range(trial_sa_conversions):
        # Trial started
        trial_start = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 20))
        
        # Sales-assisted converts on day 14 average
        conversion_day = int(random.gauss(14, 1))
        conversion_day = max(12, min(16, conversion_day))
        
        # Subscription created ON TRIAL START
        created_date = trial_start
        
        # Trial end and conversion are typically same day
        trial_end = trial_start + timedelta(days=14)
        conversion_date = trial_start + timedelta(days=conversion_day)
        
        # Select product tier
        product = select_product_tier('sales_assisted')
        
        # Billing interval (more annual for sales-assisted)
        billing_interval = 'year' if random.random() < 0.30 else 'month'
        amount = product['price_annual'] if billing_interval == 'year' else product['price_monthly']
        
        # Current period
        if billing_interval == 'month':
            current_period_start = conversion_date
            current_period_end = conversion_date + timedelta(days=30)
        else:
            current_period_start = conversion_date
            current_period_end = conversion_date + timedelta(days=365)
        
        yield {
            'id': f"sub_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'subscription',
            'application': None,
            'application_fee_percent': None,
            'automatic_tax': {'enabled': False, 'liability': None},
            'billing_cycle_anchor': int(conversion_date.timestamp()),
            'billing_cycle_anchor_config': None,
            'billing_mode': 'recurring',
            'billing_thresholds': None,
            'cancel_at': None,
            'cancel_at_period_end': False,
            'canceled_at': None,
            'cancellation_details': {'comment': None, 'feedback': None, 'reason': None},
            'collection_method': 'charge_automatically',
            'created': int(created_date.timestamp()),
            'currency': 'usd',
            'customer': f"cus_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'days_until_due': None,
            'default_payment_method': f"pm_{fake.bothify(text='????????????????????', letters='abcdefghijklmnopqrstuvwxyz0123456789')}",
            'default_source': None,
            'default_tax_rates': [],
            'description': None,
            'discounts': [],
            'ended_at': None,
            'invoice_settings': {'issuer': {'type': 'self'}},
            'items': {
                'object': 'list',
                'data': [{
                    'id': f"si_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
                    'object': 'subscription_item',
                    'created': int(created_date.timestamp()),
                    'metadata': {},
                    'plan': {
                        'id': f"plan_{product['sku'].lower()}_{billing_interval}ly",
                        'object': 'plan',
                        'active': True,
                        'amount': amount,
                        'amount_decimal': str(amount),
                        'billing_scheme': 'per_unit',
                        'created': int(START_DATE.timestamp()),
                        'currency': 'usd',
                        'interval': billing_interval,
                        'interval_count': 1,
                        'livemode': False,
                        'metadata': {'tier': product['name']},
                        'nickname': f"{product['name']} {billing_interval.title()}ly",
                        'product': product['id'],
                        'tiers_mode': None,
                        'transform_usage': None,
                        'trial_period_days': 14,
                        'usage_type': 'licensed',
                    },
                    'quantity': 1,
                }],
                'has_more': False,
                'total_count': 1,
            },
            'latest_invoice': f"in_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'livemode': False,
            'metadata': {
                'trial_path': 'sales_assisted',
                'product_sku': product['sku'],
                'conversion_day': conversion_day,
            },
            'next_pending_invoice_item_invoice': None,
            'on_behalf_of': None,
            'pause_collection': None,
            'payment_settings': {
                'payment_method_options': None,
                'payment_method_types': None,
                'save_default_payment_method': 'off',
            },
            'pending_invoice_item_interval': None,
            'pending_setup_intent': None,
            'pending_update': None,
            'schedule': None,
            'start_date': int(created_date.timestamp()),
            'status': 'active',
            'test_clock': None,
            'transfer_data': None,
            'trial_end': int(conversion_date.timestamp()),
            'trial_settings': {'end_behavior': {'missing_payment_method': 'create_invoice'}},
            'trial_start': int(trial_start.timestamp()),
            'current_period_start': int(current_period_start.timestamp()),
            'current_period_end': int(current_period_end.timestamp()),
            
            '_generated_at': datetime.now().isoformat(),
            '_subscription_type': 'trial_sales_assisted_converted',
        }
        subscription_count += 1
    
    # ==========================================
    # 3. OTHER FORM CONVERSIONS (Direct to Paid)
    # ==========================================
    
    other_conversions = demo_conversions + pricing_conversions + contact_conversions + whitepaper_conversions
    
    for _ in range(other_conversions):
        # Subscription starts after sales cycle
        start_date = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 60))
        
        # Select product tier (mixed distribution)
        product = select_product_tier()
        
        # Billing interval
        billing_interval = 'year' if random.random() < 0.20 else 'month'
        amount = product['price_annual'] if billing_interval == 'year' else product['price_monthly']
        
        # Current period
        if billing_interval == 'month':
            current_period_start = start_date
            current_period_end = start_date + timedelta(days=30)
        else:
            current_period_start = start_date
            current_period_end = start_date + timedelta(days=365)
        
        yield {
            'id': f"sub_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'subscription',
            'application': None,
            'application_fee_percent': None,
            'automatic_tax': {'enabled': False, 'liability': None},
            'billing_cycle_anchor': int(start_date.timestamp()),
            'billing_cycle_anchor_config': None,
            'billing_mode': 'recurring',
            'billing_thresholds': None,
            'cancel_at': None,
            'cancel_at_period_end': False,
            'canceled_at': None,
            'cancellation_details': {'comment': None, 'feedback': None, 'reason': None},
            'collection_method': 'charge_automatically',
            'created': int(start_date.timestamp()),
            'currency': 'usd',
            'customer': f"cus_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'days_until_due': None,
            'default_payment_method': f"pm_{fake.bothify(text='????????????????????', letters='abcdefghijklmnopqrstuvwxyz0123456789')}",
            'default_source': None,
            'default_tax_rates': [],
            'description': None,
            'discounts': [],
            'ended_at': None,
            'invoice_settings': {'issuer': {'type': 'self'}},
            'items': {
                'object': 'list',
                'data': [{
                    'id': f"si_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
                    'object': 'subscription_item',
                    'created': int(start_date.timestamp()),
                    'metadata': {},
                    'plan': {
                        'id': f"plan_{product['sku'].lower()}_{billing_interval}ly",
                        'object': 'plan',
                        'active': True,
                        'amount': amount,
                        'amount_decimal': str(amount),
                        'billing_scheme': 'per_unit',
                        'created': int(START_DATE.timestamp()),
                        'currency': 'usd',
                        'interval': billing_interval,
                        'interval_count': 1,
                        'livemode': False,
                        'metadata': {'tier': product['name']},
                        'nickname': f"{product['name']} {billing_interval.title()}ly",
                        'product': product['id'],
                        'tiers_mode': None,
                        'transform_usage': None,
                        'trial_period_days': None,  # No trial for these
                        'usage_type': 'licensed',
                    },
                    'quantity': 1,
                }],
                'has_more': False,
                'total_count': 1,
            },
            'latest_invoice': f"in_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'livemode': False,
            'metadata': {
                'trial_path': None,
                'product_sku': product['sku'],
                'conversion_day': None,
            },
            'next_pending_invoice_item_invoice': None,
            'on_behalf_of': None,
            'pause_collection': None,
            'payment_settings': {
                'payment_method_options': None,
                'payment_method_types': None,
                'save_default_payment_method': 'off',
            },
            'pending_invoice_item_interval': None,
            'pending_setup_intent': None,
            'pending_update': None,
            'schedule': None,
            'start_date': int(start_date.timestamp()),
            'status': 'active',
            'test_clock': None,
            'transfer_data': None,
            'trial_end': None,
            'trial_settings': {'end_behavior': {'missing_payment_method': 'create_invoice'}},
            'trial_start': None,
            'current_period_start': int(current_period_start.timestamp()),
            'current_period_end': int(current_period_end.timestamp()),
            
            '_generated_at': datetime.now().isoformat(),
            '_subscription_type': 'direct_paid',
        }
        subscription_count += 1
    
    # ==========================================
    # 4. ACTIVE TRIALS (Not Yet Converted)
    # ==========================================
    
    for _ in range(active_trials):
        # Trial started recently
        days_ago = random.randint(1, 14)
        trial_start = datetime.now() - timedelta(days=days_ago)
        trial_end = trial_start + timedelta(days=14)
        
        # Determine trial path
        trial_path = get_trial_path()
        
        # Select product tier
        product = select_product_tier(trial_path)
        
        # Billing interval
        billing_interval = 'month'  # Default for trials
        amount = product['price_monthly']
        
        yield {
            'id': f"sub_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'subscription',
            'application': None,
            'application_fee_percent': None,
            'automatic_tax': {'enabled': False, 'liability': None},
            'billing_cycle_anchor': int(trial_end.timestamp()),
            'billing_cycle_anchor_config': None,
            'billing_mode': 'recurring',
            'billing_thresholds': None,
            'cancel_at': None,
            'cancel_at_period_end': False,
            'canceled_at': None,
            'cancellation_details': {'comment': None, 'feedback': None, 'reason': None},
            'collection_method': 'charge_automatically',
            'created': int(trial_start.timestamp()),
            'currency': 'usd',
            'customer': f"cus_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'days_until_due': None,
            'default_payment_method': f"pm_{fake.bothify(text='????????????????????', letters='abcdefghijklmnopqrstuvwxyz0123456789')}",
            'default_source': None,
            'default_tax_rates': [],
            'description': None,
            'discounts': [],
            'ended_at': None,
            'invoice_settings': {'issuer': {'type': 'self'}},
            'items': {
                'object': 'list',
                'data': [{
                    'id': f"si_{fake.bothify(text='??????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
                    'object': 'subscription_item',
                    'created': int(trial_start.timestamp()),
                    'metadata': {},
                    'plan': {
                        'id': f"plan_{product['sku'].lower()}_{billing_interval}ly",
                        'object': 'plan',
                        'active': True,
                        'amount': amount,
                        'amount_decimal': str(amount),
                        'billing_scheme': 'per_unit',
                        'created': int(START_DATE.timestamp()),
                        'currency': 'usd',
                        'interval': billing_interval,
                        'interval_count': 1,
                        'livemode': False,
                        'metadata': {'tier': product['name']},
                        'nickname': f"{product['name']} {billing_interval.title()}ly",
                        'product': product['id'],
                        'tiers_mode': None,
                        'transform_usage': None,
                        'trial_period_days': 14,
                        'usage_type': 'licensed',
                    },
                    'quantity': 1,
                }],
                'has_more': False,
                'total_count': 1,
            },
            'latest_invoice': None,  # No invoice yet during trial
            'livemode': False,
            'metadata': {
                'trial_path': trial_path,
                'product_sku': product['sku'],
                'conversion_day': None,
            },
            'next_pending_invoice_item_invoice': None,
            'on_behalf_of': None,
            'pause_collection': None,
            'payment_settings': {
                'payment_method_options': None,
                'payment_method_types': None,
                'save_default_payment_method': 'off',
            },
            'pending_invoice_item_interval': None,
            'pending_setup_intent': None,
            'pending_update': None,
            'schedule': None,
            'start_date': int(trial_start.timestamp()),
            'status': 'trialing',
            'test_clock': None,
            'transfer_data': None,
            'trial_end': int(trial_end.timestamp()),
            'trial_settings': {'end_behavior': {'missing_payment_method': 'create_invoice'}},
            'trial_start': int(trial_start.timestamp()),
            'current_period_start': int(trial_start.timestamp()),
            'current_period_end': int(trial_end.timestamp()),
            
            '_generated_at': datetime.now().isoformat(),
            '_subscription_type': 'active_trial',
        }
        subscription_count += 1
    
    print(f"\n✓ Total subscriptions generated: {subscription_count}")
    print(f"  - Trial self-service (converted): {trial_ss_conversions}")
    print(f"  - Trial sales-assisted (converted): {trial_sa_conversions}")
    print(f"  - Direct paid (no trial): {other_conversions}")
    print(f"  - Active trials (not yet converted): {active_trials}")


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_subscriptions",
        destination="filesystem",
        dataset_name="stripe"
    )
    
    load_info = pipeline.run(subscriptions(), loader_file_format="parquet")
    
    print(f"\n✓ Stripe subscriptions: linked to product SKUs with trial tracking")
