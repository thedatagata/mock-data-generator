"""
Shared configuration for aligned GA4 and Amplitude mock data
Ensures consistent volumes, conversion rates, and trends across platforms
"""
from datetime import datetime, timedelta
import random

SEED = 42
random.seed(SEED)

# Date ranges - 12 months
START_DATE = datetime.now() - timedelta(days=365)
DAYS_OF_DATA = 365

# Base daily metrics
BASE_DAILY_ACTIVE_USERS = 50000
BASE_DAILY_NEW_USERS = 8000
BASE_DAILY_SESSIONS = 75000

# Lead vs Anonymous user distribution
LEAD_IDENTIFICATION_RATE = 0.25  # 25% of leads fill out forms
LEAD_CONVERSION_RATE = 0.05      # 5% of leads become paying customers
LEAD_PERCENTAGE_OF_TRAFFIC = 0.10  # Leads are ~10% of total traffic (rest is anonymous)

# Conversion rates - Anonymous users (baseline)
ANONYMOUS_CONVERSION_RATES = {
    'sign_up': 0.05,           # 5% anonymous users sign up
    'add_to_cart': 0.20,       # 20% add to cart
    'checkout_start': 0.60,    # 60% of carts proceed
    'purchase': 0.15,          # 15% complete purchase
}

# Conversion rates - Lead users (higher)
LEAD_CONVERSION_RATES = {
    'sign_up': 0.40,           # 40% of identified leads sign up
    'add_to_cart': 0.35,       # 35% add to cart
    'checkout_start': 0.75,    # 75% of carts proceed
    'purchase': LEAD_CONVERSION_RATE / LEAD_IDENTIFICATION_RATE,  # 20% of identified leads convert
}

# Revenue
AVERAGE_TRANSACTION_VALUE = 99.99
DAILY_TRANSACTIONS = int(
    (BASE_DAILY_SESSIONS * (1 - LEAD_PERCENTAGE_OF_TRAFFIC) * 
     ANONYMOUS_CONVERSION_RATES['add_to_cart'] * 
     ANONYMOUS_CONVERSION_RATES['checkout_start'] * 
     ANONYMOUS_CONVERSION_RATES['purchase']) +
    (BASE_DAILY_SESSIONS * LEAD_PERCENTAGE_OF_TRAFFIC * 
     LEAD_CONVERSION_RATES['add_to_cart'] * 
     LEAD_CONVERSION_RATES['checkout_start'] * 
     LEAD_CONVERSION_RATES['purchase'])
)

# ==========================================
# STRIPE PRODUCT CATALOG
# ==========================================
STRIPE_PRODUCTS = [
    {
        'id': 'prod_starter',
        'name': 'Starter',
        'price_monthly': 2900,  # $29.00
        'price_annual': 29000,  # $290.00 (17% discount)
        'trial_days': 14,
        'sku': 'STARTER',
        'tier_weight': 0.40,
        'description': 'Perfect for individuals and small teams'
    },
    {
        'id': 'prod_professional',
        'name': 'Professional',
        'price_monthly': 9900,  # $99.00
        'price_annual': 99000,  # $990.00 (17% discount)
        'trial_days': 14,
        'sku': 'PRO',
        'tier_weight': 0.35,
        'description': 'Advanced features for growing teams'
    },
    {
        'id': 'prod_business',
        'name': 'Business',
        'price_monthly': 19900,  # $199.00
        'price_annual': 199000,  # $1990.00 (17% discount)
        'trial_days': 14,
        'sku': 'BUSINESS',
        'tier_weight': 0.15,
        'description': 'Comprehensive solution for established businesses'
    },
    {
        'id': 'prod_enterprise',
        'name': 'Enterprise',
        'price_monthly': 49900,  # $499.00
        'price_annual': 499000,  # $4990.00 (17% discount)
        'trial_days': 14,
        'sku': 'ENTERPRISE',
        'tier_weight': 0.05,
        'description': 'Custom solutions with dedicated support'
    }
]

# ==========================================
# CAMPAIGN ATTRIBUTION
# ==========================================
CAMPAIGN_NAMES = [
    "acq-trial-signup",
    "acq-premium-launch",
    "brand-awareness-q4",
    "retarget-cart-abandon",
    "promo-holiday-2024",
    "leadgen-whitepaper",
    "acq-trial-conversion",
    "reeng-lapsed-users",
]

PAID_CAMPAIGNS = {
    'google_cpc': [
        {
            'id': 1001,
            'name': 'acq-trial-signup',
            'type': 'ACQUISITION',
            'targeting': 'new_users',
            'weight': 0.25
        },
        {
            'id': 1002,
            'name': 'acq-premium-launch',
            'type': 'ACQUISITION',
            'targeting': 'new_users',
            'weight': 0.20
        },
        {
            'id': 1004,
            'name': 'retarget-cart-abandon',
            'type': 'REENGAGEMENT',
            'targeting': 'returning_users',
            'weight': 0.20
        },
        {
            'id': 1007,
            'name': 'acq-trial-conversion',
            'type': 'ACQUISITION',
            'targeting': 'new_users',
            'weight': 0.25
        },
        {
            'id': 1008,
            'name': 'reeng-lapsed-users',
            'type': 'REENGAGEMENT',
            'targeting': 'returning_users',
            'weight': 0.10
        },
    ],
    'facebook_cpc': [
        {
            'id': 2001,
            'name': 'acq-trial-signup',
            'type': 'ACQUISITION',
            'targeting': 'new_users',
            'weight': 0.30
        },
        {
            'id': 2003,
            'name': 'brand-awareness-q4',
            'type': 'BRAND',
            'targeting': 'all_users',
            'weight': 0.20
        },
        {
            'id': 2005,
            'name': 'promo-holiday-2024',
            'type': 'PROMOTION',
            'targeting': 'all_users',
            'weight': 0.25
        },
        {
            'id': 2006,
            'name': 'leadgen-whitepaper',
            'type': 'LEADGEN',
            'targeting': 'new_users',
            'weight': 0.15
        },
        {
            'id': 2008,
            'name': 'reeng-lapsed-users',
            'type': 'REENGAGEMENT',
            'targeting': 'returning_users',
            'weight': 0.10
        },
    ]
}

# ==========================================
# FORM TYPES & CONVERSION PATHS
# ==========================================
FORM_TYPES = {
    'trial_signup': {
        'event_name': 'trial_started',
        'lifecycle_stage': 'Trial',
        'crm_lead_created': True,
        'path_assignment': {
            'self_service': 0.65,
            'sales_assisted': 0.35,
        },
        'distribution_weight': 0.35,  # 35% of all form fills
    },
    'demo_request': {
        'event_name': 'demo_requested',
        'lifecycle_stage': 'Lead',
        'crm_lead_created': True,
        'sales_priority': 'high',
        'crm_activities': 8,
        'conversion_rate_to_paid': 0.12,
        'distribution_weight': 0.25,
    },
    'pricing_inquiry': {
        'event_name': 'pricing_form_submit',
        'lifecycle_stage': 'Lead',
        'crm_lead_created': True,
        'sales_priority': 'high',
        'crm_activities': 6,
        'conversion_rate_to_paid': 0.15,
        'distribution_weight': 0.15,
    },
    'contact_us': {
        'event_name': 'contact_form_submit',
        'lifecycle_stage': 'Lead',
        'crm_lead_created': True,
        'sales_priority': 'medium',
        'crm_activities': 4,
        'conversion_rate_to_paid': 0.08,
        'distribution_weight': 0.15,
    },
    'whitepaper_download': {
        'event_name': 'content_download',
        'lifecycle_stage': 'Lead',
        'crm_lead_created': True,
        'sales_priority': 'low',
        'crm_activities': 2,
        'conversion_rate_to_paid': 0.03,
        'distribution_weight': 0.07,
    },
    'newsletter_signup': {
        'event_name': 'newsletter_subscribe',
        'lifecycle_stage': 'Lead',
        'crm_lead_created': True,
        'sales_priority': 'low',
        'crm_activities': 1,
        'conversion_rate_to_paid': 0.02,
        'distribution_weight': 0.03,
    }
}

# ==========================================
# TRIAL CONVERSION PATHS
# ==========================================
TRIAL_CONVERSION_PATHS = {
    'self_service': {
        'weight': 0.65,
        'crm_activities': 1,  # Just trial_started activity
        'automated_emails': 3,
        'demo_scheduled': 0.0,
        'conversion_rate_to_paid': 0.28,
        'avg_conversion_day': 12,
        'tier_distribution': {  # Self-service leans toward lower tiers
            'starter': 0.50,
            'professional': 0.35,
            'business': 0.12,
            'enterprise': 0.03,
        }
    },
    'sales_assisted': {
        'weight': 0.35,
        'crm_activities': 12,  # Heavy engagement
        'automated_emails': 3,
        'demo_scheduled': 0.85,
        'conversion_rate_to_paid': 0.45,
        'avg_conversion_day': 14,
        'tier_distribution': {  # Sales-assisted leans toward higher tiers
            'starter': 0.15,
            'professional': 0.40,
            'business': 0.30,
            'enterprise': 0.15,
        }
    }
}

# ==========================================
# CRM ACTIVITY TYPES
# ==========================================
CRM_ACTIVITY_TYPES = {
    'trial_self_service': [
        {'type': 'trial_started', 'day_offset': 0, 'probability': 1.0},
    ],
    'trial_sales_assisted': [
        {'type': 'trial_started', 'day_offset': 0, 'probability': 1.0},
        {'type': 'initial_outreach', 'day_offset': 0, 'probability': 1.0},
        {'type': 'demo_scheduled', 'day_offset': 1, 'probability': 0.85},
        {'type': 'demo_completed', 'day_offset': 2, 'probability': 0.80},
        {'type': 'follow_up_email', 'day_offset': 3, 'probability': 1.0},
        {'type': 'follow_up_call', 'day_offset': 5, 'probability': 0.90},
        {'type': 'follow_up_email', 'day_offset': 6, 'probability': 0.85},
        {'type': 'pricing_discussion', 'day_offset': 7, 'probability': 0.70},
        {'type': 'follow_up_email', 'day_offset': 9, 'probability': 0.80},
        {'type': 'technical_questions', 'day_offset': 10, 'probability': 0.60},
        {'type': 'follow_up_call', 'day_offset': 11, 'probability': 0.75},
        {'type': 'contract_sent', 'day_offset': 13, 'probability': 0.45},
    ],
    'demo_request': [
        {'type': 'demo_requested', 'day_offset': 0, 'probability': 1.0},
        {'type': 'initial_outreach', 'day_offset': 0, 'probability': 1.0},
        {'type': 'demo_scheduled', 'day_offset': 1, 'probability': 0.90},
        {'type': 'demo_completed', 'day_offset': 3, 'probability': 0.85},
        {'type': 'follow_up_email', 'day_offset': 4, 'probability': 1.0},
        {'type': 'follow_up_call', 'day_offset': 6, 'probability': 0.80},
        {'type': 'pricing_discussion', 'day_offset': 8, 'probability': 0.60},
        {'type': 'contract_sent', 'day_offset': 10, 'probability': 0.40},
    ],
    'pricing_inquiry': [
        {'type': 'pricing_inquiry_received', 'day_offset': 0, 'probability': 1.0},
        {'type': 'initial_outreach', 'day_offset': 0, 'probability': 1.0},
        {'type': 'pricing_discussion', 'day_offset': 1, 'probability': 0.90},
        {'type': 'follow_up_email', 'day_offset': 2, 'probability': 1.0},
        {'type': 'demo_scheduled', 'day_offset': 3, 'probability': 0.70},
        {'type': 'contract_sent', 'day_offset': 5, 'probability': 0.50},
    ],
    'contact_us': [
        {'type': 'contact_form_received', 'day_offset': 0, 'probability': 1.0},
        {'type': 'initial_outreach', 'day_offset': 1, 'probability': 1.0},
        {'type': 'follow_up_email', 'day_offset': 3, 'probability': 0.80},
        {'type': 'follow_up_call', 'day_offset': 5, 'probability': 0.60},
    ],
    'whitepaper_download': [
        {'type': 'content_downloaded', 'day_offset': 0, 'probability': 1.0},
        {'type': 'nurture_email_sent', 'day_offset': 3, 'probability': 1.0},
    ],
    'newsletter_signup': [
        {'type': 'newsletter_subscribed', 'day_offset': 0, 'probability': 1.0},
    ],
    'purchased_lead': [
        {'type': 'cold_email', 'day_offset': 0, 'probability': 0.60},
        {'type': 'cold_call', 'day_offset': 2, 'probability': 0.30},
        {'type': 'linkedin_outreach', 'day_offset': 5, 'probability': 0.10},
    ]
}

# Traffic sources
TRAFFIC_SOURCES = [
    {'source': 'google', 'medium': 'cpc', 'weight': 0.35, 'lead_heavy': True},
    {'source': 'facebook', 'medium': 'cpc', 'weight': 0.20, 'lead_heavy': True},
    {'source': 'google', 'medium': 'organic', 'weight': 0.25, 'lead_heavy': False},
    {'source': 'direct', 'medium': '(none)', 'weight': 0.15, 'lead_heavy': False},
    {'source': 'email', 'medium': 'email', 'weight': 0.05, 'lead_heavy': True},
]

# Device/Platform distribution
DEVICE_DISTRIBUTION = {
    'ios': 0.35,
    'android': 0.40,
    'web': 0.25,
}

# Geographic distribution
GEO_DISTRIBUTION = [
    {'country': 'United States', 'weight': 0.45},
    {'country': 'Canada', 'weight': 0.15},
    {'country': 'United Kingdom', 'weight': 0.12},
    {'country': 'Germany', 'weight': 0.10},
    {'country': 'Australia', 'weight': 0.08},
    {'country': 'France', 'weight': 0.06},
    {'country': 'Japan', 'weight': 0.04},
]

def get_daily_multiplier(day_index):
    """Apply seasonality and growth to base metrics"""
    day_of_week = day_index % 7
    weekly_factor = 0.7 if day_of_week in [5, 6] else 1.0
    
    day_of_month = day_index % 30
    monthly_factor = 1.2 if day_of_month >= 28 else 1.0
    
    quarter = day_index // 90
    growth_factor = 1.0 + (quarter * 0.05)
    
    random_factor = random.uniform(0.9, 1.1)
    
    return weekly_factor * monthly_factor * growth_factor * random_factor

def get_daily_metrics(day_index):
    """Get aligned daily metrics for both platforms"""
    multiplier = get_daily_multiplier(day_index)
    
    return {
        'active_users': int(BASE_DAILY_ACTIVE_USERS * multiplier),
        'new_users': int(BASE_DAILY_NEW_USERS * multiplier),
        'sessions': int(BASE_DAILY_SESSIONS * multiplier),
        'transactions': int(DAILY_TRANSACTIONS * multiplier),
        'revenue': round(DAILY_TRANSACTIONS * multiplier * AVERAGE_TRANSACTION_VALUE, 2),
        
        # Lead-specific metrics
        'lead_users': int(BASE_DAILY_ACTIVE_USERS * multiplier * LEAD_PERCENTAGE_OF_TRAFFIC),
        'identified_leads': int(BASE_DAILY_NEW_USERS * multiplier * LEAD_PERCENTAGE_OF_TRAFFIC * LEAD_IDENTIFICATION_RATE),
        'paying_leads': int(BASE_DAILY_NEW_USERS * multiplier * LEAD_PERCENTAGE_OF_TRAFFIC * LEAD_CONVERSION_RATE),
    }

def is_lead_user(source_info):
    """Determine if user came from lead-heavy channel"""
    if source_info.get('lead_heavy'):
        return random.random() < (LEAD_PERCENTAGE_OF_TRAFFIC * 2)  # 2x more likely in paid
    return random.random() < (LEAD_PERCENTAGE_OF_TRAFFIC * 0.3)  # Much less in organic

def get_conversion_rates(is_lead):
    """Get appropriate conversion rates based on user type"""
    return LEAD_CONVERSION_RATES if is_lead else ANONYMOUS_CONVERSION_RATES

def get_campaign_for_traffic(source, medium, is_returning_user):
    """Get appropriate campaign based on traffic source and user status"""
    if medium != 'cpc':
        return None
    
    campaign_pool = PAID_CAMPAIGNS.get(f'{source}_{medium}', [])
    
    if is_returning_user:
        # Returning users can get acquisition or re-engagement campaigns
        available_campaigns = campaign_pool
    else:
        # New users only get acquisition, brand, promotion, leadgen campaigns
        available_campaigns = [c for c in campaign_pool if c['targeting'] in ['new_users', 'all_users']]
    
    if not available_campaigns:
        return None
    
    weights = [c['weight'] for c in available_campaigns]
    return random.choices(available_campaigns, weights=weights)[0]

def get_stripe_product_by_sku(sku):
    """Get product details by SKU"""
    for product in STRIPE_PRODUCTS:
        if product['sku'] == sku:
            return product
    return None

def select_product_tier(conversion_path=None):
    """Select a product tier based on conversion path"""
    if conversion_path and conversion_path in TRIAL_CONVERSION_PATHS:
        tier_dist = TRIAL_CONVERSION_PATHS[conversion_path]['tier_distribution']
        tier_names = list(tier_dist.keys())
        tier_weights = list(tier_dist.values())
        selected_tier = random.choices(tier_names, weights=tier_weights)[0]
    else:
        # Default distribution from product weights
        tier_weights = [p['tier_weight'] for p in STRIPE_PRODUCTS]
        selected_tier = random.choices(STRIPE_PRODUCTS, weights=tier_weights)[0]['sku'].lower()
    
    return get_stripe_product_by_sku(selected_tier.upper())

def select_form_type():
    """Select a form type based on distribution weights"""
    form_names = list(FORM_TYPES.keys())
    form_weights = [FORM_TYPES[f]['distribution_weight'] for f in form_names]
    return random.choices(form_names, weights=form_weights)[0]

def get_trial_path():
    """Determine if trial is self-service or sales-assisted"""
    paths = list(TRIAL_CONVERSION_PATHS.keys())
    weights = [TRIAL_CONVERSION_PATHS[p]['weight'] for p in paths]
    return random.choices(paths, weights=weights)[0]
