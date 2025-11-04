"""
Shared configuration across Amplitude analysis scripts
"""
from event_taxonomy import SAAS_EVENT_TAXONOMY

# Key conversion events for GA4 reports
KEY_EVENTS = [
    'trial_started',
    'trial_converted', 
    'demo_requested',
    'payment_completed',
    'subscription_created',
    'account_created',
    'onboarding_completed',
    'first_project_created',
]

# High-intent events for lead scoring
HIGH_INTENT_EVENTS = [
    'demo_requested',
    'contact_sales_clicked',
    'pricing_calculator_used',
    'trial_started',
    'enterprise_plan_requested',
]

# Activation milestone events
ACTIVATION_EVENTS = [
    'onboarding_completed',
    'first_project_created',
    'first_integration_connected',
    'first_data_uploaded',
    'first_report_generated',
]

# Churn risk indicators
CHURN_RISK_EVENTS = [
    'payment_failed',
    'subscription_cancelled',
    'usage_decreased',
    'login_decreased',
    'negative_feedback',
    'account_deletion_requested',
]

# Expansion opportunity events  
EXPANSION_EVENTS = [
    'usage_limit_reached',
    'feature_limit_reached',
    'upgrade_prompt_clicked',
    'additional_seat_requested',
    'addon_viewed',
]
