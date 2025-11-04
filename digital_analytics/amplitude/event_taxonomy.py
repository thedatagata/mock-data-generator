# Comprehensive SaaS Event Taxonomy
# Based on typical B2B SaaS customer journey

SAAS_EVENT_TAXONOMY = {
    # ==================== AWARENESS ====================
    'awareness': [
        'page_view',
        'blog_post_view',
        'documentation_view',
        'case_study_view',
        'video_play',
        'video_complete',
        'webinar_registration',
        'webinar_attended',
        'podcast_play',
    ],
    
    # ==================== INTEREST ====================
    'interest': [
        'pricing_page_view',
        'features_page_view',
        'comparison_page_view',
        'integration_page_view',
        'demo_video_play',
        'product_tour_started',
        'product_tour_completed',
        'search',
        'whitepaper_download',
        'ebook_download',
        'calculator_used',
    ],
    
    # ==================== CONSIDERATION ====================
    'consideration': [
        'demo_requested',
        'contact_sales_clicked',
        'contact_form_submit',
        'chat_initiated',
        'chat_message_sent',
        'pricing_calculator_used',
        'roi_calculator_used',
        'comparison_tool_used',
        'testimonials_viewed',
        'api_docs_viewed',
    ],
    
    # ==================== TRIAL / SIGNUP ====================
    'trial_signup': [
        'trial_started',
        'account_created',
        'signup_form_started',
        'signup_form_completed',
        'email_verified',
        'profile_completed',
        'onboarding_started',
        'onboarding_step_completed',
        'onboarding_completed',
        'invite_sent',
        'workspace_created',
    ],
    
    # ==================== ACTIVATION ====================
    'activation': [
        'first_login',
        'dashboard_viewed',
        'first_project_created',
        'first_integration_connected',
        'first_data_uploaded',
        'first_report_generated',
        'first_team_member_invited',
        'setup_wizard_completed',
        'api_key_generated',
        'sdk_installed',
    ],
    
    # ==================== PRODUCT USAGE ====================
    'product_usage': [
        'feature_used',
        'dashboard_viewed',
        'report_created',
        'report_exported',
        'data_imported',
        'data_exported',
        'filter_applied',
        'search_performed',
        'settings_changed',
        'integration_added',
        'integration_removed',
        'api_call_made',
        'workflow_created',
        'automation_triggered',
    ],
    
    # ==================== CONVERSION ====================
    'conversion': [
        'trial_converted',
        'plan_selected',
        'checkout_started',
        'payment_info_entered',
        'payment_completed',
        'subscription_created',
        'subscription_upgraded',
        'addon_purchased',
        'annual_plan_selected',
        'enterprise_plan_requested',
    ],
    
    # ==================== RETENTION ====================
    'retention': [
        'session_started',
        'daily_active',
        'weekly_active',
        'notification_clicked',
        'email_opened',
        'email_clicked',
        'in_app_message_viewed',
        'feature_announcement_viewed',
        'help_center_visited',
        'support_ticket_created',
        'feedback_submitted',
        'nps_survey_completed',
    ],
    
    # ==================== EXPANSION ====================
    'expansion': [
        'upgrade_prompt_viewed',
        'upgrade_prompt_clicked',
        'plan_comparison_viewed',
        'billing_page_viewed',
        'usage_limit_reached',
        'feature_limit_reached',
        'additional_seat_requested',
        'addon_viewed',
        'enterprise_contact_form',
    ],
    
    # ==================== CHURN RISK ====================
    'churn_risk': [
        'payment_failed',
        'trial_expiring_soon',
        'subscription_cancelled',
        'subscription_paused',
        'usage_decreased',
        'login_decreased',
        'support_ticket_unresolved',
        'negative_feedback',
        'unsubscribe_email',
        'account_deletion_requested',
    ],
}

# Engagement weights for each event
EVENT_ENGAGEMENT_SCORES = {
    # Awareness (1-3 points)
    'page_view': 1,
    'blog_post_view': 2,
    'documentation_view': 2,
    'video_play': 3,
    
    # Interest (3-8 points)
    'pricing_page_view': 8,
    'features_page_view': 5,
    'comparison_page_view': 6,
    'whitepaper_download': 12,
    'calculator_used': 7,
    
    # Consideration (10-20 points)
    'demo_requested': 20,
    'contact_sales_clicked': 18,
    'contact_form_submit': 15,
    'chat_initiated': 12,
    'pricing_calculator_used': 15,
    
    # Trial (20-30 points)
    'trial_started': 25,
    'account_created': 22,
    'email_verified': 20,
    'onboarding_completed': 28,
    
    # Activation (25-40 points)
    'first_project_created': 35,
    'first_integration_connected': 38,
    'first_report_generated': 32,
    'api_key_generated': 30,
    
    # Product Usage (5-15 points)
    'feature_used': 8,
    'report_created': 12,
    'data_imported': 15,
    'workflow_created': 18,
    
    # Conversion (50+ points)
    'trial_converted': 100,
    'payment_completed': 100,
    'subscription_created': 95,
    
    # Expansion (30-60 points)
    'subscription_upgraded': 60,
    'addon_purchased': 40,
    'additional_seat_requested': 35,
}

# Event flow probabilities (what events commonly lead to others)
EVENT_FLOW_PATTERNS = {
    'page_view': {
        'pricing_page_view': 0.15,
        'features_page_view': 0.12,
        'blog_post_view': 0.08,
        'documentation_view': 0.05,
    },
    
    'pricing_page_view': {
        'trial_started': 0.25,
        'demo_requested': 0.15,
        'contact_sales_clicked': 0.10,
        'features_page_view': 0.20,
    },
    
    'features_page_view': {
        'pricing_page_view': 0.30,
        'trial_started': 0.15,
        'product_tour_started': 0.20,
    },
    
    'demo_requested': {
        'trial_started': 0.40,
        'contact_sales_clicked': 0.25,
    },
    
    'trial_started': {
        'email_verified': 0.85,
        'onboarding_started': 0.75,
        'first_project_created': 0.45,
    },
    
    'onboarding_completed': {
        'first_project_created': 0.70,
        'first_integration_connected': 0.50,
    },
    
    'first_project_created': {
        'first_integration_connected': 0.60,
        'first_data_uploaded': 0.55,
        'first_report_generated': 0.50,
    },
    
    'usage_limit_reached': {
        'upgrade_prompt_clicked': 0.35,
        'plan_comparison_viewed': 0.28,
        'subscription_upgraded': 0.20,
    },
}

# Typical user journeys
JOURNEY_ARCHETYPES = {
    'self_service_fast': [
        'page_view', 'pricing_page_view', 'trial_started', 
        'onboarding_completed', 'first_project_created', 
        'trial_converted', 'payment_completed'
    ],
    
    'researcher': [
        'blog_post_view', 'documentation_view', 'features_page_view',
        'comparison_page_view', 'whitepaper_download', 'pricing_page_view',
        'trial_started', 'onboarding_completed', 'trial_converted'
    ],
    
    'sales_led': [
        'page_view', 'demo_requested', 'webinar_attended',
        'contact_sales_clicked', 'trial_started', 'onboarding_completed',
        'enterprise_plan_requested', 'payment_completed'
    ],
    
    'product_led': [
        'page_view', 'trial_started', 'onboarding_completed',
        'first_project_created', 'first_integration_connected',
        'feature_used', 'feature_used', 'trial_converted'
    ],
    
    'churner': [
        'trial_started', 'onboarding_started', 'usage_decreased',
        'login_decreased', 'trial_expiring_soon', 'subscription_cancelled'
    ],
}
