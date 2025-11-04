import dlt
import pyarrow as pa
import pyarrow.compute as pc
from typing import Iterator
from datetime import datetime, timedelta

# ============================================================================
# CONFORMED DIMENSIONS
# ============================================================================

@dlt.resource(name="dim_customer", write_disposition="replace", primary_key="customer_key")
def dim_customer() -> Iterator[pa.Table]:
    """Conformed customer dimension (ECM entity)"""
    pipeline = dlt.pipeline(
        pipeline_name="staging",
        destination="duckdb",
        dataset_name="analytics_staging"
    )
    
    query = """
    WITH email_bridge AS (
        SELECT DISTINCT
            LOWER(TRIM(email)) as email
        FROM (
            SELECT email FROM crm_staging.contacts WHERE email IS NOT NULL
            UNION
            SELECT email FROM stripe_staging.customers WHERE email IS NOT NULL
        )
    ),
    customer_spine AS (
        SELECT
            MD5(eb.email) as customer_key,
            eb.email,
            c.contact_id as crm_contact_id,
            s.customer_id as stripe_customer_id,
            -- CRM attributes
            c.first_name,
            c.last_name,
            c.company,
            c.job_title,
            c.lifecycle_stage,
            c.lead_source,
            c.created_at as crm_created_at,
            -- Stripe attributes
            s.created as stripe_created_at,
            s.delinquent,
            s.currency,
            -- Segmentation
            CASE
                WHEN s.customer_id IS NOT NULL THEN 'paying'
                WHEN c.lifecycle_stage IN ('customer', 'opportunity') THEN 'qualified'
                WHEN c.lifecycle_stage = 'lead' THEN 'lead'
                ELSE 'anonymous'
            END as customer_segment
        FROM email_bridge eb
        LEFT JOIN crm_staging.contacts c ON LOWER(TRIM(c.email)) = eb.email
        LEFT JOIN stripe_staging.customers s ON LOWER(TRIM(s.email)) = eb.email
    )
    SELECT * FROM customer_spine
    """
    
    result = pipeline.dataset().query(query)
    
    for batch in result.iter_arrow(chunk_size=10000):
        yield batch


@dlt.resource(name="dim_campaign", write_disposition="replace", primary_key="campaign_key")
def dim_campaign() -> Iterator[pa.Table]:
    """Campaign dimension from GA4 traffic sources"""
    pipeline = dlt.pipeline(
        pipeline_name="staging",
        destination="duckdb",
        dataset_name="analytics_staging"
    )
    
    query = """
    SELECT DISTINCT
        MD5(CONCAT(
            COALESCE(traffic_source, 'direct'), '|',
            COALESCE(medium, 'none'), '|',
            COALESCE(campaign, '(not set)')
        )) as campaign_key,
        traffic_source as source,
        medium,
        campaign,
        CASE
            WHEN medium IN ('cpc', 'ppc', 'paid') THEN 'paid'
            WHEN medium IN ('organic', 'referral') THEN 'organic'
            WHEN medium = 'email' THEN 'email'
            WHEN medium = 'social' THEN 'social'
            ELSE 'other'
        END as channel_group
    FROM ga4_staging.events
    WHERE traffic_source IS NOT NULL OR medium IS NOT NULL
    """
    
    result = pipeline.dataset().query(query)
    
    for batch in result.iter_arrow(chunk_size=10000):
        yield batch


# ============================================================================
# ENTITY-CENTRIC MODELS (ECM)
# ============================================================================

@dlt.resource(name="entity_customer", write_disposition="replace", primary_key="customer_key")
def entity_customer() -> Iterator[pa.Table]:
    """
    ECM: Customer entity with time-bound metrics (TBM)
    Supports marketing CAC, sales pipeline, product engagement
    """
    pipeline = dlt.pipeline(
        pipeline_name="staging",
        destination="duckdb",
        dataset_name="analytics_staging"
    )
    
    query = """
    WITH customer_base AS (
        SELECT * FROM dim_customer
    ),
    -- Web activity metrics (7d, 28d, 90d, all-time)
    web_activity AS (
        SELECT
            dc.customer_key,
            COUNT(DISTINCT CASE 
                WHEN e.event_timestamp >= CURRENT_DATE - INTERVAL '7 days' 
                THEN DATE(e.event_timestamp) 
            END) as visits_7d,
            COUNT(DISTINCT CASE 
                WHEN e.event_timestamp >= CURRENT_DATE - INTERVAL '28 days' 
                THEN DATE(e.event_timestamp) 
            END) as visits_28d,
            COUNT(DISTINCT CASE 
                WHEN e.event_timestamp >= CURRENT_DATE - INTERVAL '90 days' 
                THEN DATE(e.event_timestamp) 
            END) as visits_90d,
            COUNT(DISTINCT DATE(e.event_timestamp)) as visits_lifetime,
            
            MIN(e.event_timestamp) as first_seen_timestamp,
            MAX(e.event_timestamp) as last_seen_timestamp,
            
            -- Key events
            COUNT(CASE 
                WHEN e.event_name = 'sign_up' 
                AND e.event_timestamp >= CURRENT_DATE - INTERVAL '28 days'
                THEN 1 
            END) as signups_28d,
            COUNT(CASE WHEN e.event_name = 'sign_up' THEN 1 END) as signups_lifetime,
            
            -- First/last touch attribution
            FIRST_VALUE(dc2.campaign_key) OVER (
                PARTITION BY dc.customer_key 
                ORDER BY e.event_timestamp
            ) as first_touch_campaign_key,
            LAST_VALUE(dc2.campaign_key) OVER (
                PARTITION BY dc.customer_key 
                ORDER BY e.event_timestamp
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) as last_touch_campaign_key
        FROM customer_base dc
        LEFT JOIN ga4_staging.events e ON dc.email = e.user_id  -- assuming user_id contains email
        LEFT JOIN dim_campaign dc2 ON MD5(CONCAT(
            COALESCE(e.traffic_source, 'direct'), '|',
            COALESCE(e.medium, 'none'), '|',
            COALESCE(e.campaign, '(not set)')
        )) = dc2.campaign_key
        GROUP BY dc.customer_key
    ),
    -- Revenue metrics (MRR, LTV)
    revenue_metrics AS (
        SELECT
            dc.customer_key,
            SUM(CASE WHEN sub.status IN ('active', 'trialing') THEN sub.mrr ELSE 0 END) as current_mrr,
            MAX(sub.mrr) as peak_mrr,
            SUM(inv.amount_paid) as ltv,
            SUM(CASE 
                WHEN inv.created >= CURRENT_DATE - INTERVAL '28 days' 
                THEN inv.amount_paid 
                ELSE 0 
            END) as revenue_28d,
            SUM(CASE 
                WHEN inv.created >= CURRENT_DATE - INTERVAL '90 days' 
                THEN inv.amount_paid 
                ELSE 0 
            END) as revenue_90d,
            MIN(sub.created) as first_subscription_date,
            MAX(CASE WHEN sub.status IN ('active', 'trialing') THEN sub.subscription_id END) as active_subscription_id,
            COUNT(DISTINCT sub.subscription_id) as total_subscriptions
        FROM customer_base dc
        LEFT JOIN stripe_staging.subscriptions sub ON dc.stripe_customer_id = sub.customer_id
        LEFT JOIN stripe_staging.invoices inv ON dc.stripe_customer_id = inv.customer_id AND inv.paid = TRUE
        GROUP BY dc.customer_key
    ),
    -- Sales pipeline metrics
    sales_metrics AS (
        SELECT
            dc.customer_key,
            COUNT(DISTINCT opp.opportunity_id) as opportunities_total,
            COUNT(DISTINCT CASE WHEN opp.is_closed = FALSE THEN opp.opportunity_id END) as opportunities_open,
            COUNT(DISTINCT CASE WHEN opp.is_won = TRUE THEN opp.opportunity_id END) as opportunities_won,
            SUM(CASE WHEN opp.is_closed = FALSE THEN opp.amount ELSE 0 END) as pipeline_value,
            SUM(CASE WHEN opp.is_won = TRUE THEN opp.amount ELSE 0 END) as closed_won_value,
            MAX(opp.stage) as current_pipeline_stage,
            MAX(CASE WHEN opp.is_closed = FALSE THEN opp.opportunity_created_at END) as latest_opportunity_date
        FROM customer_base dc
        LEFT JOIN crm_staging.opportunities opp ON dc.crm_contact_id = opp.contact_id
        GROUP BY dc.customer_key
    ),
    -- Activity metrics
    activity_metrics AS (
        SELECT
            dc.customer_key,
            COUNT(CASE 
                WHEN act.activity_date >= CURRENT_DATE - INTERVAL '28 days' 
                THEN 1 
            END) as activities_28d,
            COUNT(*) as activities_lifetime,
            MAX(act.activity_date) as last_activity_date
        FROM customer_base dc
        LEFT JOIN crm_staging.activities act ON dc.crm_contact_id = act.contact_id
        GROUP BY dc.customer_key
    )
    SELECT
        cb.*,
        
        -- Web metrics (TBM pattern)
        wa.visits_7d,
        wa.visits_28d,
        wa.visits_90d,
        wa.visits_lifetime,
        wa.signups_28d,
        wa.signups_lifetime,
        wa.first_seen_timestamp,
        wa.last_seen_timestamp,
        DATEDIFF('day', wa.first_seen_timestamp, CURRENT_DATE) as customer_age_days,
        
        -- Attribution
        wa.first_touch_campaign_key,
        wa.last_touch_campaign_key,
        
        -- Revenue (TBM pattern)
        rm.current_mrr,
        rm.peak_mrr,
        rm.ltv,
        rm.revenue_28d,
        rm.revenue_90d,
        rm.first_subscription_date,
        rm.active_subscription_id,
        rm.total_subscriptions,
        
        -- Sales
        sm.opportunities_total,
        sm.opportunities_open,
        sm.opportunities_won,
        sm.pipeline_value,
        sm.closed_won_value,
        sm.current_pipeline_stage,
        sm.latest_opportunity_date,
        
        -- Activity
        am.activities_28d,
        am.activities_lifetime,
        am.last_activity_date,
        
        -- Derived flags
        CASE WHEN wa.visits_7d >= 3 THEN TRUE ELSE FALSE END as is_active_7d,
        CASE WHEN rm.current_mrr > 0 THEN TRUE ELSE FALSE END as is_paying,
        CASE WHEN sm.opportunities_open > 0 THEN TRUE ELSE FALSE END as is_in_sales_pipeline
        
    FROM customer_base cb
    LEFT JOIN web_activity wa ON cb.customer_key = wa.customer_key
    LEFT JOIN revenue_metrics rm ON cb.customer_key = rm.customer_key
    LEFT JOIN sales_metrics sm ON cb.customer_key = sm.customer_key
    LEFT JOIN activity_metrics am ON cb.customer_key = am.customer_key
    """
    
    result = pipeline.dataset().query(query)
    
    for batch in result.iter_arrow(chunk_size=10000):
        yield batch


@dlt.resource(name="entity_campaign", write_disposition="replace", primary_key="campaign_key")
def entity_campaign() -> Iterator[pa.Table]:
    """
    ECM: Campaign entity with performance metrics
    Supports CAC, ROAS, attribution analysis
    """
    pipeline = dlt.pipeline(
        pipeline_name="staging",
        destination="duckdb",
        dataset_name="analytics_staging"
    )
    
    query = """
    WITH campaign_base AS (
        SELECT * FROM dim_campaign
    ),
    -- Traffic metrics
    traffic_metrics AS (
        SELECT
            dc.campaign_key,
            COUNT(DISTINCT e.user_pseudo_id) as unique_users,
            COUNT(DISTINCT e.session_id) as sessions,
            COUNT(DISTINCT CASE 
                WHEN ep.param_key = 'session_engaged' AND ep.value = '1' 
                THEN e.session_id 
            END) as engaged_sessions,
            COUNT(*) as events
        FROM dim_campaign dc
        JOIN ga4_staging.events e ON MD5(CONCAT(
            COALESCE(e.traffic_source, 'direct'), '|',
            COALESCE(e.medium, 'none'), '|',
            COALESCE(e.campaign, '(not set)')
        )) = dc.campaign_key
        LEFT JOIN ga4_staging.event_params ep ON e.event_id = ep.event_id
        GROUP BY dc.campaign_key
    ),
    -- Conversion metrics (first-touch)
    first_touch_conversions AS (
        SELECT
            ec.first_touch_campaign_key as campaign_key,
            COUNT(DISTINCT ec.customer_key) as first_touch_customers,
            SUM(ec.ltv) as first_touch_ltv,
            SUM(ec.revenue_90d) as first_touch_revenue_90d
        FROM entity_customer ec
        WHERE ec.first_touch_campaign_key IS NOT NULL
        GROUP BY ec.first_touch_campaign_key
    ),
    -- Last-touch conversions
    last_touch_conversions AS (
        SELECT
            ec.last_touch_campaign_key as campaign_key,
            COUNT(DISTINCT ec.customer_key) as last_touch_customers,
            SUM(ec.ltv) as last_touch_ltv,
            SUM(ec.revenue_90d) as last_touch_revenue_90d
        FROM entity_customer ec
        WHERE ec.last_touch_campaign_key IS NOT NULL
        GROUP BY ec.last_touch_campaign_key
    ),
    -- Ad spend (placeholder - integrate with ad platforms)
    ad_spend AS (
        SELECT
            campaign_key,
            0 as total_spend  -- TODO: Join with Google Ads/Facebook Ads
        FROM campaign_base
    )
    SELECT
        cb.*,
        tm.unique_users,
        tm.sessions,
        tm.engaged_sessions,
        tm.events,
        ROUND(tm.engaged_sessions::FLOAT / NULLIF(tm.sessions, 0), 3) as engagement_rate,
        
        -- First-touch attribution
        ftc.first_touch_customers,
        ftc.first_touch_ltv,
        ftc.first_touch_revenue_90d,
        ROUND(ftc.first_touch_ltv / NULLIF(ftc.first_touch_customers, 0), 2) as first_touch_ltv_per_customer,
        
        -- Last-touch attribution
        ltc.last_touch_customers,
        ltc.last_touch_ltv,
        ltc.last_touch_revenue_90d,
        ROUND(ltc.last_touch_ltv / NULLIF(ltc.last_touch_customers, 0), 2) as last_touch_ltv_per_customer,
        
        -- Performance metrics
        asp.total_spend,
        ROUND(asp.total_spend / NULLIF(ftc.first_touch_customers, 0), 2) as first_touch_cac,
        ROUND(asp.total_spend / NULLIF(ltc.last_touch_customers, 0), 2) as last_touch_cac,
        ROUND(ftc.first_touch_ltv / NULLIF(asp.total_spend, 0), 2) as first_touch_roas,
        ROUND(ltc.last_touch_ltv / NULLIF(asp.total_spend, 0), 2) as last_touch_roas
        
    FROM campaign_base cb
    LEFT JOIN traffic_metrics tm ON cb.campaign_key = tm.campaign_key
    LEFT JOIN first_touch_conversions ftc ON cb.campaign_key = ftc.campaign_key
    LEFT JOIN last_touch_conversions ltc ON cb.campaign_key = ltc.campaign_key
    LEFT JOIN ad_spend asp ON cb.campaign_key = asp.campaign_key
    """
    
    result = pipeline.dataset().query(query)
    
    for batch in result.iter_arrow(chunk_size=10000):
        yield batch


@dlt.resource(name="entity_product_funnel", write_disposition="replace")
def entity_product_funnel() -> Iterator[pa.Table]:
    """
    ECM: Product funnel analysis
    Tracks PLG self-serve flow: visit → signup → trial → payment
    """
    pipeline = dlt.pipeline(
        pipeline_name="staging",
        destination="duckdb",
        dataset_name="analytics_staging"
    )
    
    query = """
    WITH funnel_events AS (
        SELECT
            user_pseudo_id,
            event_timestamp,
            event_name,
            CASE
                WHEN event_name = 'page_view' AND page_path LIKE '%/signup%' THEN 'signup_page_view'
                WHEN event_name = 'sign_up' THEN 'signup_complete'
                WHEN event_name = 'page_view' AND page_path LIKE '%/trial%' THEN 'trial_start'
                WHEN event_name IN ('add_payment_info', 'begin_checkout') THEN 'payment_intent'
                WHEN event_name = 'purchase' THEN 'conversion'
            END as funnel_stage
        FROM ga4_staging.events e
        WHERE event_name IN ('page_view', 'sign_up', 'add_payment_info', 'begin_checkout', 'purchase')
    ),
    funnel_progression AS (
        SELECT
            user_pseudo_id,
            MIN(CASE WHEN funnel_stage = 'signup_page_view' THEN event_timestamp END) as signup_page_view_at,
            MIN(CASE WHEN funnel_stage = 'signup_complete' THEN event_timestamp END) as signup_complete_at,
            MIN(CASE WHEN funnel_stage = 'trial_start' THEN event_timestamp END) as trial_start_at,
            MIN(CASE WHEN funnel_stage = 'payment_intent' THEN event_timestamp END) as payment_intent_at,
            MIN(CASE WHEN funnel_stage = 'conversion' THEN event_timestamp END) as conversion_at
        FROM funnel_events
        GROUP BY user_pseudo_id
    ),
    funnel_metrics AS (
        SELECT
            user_pseudo_id,
            signup_page_view_at,
            signup_complete_at,
            trial_start_at,
            payment_intent_at,
            conversion_at,
            
            -- Time to progress
            DATEDIFF('hour', signup_page_view_at, signup_complete_at) as hours_to_signup,
            DATEDIFF('hour', signup_complete_at, trial_start_at) as hours_signup_to_trial,
            DATEDIFF('hour', trial_start_at, payment_intent_at) as hours_trial_to_intent,
            DATEDIFF('hour', payment_intent_at, conversion_at) as hours_intent_to_conversion,
            DATEDIFF('hour', signup_page_view_at, conversion_at) as hours_to_conversion,
            
            -- Stage reached
            CASE
                WHEN conversion_at IS NOT NULL THEN 'converted'
                WHEN payment_intent_at IS NOT NULL THEN 'payment_intent'
                WHEN trial_start_at IS NOT NULL THEN 'trial'
                WHEN signup_complete_at IS NOT NULL THEN 'signed_up'
                WHEN signup_page_view_at IS NOT NULL THEN 'viewed_signup'
                ELSE 'no_activity'
            END as funnel_stage_reached,
            
            -- Dropoff detection
            CASE
                WHEN signup_page_view_at IS NOT NULL AND signup_complete_at IS NULL THEN 'signup_page'
                WHEN signup_complete_at IS NOT NULL AND trial_start_at IS NULL THEN 'post_signup'
                WHEN trial_start_at IS NOT NULL AND payment_intent_at IS NULL THEN 'trial'
                WHEN payment_intent_at IS NOT NULL AND conversion_at IS NULL THEN 'payment_intent'
            END as dropoff_stage
        FROM funnel_progression
    )
    SELECT * FROM funnel_metrics
    """
    
    result = pipeline.dataset().query(query)
    
    for batch in result.iter_arrow(chunk_size=10000):
        yield batch


# ============================================================================
# ORCHESTRATION
# ============================================================================

def run_entity_models():
    """Execute entity-centric modeling pipeline"""
    pipeline = dlt.pipeline(
        pipeline_name="entity_models",
        destination="filesystem",
        dataset_name="entity_analytics",
        destination_kwargs={
            "layout": "{table_name}/{load_id}.parquet"
        }
    )
    
    # Build dimensions
    pipeline.run([
        dim_customer(),
        dim_campaign()
    ])
    print("✓ Dimensions built")
    
    # Build ECM entities
    pipeline.run([
        entity_customer(),
        entity_campaign(),
        entity_product_funnel()
    ])
    print("✓ Entity models built")
    
    print("\n✓ Entity-centric models ready for DuckDB Wasm ingestion")


if __name__ == "__main__":
    run_entity_models()
