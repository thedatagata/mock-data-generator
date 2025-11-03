# ads_insights_generator.py
"""
Facebook Ads Insights - 365 days of performance data
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

CAMPAIGN_NAMES = [
    "Spring Sale 2024",
    "Product Launch - Premium", 
    "Brand Awareness Q4",
    "Retargeting - Cart Abandoners",
    "Holiday Promotion 2024",
    "Lead Generation - Whitepaper",
    "Customer Acquisition - Trial",
    "Re-engagement Campaign"
]

@dlt.resource(write_disposition="replace", table_name="ads_insights")
def ads_insights():
    """Generate 365 days of insights for all ads"""
    
    account_id = "act_1234567890"
    account_name = "Primary Ad Account"
    
    # 80 ads total (8 campaigns * 5 ad sets * 2 ads)
    ad_count = 80
    
    objectives = ['OUTCOME_LEADS', 'OUTCOME_SALES', 'OUTCOME_AWARENESS']
    optimization_goals = ['LINK_CLICKS', 'LANDING_PAGE_VIEWS', 'OFFSITE_CONVERSIONS']
    
    start_date = datetime.now() - timedelta(days=365)
    
    for day in range(365):
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Generate insights for 20 active ads per day (rotating subset)
        active_ads = random.sample(range(ad_count), 20)
        
        for ad_index in active_ads:
            campaign_index = ad_index // 10
            ad_set_index = ad_index // 2
            
            campaign_id = f"23857{10000000 + campaign_index}"
            adset_id = f"23857{20000000 + ad_set_index}"
            ad_id = f"23857{30000000 + ad_index}"
            
            impressions = random.randint(1000, 20000)
            reach = int(impressions * random.uniform(0.7, 0.9))
            clicks = int(impressions * random.uniform(0.01, 0.04))
            spend = round(random.uniform(50, 300), 2)
            
            conversions_count = int(clicks * random.uniform(0.02, 0.08))
            conversion_value = round(conversions_count * random.uniform(50, 150), 2)
            
            yield {
                'account_id': account_id,
                'account_name': account_name,
                'account_currency': 'USD',
                'campaign_id': campaign_id,
                'campaign_name': CAMPAIGN_NAMES[campaign_index],
                'adset_id': adset_id,
                'adset_name': f"AdSet {ad_set_index}",
                'ad_id': ad_id,
                'ad_name': f"Ad {ad_index}",
                'date_start': date_str,
                'date_stop': date_str,
                'created_time': current_date.strftime('%Y-%m-%dT%H:%M:%S+0000'),
                'updated_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S+0000'),
                'objective': random.choice(objectives),
                'optimization_goal': random.choice(optimization_goals),
                'buying_type': 'AUCTION',
                'attribution_setting': '7d_click_1d_view',
                'impressions': impressions,
                'reach': reach,
                'frequency': round(impressions / reach, 2),
                'clicks': clicks,
                'unique_clicks': int(clicks * 0.85),
                'spend': spend,
                'social_spend': round(spend * 0.2, 2),
                'inline_link_clicks': int(clicks * 0.8),
                'unique_inline_link_clicks': int(clicks * 0.7),
                'inline_link_click_ctr': round((clicks * 0.8 / impressions) * 100, 2),
                'unique_inline_link_click_ctr': round((clicks * 0.7 / reach) * 100, 2),
                'inline_post_engagement': int(impressions * 0.03),
                'cpc': round(spend / clicks, 2) if clicks > 0 else 0,
                'cpm': round((spend / impressions) * 1000, 2),
                'cpp': round((spend / reach) * 1000, 2),
                'ctr': round((clicks / impressions) * 100, 2),
                'unique_ctr': round((clicks * 0.85 / reach) * 100, 2),
                'cost_per_inline_link_click': round(spend / (clicks * 0.8), 2) if clicks > 0 else 0,
                'cost_per_unique_click': round(spend / (clicks * 0.85), 2) if clicks > 0 else 0,
                'cost_per_unique_inline_link_click': round(spend / (clicks * 0.7), 2) if clicks > 0 else 0,
                'cost_per_inline_post_engagement': round(spend / (impressions * 0.03), 2),
                'actions': [
                    {'action_type': 'link_click', 'value': str(int(clicks * 0.8))},
                    {'action_type': 'offsite_conversion.fb_pixel_purchase', 'value': str(conversions_count)},
                ],
                'action_values': [
                    {'action_type': 'offsite_conversion.fb_pixel_purchase', 'value': str(conversion_value)},
                ],
                'unique_actions': [
                    {'action_type': 'link_click', 'value': str(int(clicks * 0.7))},
                ],
                'conversions': [
                    {'action_type': 'offsite_conversion.fb_pixel_purchase', 'value': str(conversions_count)},
                ],
                'conversion_values': [
                    {'action_type': 'offsite_conversion.fb_pixel_purchase', 'value': str(conversion_value)},
                ],
                'cost_per_conversion': [
                    {'action_type': 'offsite_conversion.fb_pixel_purchase', 
                     'value': str(round(spend / conversions_count, 2)) if conversions_count > 0 else '0'},
                ],
                'cost_per_action_type': [
                    {'action_type': 'link_click', 'value': str(round(spend / (clicks * 0.8), 2)) if clicks > 0 else '0'},
                ],
                'outbound_clicks': [{'action_type': 'outbound_click', 'value': str(int(clicks * 0.75))}],
                'unique_outbound_clicks': [{'action_type': 'outbound_click', 'value': str(int(clicks * 0.65))}],
                'cost_per_outbound_click': [{'action_type': 'outbound_click', 'value': str(round(spend / (clicks * 0.75), 2)) if clicks > 0 else '0'}],
                'outbound_clicks_ctr': [{'action_type': 'outbound_click', 'value': str(round((clicks * 0.75 / impressions) * 100, 2))}],
                'unique_outbound_clicks_ctr': [{'action_type': 'outbound_click', 'value': str(round((clicks * 0.65 / reach) * 100, 2))}],
                'purchase_roas': [{'action_type': 'offsite_conversion.fb_pixel_purchase', 'value': str(round(conversion_value / spend, 2)) if spend > 0 else '0'}],
                'website_purchase_roas': [{'action_type': 'offsite_conversion.fb_pixel_purchase', 'value': str(round(conversion_value / spend, 2)) if spend > 0 else '0'}],
                'video_play_actions': [],
                'video_30_sec_watched_actions': [],
                'video_p25_watched_actions': [],
                'video_p50_watched_actions': [],
                'video_p75_watched_actions': [],
                'video_p100_watched_actions': [],
                'quality_ranking': random.choice(['AVERAGE', 'ABOVE_AVERAGE']),
                'engagement_rate_ranking': random.choice(['AVERAGE', 'ABOVE_AVERAGE']),
                'conversion_rate_ranking': random.choice(['AVERAGE', 'ABOVE_AVERAGE']),
                'auction_bid': round(random.uniform(1, 3), 2),
                'auction_competitiveness': round(random.uniform(0.5, 0.9), 2),
                'auction_max_competitor_bid': round(random.uniform(1.5, 4), 2),
                'estimated_ad_recallers': int(reach * 0.2),
                'cost_per_estimated_ad_recallers': round(spend / (reach * 0.2), 2),
                'full_view_impressions': int(impressions * 0.85),
                'full_view_reach': int(reach * 0.85),
                'canvas_avg_view_percent': None,
                'canvas_avg_view_time': None,
                'instant_experience_clicks_to_open': None,
                'instant_experience_clicks_to_start': None,
                '_generated_at': datetime.now().isoformat(),
            }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads_insights",
        destination="filesystem",
        dataset_name="facebook_ads"
    )
    
    load_info = pipeline.run(ads_insights(), loader_file_format="parquet")
    print("✓ Generated 365 days of insights (~7,300 records)")
