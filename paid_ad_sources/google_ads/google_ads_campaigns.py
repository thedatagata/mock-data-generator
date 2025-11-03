# google_ads_campaigns.py
"""
Google Ads Campaigns Data Generator
"""
import dlt
from datetime import datetime, timedelta
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from shared_config import PAID_CAMPAIGNS

random.seed(54321)

# Get Google campaign names from shared config
GOOGLE_CAMPAIGNS = PAID_CAMPAIGNS['google_cpc']

@dlt.resource(write_disposition="replace")
def campaigns():
    """Generate campaigns with consistent IDs"""
    
    customer_id = 1234567890
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    
    for campaign in GOOGLE_CAMPAIGNS:
        campaign_id = campaign['id']
        campaign_name = campaign['name']
        campaign_type = 'SEARCH' if campaign['type'] in ['ACQUISITION', 'REENGAGEMENT'] else 'DISPLAY'
        
        yield {
            'customer_id': customer_id,
            'campaign_id': campaign_id,
            'campaign_name': campaign_name,
            'resource_name': f"customers/{customer_id}/campaigns/{campaign_id}",
            'campaign_status': 'ENABLED',
            'serving_status': 'SERVING',
            'ad_serving_optimization_status': 'OPTIMIZE',
            'advertising_channel_type': campaign_type,
            'advertising_channel_sub_type': 'UNSPECIFIED',
            'campaign_budget': f"customers/{customer_id}/campaignBudgets/{campaign_id}",
            'bidding_strategy_type': 'TARGET_CPA',
            'target_cpa_target_cpa_micros': int(random.uniform(65, 100) * 1_000_000),
            'target_cpa_cpc_bid_ceiling_micros': int(random.uniform(15, 25) * 1_000_000),
            'target_cpa_cpc_bid_floor_micros': int(random.uniform(2, 5) * 1_000_000),
            'start_date': start_date,
            'end_date': '2037-12-30',
            'network_settings_target_google_search': True,
            'network_settings_target_search_network': True if campaign_type == 'SEARCH' else False,
            'network_settings_target_content_network': True if campaign_type == 'DISPLAY' else False,
            'network_settings_target_partner_search_network': False,
            'tracking_url_template': f'https://example.com/?utm_source=google&utm_medium=cpc&utm_campaign={campaign_name}',
            'final_url_suffix': 'gclid={gclid}',
            'optimization_score': round(random.uniform(0.70, 0.95), 3),
            'experiment_type': 'BASE',
            'base_campaign': f"customers/{customer_id}/campaigns/{campaign_id}",
            '_generated_at': datetime.now().isoformat(),
            '_campaign_type': campaign['type'],
            '_targeting': campaign['targeting'],
        }

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_campaigns",
        destination="filesystem",
        dataset_name="google_ads"
    )
    load_info = pipeline.run(campaigns(), loader_file_format="parquet")
    print(f"✓ Generated {len(GOOGLE_CAMPAIGNS)} campaigns")
