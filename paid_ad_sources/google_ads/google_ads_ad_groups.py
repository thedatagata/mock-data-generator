# google_ads_ad_groups.py
"""
Google Ads Ad Group Data Generator
"""
import dlt
from datetime import datetime
import random

random.seed(54321)

AD_GROUP_TEMPLATES = [
    "Prospecting - 25-34",
    "Prospecting - 35-44", 
    "Retargeting - Site Visitors",
    "Lookalike - Enterprise",
    "Interest - Tech Decision Makers"
]

@dlt.resource(write_disposition="replace")
def ad_group():
    """Generate 5 ad groups per campaign"""
    
    customer_id = 1234567890
    ad_group_id = 5001
    
    for campaign_id in range(1001, 1009):  # 8 campaigns
        for template in AD_GROUP_TEMPLATES:
            base_cpc = random.randint(2000000, 8000000)
            
            yield {
                'customer_id': customer_id,
                'ad_group_id': ad_group_id,
                'campaign_id': campaign_id,
                'ad_group_name': template,
                'resource_name': f"customers/{customer_id}/adGroups/{ad_group_id}",
                'campaign': f"customers/{customer_id}/campaigns/{campaign_id}",
                'status': 'ENABLED' if random.random() < 0.9 else 'PAUSED',
                'type': 'SEARCH_STANDARD',
                'ad_rotation_mode': 'OPTIMIZE',
                'cpc_bid_micros': base_cpc,
                'target_cpa_micros': base_cpc * random.randint(8, 15),
                'effective_target_cpa_micros': base_cpc * random.randint(8, 15),
                'optimized_targeting_enabled': random.choice([True, False]),
                '_generated_at': datetime.now().isoformat()
            }
            ad_group_id += 1

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_ad_groups",
        destination="filesystem",
        dataset_name="google_ads"
    )
    load_info = pipeline.run(ad_group(), loader_file_format="parquet")
    print(f"✓ Generated 40 ad groups (8 campaigns × 5)")
