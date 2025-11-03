# ads_generator.py  
"""
Facebook Ads Ad Data Generator
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# Match ad set count
AD_SET_COUNT = 40  # 8 campaigns * 5 ad sets
AD_CREATIVE_VARIANTS = ["Carousel", "Video", "Image", "Story"]

@dlt.resource(write_disposition="replace", table_name="ads")
def ads():
    """Generate 2 ad creatives per ad set"""
    
    account_id = "act_1234567890"
    statuses = ['ACTIVE', 'PAUSED']
    
    for i in range(AD_SET_COUNT):
        adset_id = f"23857{20000000 + i}"
        campaign_id = f"23857{10000000 + (i // 5)}"  # 5 ad sets per campaign
        
        for j, creative_type in enumerate(random.sample(AD_CREATIVE_VARIANTS, 2)):
            ad_id = f"23857{30000000 + (i * 2) + j}"
            
            created = datetime.now() - timedelta(days=350)
            
            yield {
                'id': ad_id,
                'account_id': account_id,
                'campaign_id': campaign_id,
                'adset_id': adset_id,
                'name': f"{creative_type} Ad - Variant {j+1}",
                'status': random.choice(statuses),
                'effective_status': random.choice(statuses),
                'bid_type': 'CPC',
                'bid_amount': None,
                'bid_info': {},
                'creative': {
                    'id': f"23857{40000000 + (i * 2) + j}",
                    'name': f"{creative_type} Creative",
                    'object_type': 'SHARE',
                },
                'targeting': {},
                'created_time': created.strftime('%Y-%m-%dT%H:%M:%S+0000'),
                'updated_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S+0000'),
                'tracking_specs': [],
                'conversion_specs': [],
                'adlabels': [],
                'recommendations': [],
                'source_ad_id': None,
                'last_updated_by_app_id': None,
                '_generated_at': datetime.now().isoformat(),
            }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads_ads",
        destination="filesystem",
        dataset_name="facebook_ads"
    )
    
    load_info = pipeline.run(ads(), loader_file_format="parquet")
    print(f"✓ Generated {AD_SET_COUNT * 2} ads")
