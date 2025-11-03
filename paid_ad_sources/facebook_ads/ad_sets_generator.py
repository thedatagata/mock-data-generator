# ad_sets_generator.py
"""
Facebook Ads Ad Set Data Generator
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

CAMPAIGN_IDS = [f"23857{10000000 + i}" for i in range(8)]

AD_SET_TEMPLATES = [
    ("Prospecting - 25-34", "US,CA"),
    ("Prospecting - 35-44", "US,CA,GB"),
    ("Retargeting - Site Visitors", "US"),
    ("Lookalike - 1%", "US,CA"),
    ("Interest - Tech Enthusiasts", "US,GB,AU"),
]

@dlt.resource(write_disposition="replace", table_name="ad_sets")
def ad_sets():
    """Generate ad sets - 5 per campaign"""
    
    account_id = "act_1234567890"
    bid_strategies = ['LOWEST_COST_WITHOUT_CAP', 'COST_CAP']
    
    ad_set_counter = 0
    for campaign_id in CAMPAIGN_IDS:
        for template_name, countries in AD_SET_TEMPLATES:
            adset_id = f"23857{20000000 + ad_set_counter}"
            ad_set_counter += 1
            
            created = datetime.now() - timedelta(days=360)
            
            daily_budget = round(random.uniform(50, 200), 2)
            
            yield {
                'id': adset_id,
                'account_id': account_id,
                'campaign_id': campaign_id,
                'name': template_name,
                'effective_status': random.choice(['ACTIVE', 'PAUSED']),
                'daily_budget': daily_budget,
                'lifetime_budget': None,
                'budget_remaining': round(random.uniform(100, 2000), 2),
                'bid_strategy': random.choice(bid_strategies),
                'bid_amount': round(random.uniform(1, 5), 2),
                'bid_info': {},
                'bid_constraints': {},
                'created_time': created.strftime('%Y-%m-%dT%H:%M:%S+0000'),
                'updated_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S+0000'),
                'start_time': created.strftime('%Y-%m-%dT%H:%M:%S+0000'),
                'end_time': None,
                'promoted_object': {},
                'targeting': {
                    'geo_locations': {
                        'countries': countries.split(','),
                        'cities': [],
                    },
                    'age_min': 25,
                    'age_max': 54,
                    'genders': 0,
                    'interests': [],
                    'device_platforms': ['mobile', 'desktop'],
                },
                'adlabels': [],
                '_generated_at': datetime.now().isoformat(),
            }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads_ad_sets",
        destination="filesystem",
        dataset_name="facebook_ads"
    )
    
    load_info = pipeline.run(ad_sets(), loader_file_format="parquet")
    print(f"✓ Generated {len(CAMPAIGN_IDS) * len(AD_SET_TEMPLATES)} ad sets")
