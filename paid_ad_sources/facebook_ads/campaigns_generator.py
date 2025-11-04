# campaigns_generator.py
"""
Facebook Ads Campaign Data Generator
Generates campaign data following Facebook Ads campaigns schema
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
import sys
import os
# Path to digital_analytics where shared_config lives
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'digital_analytics')))
from shared_config import PAID_CAMPAIGNS
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# Get Facebook campaign names from shared config
FACEBOOK_CAMPAIGNS = PAID_CAMPAIGNS['facebook_cpc']

@dlt.resource(write_disposition="replace", table_name="campaigns")
def campaigns():
    """Generate Facebook Ads campaign data"""
    
    account_id = "act_1234567890"
    
    objectives = ['OUTCOME_AWARENESS', 'OUTCOME_ENGAGEMENT', 'OUTCOME_LEADS', 'OUTCOME_SALES', 'OUTCOME_TRAFFIC']
    buying_types = ['AUCTION']
    bid_strategies = ['LOWEST_COST_WITHOUT_CAP', 'LOWEST_COST_WITH_BID_CAP', 'COST_CAP']
    statuses = ['ACTIVE', 'PAUSED']
    
    for campaign in FACEBOOK_CAMPAIGNS:
        campaign_id = f"2385{campaign['id']}"  # Use campaign ID from config
        campaign_name = campaign['name']
        
        # Campaigns started throughout the year
        created = datetime.now() - timedelta(days=365 - (campaign['id'] % 100 * 4))
        updated = datetime.now() - timedelta(days=random.randint(1, 7))
        start = created
        stop = None  # Keep campaigns running
        
        daily_budget = round(random.uniform(100, 1000), 2)
        
        yield {
            'id': campaign_id,
            'account_id': account_id,
            'name': campaign_name,
            'objective': random.choice(objectives),
            'buying_type': 'AUCTION',
            'bid_strategy': random.choice(bid_strategies),
            'daily_budget': daily_budget,
            'lifetime_budget': None,
            'spend_cap': round(daily_budget * 180, 2),
            'budget_remaining': round(random.uniform(1000, 10000), 2),
            'budget_rebalance_flag': False,
            'status': random.choice(statuses),
            'configured_status': random.choice(statuses),
            'effective_status': random.choice(statuses),
            'created_time': created.strftime('%Y-%m-%dT%H:%M:%S+0000'),
            'updated_time': updated.strftime('%Y-%m-%dT%H:%M:%S+0000'),
            'start_time': start.strftime('%Y-%m-%dT%H:%M:%S+0000'),
            'stop_time': None,
            'special_ad_category': 'NONE',
            'special_ad_category_country': [],
            'adlabels': [],
            'issues_info': [],
            'boosted_object_id': None,
            'source_campaign_id': None,
            'smart_promotion_type': None,
            '_generated_at': datetime.now().isoformat(),
            '_campaign_type': campaign['type'],
            '_targeting': campaign['targeting'],
        }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads_campaigns",
        destination="filesystem",
        dataset_name="facebook_ads"
    )
    
    load_info = pipeline.run(campaigns(), loader_file_format="parquet")
    print(f"✓ Generated {len(FACEBOOK_CAMPAIGNS)} campaigns")
