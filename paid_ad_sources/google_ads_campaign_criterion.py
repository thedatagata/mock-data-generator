"""
Google Ads Campaign Criterion Data Generator
Generates campaign targeting criteria (locations, audiences, devices, etc.)
"""
import dlt
import pandas as pd
from datetime import datetime
import random


random.seed(54321)

# Load data
print("Loading data...")
customer_df = pd.read_parquet('gs://mock-source-data/paid_advertising/google_ads/customer/1762099054.8781552.0ea9195f72.parquet')
campaigns_df = pd.read_parquet('gs://mock-source-data/paid_advertising/google_ads/campaigns/1762099577.1046817.c4f56dac83.parquet')
byd_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_bookyourdata/bookyourdata/1762095548.474671.701ad8b601.parquet')
uplead_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_upleads/uplead/1762095612.4264941.2fb362f699.parquet')
leads_df = pd.concat([byd_df, uplead_df], ignore_index=True)

customer_id = int(customer_df['customer_id'].iloc[0])

# Extract targeting data from leads
top_states = leads_df['company_state'].value_counts().head(10).index.tolist() if 'company_state' in leads_df.columns else []
top_cities = leads_df['company_city'].value_counts().head(20).index.tolist() if 'company_city' in leads_df.columns else []
departments = leads_df['department'].value_counts().head(10).index.tolist() if 'department' in leads_df.columns else []
job_functions = leads_df['job_function'].value_counts().head(10).index.tolist() if 'job_function' in leads_df.columns else []

print(f"Campaigns: {len(campaigns_df)}")
print(f"Top states: {', '.join(top_states[:3])}")


@dlt.resource(write_disposition="replace")
def campaign_criterion():
    """Generate campaign targeting criteria"""
    
    criterion_id = 4001
    
    for _, campaign in campaigns_df.iterrows():
        campaign_id = int(campaign['campaign_id'])
        
        # LOCATION targeting - 4-6 states per campaign
        for state in random.sample(top_states, min(random.randint(4, 6), len(top_states))):
            geo_target_id = abs(hash(state)) % 9000000 + 1000000
            
            yield {
                'customer_id': customer_id,
                'campaign_id': campaign_id,
                'criterion_id': criterion_id,
                'campaign': f"customers/{customer_id}/campaigns/{campaign_id}",
                'resource_name': f"customers/{customer_id}/campaignCriteria/{campaign_id}~{criterion_id}",
                'type': 'LOCATION',
                'status': 'ENABLED',
                'negative': False,
                'bid_modifier': round(random.uniform(0.9, 1.2), 2),
                'location_geo_target_constant': f"geoTargetConstants/{geo_target_id}",
                'display_name': f"{state}, USA",
                '_generated_at': datetime.now().isoformat()
            }
            criterion_id += 1
        
        # DEVICE targeting - desktop, mobile, tablet
        for device_type in ['DESKTOP', 'MOBILE', 'TABLET']:
            bid_mod = {
                'DESKTOP': 1.0,
                'MOBILE': round(random.uniform(0.8, 1.3), 2),
                'TABLET': round(random.uniform(0.7, 1.1), 2)
            }[device_type]
            
            yield {
                'customer_id': customer_id,
                'campaign_id': campaign_id,
                'criterion_id': criterion_id,
                'campaign': f"customers/{customer_id}/campaigns/{campaign_id}",
                'resource_name': f"customers/{customer_id}/campaignCriteria/{campaign_id}~{criterion_id}",
                'type': 'DEVICE',
                'status': 'ENABLED',
                'negative': False,
                'bid_modifier': bid_mod,
                'device_type': device_type,
                'display_name': device_type.title(),
                '_generated_at': datetime.now().isoformat()
            }
            criterion_id += 1
        
        # USER_INTEREST targeting - 2-3 per campaign
        interests = random.sample(departments + job_functions, min(random.randint(2, 3), len(departments + job_functions)))
        for interest in interests:
            interest_id = 5000 + abs(hash(interest)) % 1000
            
            yield {
                'customer_id': customer_id,
                'campaign_id': campaign_id,
                'criterion_id': criterion_id,
                'campaign': f"customers/{customer_id}/campaigns/{campaign_id}",
                'resource_name': f"customers/{customer_id}/campaignCriteria/{campaign_id}~{criterion_id}",
                'type': 'USER_INTEREST',
                'status': 'ENABLED',
                'negative': False,
                'bid_modifier': round(random.uniform(0.9, 1.3), 2),
                'user_interest_user_interest_category': f"customers/{customer_id}/userInterests/{interest_id}",
                'display_name': interest,
                '_generated_at': datetime.now().isoformat()
            }
            criterion_id += 1
        
        # AGE_RANGE targeting - business decision makers typically 25-64
        for age_range in ['AGE_RANGE_25_34', 'AGE_RANGE_35_44', 'AGE_RANGE_45_54', 'AGE_RANGE_55_64']:
            yield {
                'customer_id': customer_id,
                'campaign_id': campaign_id,
                'criterion_id': criterion_id,
                'campaign': f"customers/{customer_id}/campaigns/{campaign_id}",
                'resource_name': f"customers/{customer_id}/campaignCriteria/{campaign_id}~{criterion_id}",
                'type': 'AGE_RANGE',
                'status': 'ENABLED',
                'negative': False,
                'bid_modifier': 1.0,
                'age_range_type': age_range,
                'display_name': age_range.replace('_', ' ').title(),
                '_generated_at': datetime.now().isoformat()
            }
            criterion_id += 1


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_campaign_criterion",
        destination="filesystem",
        dataset_name="google_ads"
    )
    
    load_info = pipeline.run(
        campaign_criterion(),
        loader_file_format="parquet"
    )
    
    print("\n✓ Campaign criterion data generated")
    print(f"Loads: {len(load_info.loads_ids)}")
