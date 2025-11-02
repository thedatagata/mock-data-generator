"""
Google Ads Ad Group Data Generator
Generates ad groups within campaigns, segmented by keyword themes
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

# Extract targeting themes from leads
industries = leads_df['industry'].value_counts().head(15).index.tolist() if 'industry' in leads_df.columns else []
job_functions = leads_df['job_function'].value_counts().head(10).index.tolist() if 'job_function' in leads_df.columns else []
departments = leads_df['department'].value_counts().head(10).index.tolist() if 'department' in leads_df.columns else []

# Company size segments
size_segments = ['Enterprise', 'Mid-Market', 'SMB']

print(f"Campaigns: {len(campaigns_df)}")
print(f"Industries: {', '.join(industries[:3])}")
print(f"Job functions: {', '.join(job_functions[:3])}")


@dlt.resource(write_disposition="replace")
def ad_group():
    """Generate ad groups for each campaign"""
    
    ad_group_id = 5001
    
    for _, campaign in campaigns_df.iterrows():
        campaign_id = int(campaign['campaign_id'])
        campaign_name = campaign['campaign_name']
        
        # 4-7 ad groups per campaign
        num_groups = random.randint(4, 7)
        
        # Mix of targeting strategies
        strategies = []
        
        # Industry-based ad groups (1-2)
        if industries:
            for industry in random.sample(industries, min(2, len(industries))):
                strategies.append(('industry', industry))
        
        # Job function-based (1-2)
        if job_functions:
            for func in random.sample(job_functions, min(2, len(job_functions))):
                strategies.append(('function', func))
        
        # Department-based (1)
        if departments:
            dept = random.choice(departments)
            strategies.append(('department', dept))
        
        # Company size-based (1)
        size = random.choice(size_segments)
        strategies.append(('size', size))
        
        # Generic brand/competitor (1)
        strategies.append(('generic', 'Brand Terms'))
        
        # Select strategies for this campaign
        selected = random.sample(strategies, min(num_groups, len(strategies)))
        
        for strategy_type, strategy_name in selected:
            # Generate ad group name
            if strategy_type == 'industry':
                ad_group_name = f"{industry} - {campaign_name.split(' - ')[0]}"
            elif strategy_type == 'function':
                ad_group_name = f"{strategy_name} - Targeting"
            elif strategy_type == 'department':
                ad_group_name = f"{strategy_name} Department"
            elif strategy_type == 'size':
                ad_group_name = f"{strategy_name} Companies"
            else:
                ad_group_name = strategy_name
            
            # Base CPC bid: $2-8 per click
            base_cpc = random.randint(2000000, 8000000)  # in micros
            
            # Status: 90% enabled, 10% paused
            status = 'ENABLED' if random.random() < 0.9 else 'PAUSED'
            
            yield {
                'customer_id': customer_id,
                'ad_group_id': ad_group_id,
                'campaign_id': campaign_id,
                'ad_group_name': ad_group_name,
                'resource_name': f"customers/{customer_id}/adGroups/{ad_group_id}",
                'campaign': f"customers/{customer_id}/campaigns/{campaign_id}",
                'status': status,
                'type': 'SEARCH_STANDARD',
                'ad_rotation_mode': 'OPTIMIZE',
                'cpc_bid_micros': base_cpc,
                'target_cpa_micros': base_cpc * random.randint(8, 15),  # Target CPA 8-15x CPC
                'effective_target_cpa_micros': base_cpc * random.randint(8, 15),
                'optimized_targeting_enabled': random.choice([True, False]),
                '_generated_at': datetime.now().isoformat(),
                '_targeting_theme': strategy_type,
                '_targeting_value': strategy_name
            }
            ad_group_id += 1


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_ad_groups",
        destination="filesystem",
        dataset_name="google_ads"
    )
    
    load_info = pipeline.run(
        ad_group(),
        loader_file_format="parquet"
    )
    
    print("\n✓ Ad group data generated")
    print(f"Loads: {len(load_info.loads_ids)}")
