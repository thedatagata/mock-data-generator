"""
Google Ads Ad Group Criterion Data Generator
Generates keywords and other targeting criteria for ad groups
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

# Keyword templates by category
keyword_templates = {
    'software': ['{industry} software', '{function} software', '{dept} tools', '{industry} platform'],
    'solution': ['{industry} solution', '{function} solution', 'best {industry} software', '{dept} management'],
    'tool': ['{industry} tool', '{function} tool', '{dept} automation', '{industry} analytics'],
    'platform': ['{industry} platform', '{function} platform', '{dept} platform', '{industry} dashboard'],
    'service': ['{industry} service', '{function} service', '{dept} consulting', '{industry} expert']
}

# Match types with distribution
match_types = ['EXACT', 'PHRASE', 'BROAD_MATCH']
match_type_weights = [0.3, 0.5, 0.2]  # 30% exact, 50% phrase, 20% broad

print(f"Generating keywords for campaigns...")


@dlt.resource(write_disposition="replace")
def ad_group_criterion():
    """Generate keywords for each ad group"""
    
    criterion_id = 10001
    
    # Load ad groups (we'll need to read from GCS after they're generated)
    # For now, generate based on campaign structure
    ad_group_id = 5001
    
    for _, campaign in campaigns_df.iterrows():
        campaign_id = int(campaign['campaign_id'])
        
        # 4-7 ad groups per campaign (matching ad_groups.py)
        num_groups = random.randint(4, 7)
        
        for group_idx in range(num_groups):
            current_ad_group_id = ad_group_id + group_idx
            
            # 10-25 keywords per ad group
            num_keywords = random.randint(10, 25)
            
            # Sample industries, functions, departments from leads
            industries_sample = leads_df['industry'].dropna().unique()[:20] if 'industry' in leads_df.columns else []
            functions_sample = leads_df['job_function'].dropna().unique()[:15] if 'job_function' in leads_df.columns else []
            depts_sample = leads_df['department'].dropna().unique()[:15] if 'department' in leads_df.columns else []
            
            for kw_idx in range(num_keywords):
                # Select template category
                category = random.choice(list(keyword_templates.keys()))
                template = random.choice(keyword_templates[category])
                
                # Fill template with data
                keyword_text = template.format(
                    industry=random.choice(industries_sample) if len(industries_sample) > 0 else 'technology',
                    function=random.choice(functions_sample) if len(functions_sample) > 0 else 'sales',
                    dept=random.choice(depts_sample) if len(depts_sample) > 0 else 'marketing'
                ).lower()
                
                # Match type
                match_type = random.choices(match_types, weights=match_type_weights)[0]
                
                # CPC bid: $1.50 - $12 per click
                cpc_bid = random.randint(1500000, 12000000)  # in micros
                
                # Quality score: 3-10, weighted toward 6-8
                quality_score = random.choices(
                    [3, 4, 5, 6, 7, 8, 9, 10],
                    weights=[0.05, 0.10, 0.15, 0.20, 0.20, 0.15, 0.10, 0.05]
                )[0]
                
                # Status: 85% enabled, 15% paused
                status = 'ENABLED' if random.random() < 0.85 else 'PAUSED'
                
                yield {
                    'customer_id': customer_id,
                    'ad_group_id': current_ad_group_id,
                    'criterion_id': criterion_id,
                    'resource_name': f"customers/{customer_id}/adGroupCriteria/{current_ad_group_id}~{criterion_id}",
                    'ad_group': f"customers/{customer_id}/adGroups/{current_ad_group_id}",
                    'type': 'KEYWORD',
                    'status': status,
                    'negative': False,
                    'keyword_text': keyword_text[:80],  # Max 80 chars
                    'keyword_match_type': match_type,
                    'cpc_bid_micros': cpc_bid,
                    'effective_cpc_bid_micros': cpc_bid,
                    'quality_info_quality_score': quality_score,
                    'approval_status': 'APPROVED',
                    '_generated_at': datetime.now().isoformat()
                }
                criterion_id += 1
        
        ad_group_id += num_groups


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_ad_group_criterion",
        destination="filesystem",
        dataset_name="google_ads"
    )
    
    load_info = pipeline.run(
        ad_group_criterion(),
        loader_file_format="parquet"
    )
    
    print("\n✓ Ad group criterion (keywords) data generated")
    print(f"Loads: {len(load_info.loads_ids)}")
