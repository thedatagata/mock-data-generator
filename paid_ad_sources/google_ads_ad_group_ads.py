"""
Google Ads Ad Group Ad Data Generator
Generates responsive search ads with multiple headlines and descriptions
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

# Headline templates (max 30 chars)
headline_templates = [
    "Best {topic} Software",
    "{topic} Made Simple",
    "Top {topic} Platform",
    "{topic} for {role}",
    "Try {topic} Free",
    "Get {topic} Today",
    "{topic} Solution",
    "Trusted {topic} Tool",
]

# Description templates (max 90 chars)
description_templates = [
    "Transform your business with our {topic} solution. Get started free.",
    "Join thousands using our {topic} platform. Try it today.",
    "Streamline {topic} with our tools. Try it free for 14 days.",
    "All-in-one {topic} software. No credit card required."
]

print(f"Generating ads...")


@dlt.resource(write_disposition="replace")
def ad_group_ad():
    """Generate responsive search ads for each ad group"""
    
    ad_id = 20001
    ad_group_id = 5001
    
    topics = leads_df['industry'].dropna().unique()[:15] if 'industry' in leads_df.columns else ['Software', 'Marketing', 'Sales']
    roles = leads_df['job_function'].dropna().unique()[:10] if 'job_function' in leads_df.columns else ['Teams', 'Pros', 'Experts']
    
    for _, campaign in campaigns_df.iterrows():
        campaign_id = int(campaign['campaign_id'])
        
        num_groups = random.randint(4, 7)
        
        for group_idx in range(num_groups):
            current_ad_group_id = ad_group_id + group_idx
            
            # 2-3 ads per ad group
            num_ads = random.randint(2, 3)
            
            for ad_idx in range(num_ads):
                topic = random.choice(topics)
                role = random.choice(roles)
                
                # Generate 5-10 headlines
                headlines = []
                for _ in range(random.randint(5, 10)):
                    template = random.choice(headline_templates)
                    headline = template.format(topic=topic, role=role)[:30]
                    headlines.append(headline)
                
                # Generate 2-4 descriptions
                descriptions = []
                for _ in range(random.randint(2, 4)):
                    template = random.choice(description_templates)
                    desc = template.format(topic=topic)[:90]
                    descriptions.append(desc)
                
                # Ad strength
                ad_strength = random.choices(
                    ['POOR', 'AVERAGE', 'GOOD', 'EXCELLENT'],
                    weights=[0.1, 0.3, 0.4, 0.2]
                )[0]
                
                # Status
                status = 'ENABLED' if random.random() < 0.85 else 'PAUSED'
                
                yield {
                    'customer_id': customer_id,
                    'ad_group_id': current_ad_group_id,
                    'ad_id': ad_id,
                    'resource_name': f"customers/{customer_id}/adGroupAds/{current_ad_group_id}~{ad_id}",
                    'ad_group': f"customers/{customer_id}/adGroups/{current_ad_group_id}",
                    'ad_resource_name': f"customers/{customer_id}/ads/{ad_id}",
                    'status': status,
                    'ad_type': 'RESPONSIVE_SEARCH_AD',
                    'ad_strength': ad_strength,
                    'headlines': headlines,
                    'descriptions': descriptions,
                    'final_urls': ['https://example.com/landing'],
                    'policy_summary_approval_status': 'APPROVED',
                    '_generated_at': datetime.now().isoformat()
                }
                ad_id += 1
        
        ad_group_id += num_groups


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_ad_group_ads",
        destination="filesystem",
        dataset_name="google_ads"
    )
    
    load_info = pipeline.run(
        ad_group_ad(),
        loader_file_format="parquet"
    )
    
    print("\n✓ Ad group ads data generated")
    print(f"Loads: {len(load_info.loads_ids)}")
