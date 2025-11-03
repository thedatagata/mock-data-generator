# google_ads_ad_group_ads.py
"""
Google Ads Ad Group Ad Data Generator
"""
import dlt
from datetime import datetime
import random

random.seed(54321)

HEADLINE_TEMPLATES = [
    "Best {topic} Software",
    "{topic} Made Simple",
    "Top {topic} Platform",
    "Try {topic} Free",
    "{topic} for Teams"
]

DESCRIPTION_TEMPLATES = [
    "Transform your business with our {topic} solution.",
    "Join thousands using our {topic} platform.",
    "Streamline {topic} with our tools."
]

TOPICS = ['Marketing', 'Sales', 'Analytics', 'CRM', 'Automation']

@dlt.resource(write_disposition="replace")
def ad_group_ad():
    """Generate 2-3 ads per ad group"""
    
    customer_id = 1234567890
    ad_id = 20001
    
    for ad_group_id in range(5001, 5041):  # 40 ad groups
        num_ads = random.randint(2, 3)
        
        for _ in range(num_ads):
            topic = random.choice(TOPICS)
            
            headlines = [t.format(topic=topic)[:30] for t in random.sample(HEADLINE_TEMPLATES, 5)]
            descriptions = [t.format(topic=topic)[:90] for t in random.sample(DESCRIPTION_TEMPLATES, 2)]
            
            yield {
                'customer_id': customer_id,
                'ad_group_id': ad_group_id,
                'ad_id': ad_id,
                'resource_name': f"customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}",
                'ad_group': f"customers/{customer_id}/adGroups/{ad_group_id}",
                'ad_resource_name': f"customers/{customer_id}/ads/{ad_id}",
                'status': 'ENABLED' if random.random() < 0.85 else 'PAUSED',
                'ad_type': 'RESPONSIVE_SEARCH_AD',
                'ad_strength': random.choices(['POOR', 'AVERAGE', 'GOOD', 'EXCELLENT'], weights=[0.1, 0.3, 0.4, 0.2])[0],
                'headlines': headlines,
                'descriptions': descriptions,
                'final_urls': ['https://example.com/landing'],
                'policy_summary_approval_status': 'APPROVED',
                '_generated_at': datetime.now().isoformat()
            }
            ad_id += 1

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_ad_group_ads",
        destination="filesystem",
        dataset_name="google_ads"
    )
    load_info = pipeline.run(ad_group_ad(), loader_file_format="parquet")
    print("✓ Generated 80-120 ads")
