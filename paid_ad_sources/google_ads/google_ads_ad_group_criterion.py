# google_ads_ad_group_criterion.py
"""
Google Ads Ad Group Criterion (Keywords) Data Generator
"""
import dlt
from datetime import datetime
import random

random.seed(54321)

KEYWORD_TEMPLATES = [
    '{topic} software', '{topic} platform', '{topic} solution',
    'best {topic} tool', '{topic} for teams', '{topic} automation'
]

TOPICS = ['marketing', 'sales', 'crm', 'analytics', 'automation', 'collaboration', 'productivity']

@dlt.resource(write_disposition="replace")
def ad_group_criterion():
    """Generate 15-20 keywords per ad group"""
    
    customer_id = 1234567890
    criterion_id = 10001
    
    for ad_group_id in range(5001, 5041):  # 40 ad groups
        num_keywords = random.randint(15, 20)
        
        for _ in range(num_keywords):
            template = random.choice(KEYWORD_TEMPLATES)
            topic = random.choice(TOPICS)
            keyword_text = template.format(topic=topic)
            
            match_type = random.choices(['EXACT', 'PHRASE', 'BROAD_MATCH'], weights=[0.3, 0.5, 0.2])[0]
            cpc_bid = random.randint(1500000, 12000000)
            quality_score = random.choices([3,4,5,6,7,8,9,10], weights=[0.05,0.10,0.15,0.20,0.20,0.15,0.10,0.05])[0]
            
            yield {
                'customer_id': customer_id,
                'ad_group_id': ad_group_id,
                'criterion_id': criterion_id,
                'resource_name': f"customers/{customer_id}/adGroupCriteria/{ad_group_id}~{criterion_id}",
                'ad_group': f"customers/{customer_id}/adGroups/{ad_group_id}",
                'type': 'KEYWORD',
                'status': 'ENABLED' if random.random() < 0.85 else 'PAUSED',
                'negative': False,
                'keyword_text': keyword_text,
                'keyword_match_type': match_type,
                'cpc_bid_micros': cpc_bid,
                'effective_cpc_bid_micros': cpc_bid,
                'quality_info_quality_score': quality_score,
                'approval_status': 'APPROVED',
                '_generated_at': datetime.now().isoformat()
            }
            criterion_id += 1

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_ad_group_criterion",
        destination="filesystem",
        dataset_name="google_ads"
    )
    load_info = pipeline.run(ad_group_criterion(), loader_file_format="parquet")
    print("✓ Generated 600-800 keywords")
