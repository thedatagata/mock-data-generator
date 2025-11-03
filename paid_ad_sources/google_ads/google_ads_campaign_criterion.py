# google_ads_campaign_criterion.py
"""
Google Ads Campaign Criterion (Targeting) Data Generator
"""
import dlt
from datetime import datetime
import random

random.seed(54321)

STATES = ['CA', 'NY', 'TX', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI']

@dlt.resource(write_disposition="replace")
def campaign_criterion():
    """Generate targeting criteria per campaign"""
    
    customer_id = 1234567890
    criterion_id = 4001
    
    for campaign_id in range(1001, 1009):
        # Location targeting - 4-6 states
        for state in random.sample(STATES, random.randint(4, 6)):
            geo_target_id = 1000000 + abs(hash(state)) % 9000000
            
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
        
        # Device targeting
        for device, bid_mod in [('DESKTOP', 1.0), ('MOBILE', 1.1), ('TABLET', 0.9)]:
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
                'device_type': device,
                'display_name': device.title(),
                '_generated_at': datetime.now().isoformat()
            }
            criterion_id += 1

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_campaign_criterion",
        destination="filesystem",
        dataset_name="google_ads"
    )
    load_info = pipeline.run(campaign_criterion(), loader_file_format="parquet")
    print("✓ Generated campaign targeting criteria")
