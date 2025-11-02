"""
Google Ads Click View Data Generator
Generates click-level data with gclid granularity
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
import string

# Set seed for reproducibility
random.seed(54321)

# Load existing data
print("Loading data...")
customer_df = pd.read_parquet('gs://mock-source-data/paid_advertising/google_ads/customer/1762099054.8781552.0ea9195f72.parquet')
campaigns_df = pd.read_parquet('gs://mock-source-data/paid_advertising/google_ads/campaigns/1762099577.1046817.c4f56dac83.parquet')
ad_groups_df = pd.read_parquet('gs://mock-source-data/paid_advertising/google_ads/ad_group/1762100784.7000284.26cbbaa9f0.parquet')
ads_df = pd.read_parquet('gs://mock-source-data/paid_advertising/google_ads/ad_group_ad/1762104143.908496.e5e36fc923.parquet')

customer_id = int(customer_df['customer_id'].iloc[0])
print(f"Customer ID: {customer_id}")
print(f"Campaigns: {len(campaigns_df)}, Ad Groups: {len(ad_groups_df)}, Ads: {len(ads_df)}")


def generate_gclid():
    """Generate realistic gclid"""
    return 'Cj0KCQiA' + ''.join(random.choices(string.ascii_letters + string.digits + '_-', k=20))


@dlt.resource(write_disposition="append")
def click_view():
    """Generate click view records for the past 90 days"""
    
    # Date range for clicks
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    # Get enabled campaigns
    enabled_campaigns = campaigns_df[campaigns_df['campaign_status'] == 'ENABLED']
    
    # Generate clicks per day
    for single_date in pd.date_range(start_date, end_date):
        date_str = single_date.strftime('%Y-%m-%d')
        
        # Random number of clicks per day (20-100)
        num_clicks = random.randint(20, 100)
        
        for _ in range(num_clicks):
            # Randomly select campaign, ad group, and ad
            campaign = enabled_campaigns.sample(1).iloc[0]
            campaign_id = campaign['campaign_id']
            
            # Get ad groups for this campaign
            campaign_ad_groups = ad_groups_df[ad_groups_df['campaign_id'] == campaign_id]
            if len(campaign_ad_groups) == 0:
                continue
            
            ad_group = campaign_ad_groups.sample(1).iloc[0]
            ad_group_id = ad_group['ad_group_id']
            
            # Get ads for this ad group
            group_ads = ads_df[ads_df['ad_group_id'] == ad_group_id]
            if len(group_ads) == 0:
                continue
            
            ad = group_ads.sample(1).iloc[0]
            ad_id = ad['ad_id']
            
            gclid = generate_gclid()
            
            # Location data (US cities)
            cities = [
                ('geoTargetConstants/1014221', 'geoTargetConstants/2840'),  # San Francisco, US
                ('geoTargetConstants/1023191', 'geoTargetConstants/2840'),  # New York, US
                ('geoTargetConstants/1013962', 'geoTargetConstants/2840'),  # Los Angeles, US
                ('geoTargetConstants/1014044', 'geoTargetConstants/2840'),  # Chicago, US
                ('geoTargetConstants/1022183', 'geoTargetConstants/2840'),  # Austin, US
            ]
            city, country = random.choice(cities)
            
            yield {
                # Primary identifiers
                'gclid': gclid,
                'resource_name': f"customers/{customer_id}/clickViews/{date_str}~{gclid}",
                
                # Associated resources
                'ad_group_ad': f"customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}",
                'campaign_location_target': None,
                'keyword': f"customers/{customer_id}/adGroupCriteria/{ad_group_id}~{random.randint(1000, 9999)}",
                'user_list': None,
                
                # Keyword info
                'keyword_text': random.choice(['b2b software', 'enterprise solutions', 'business platform', 'saas product']),
                'keyword_match_type': random.choice(['EXACT', 'PHRASE', 'BROAD']),
                'page_number': random.randint(1, 3),
                
                # Area of Interest
                'area_of_interest_city': city,
                'area_of_interest_country': country,
                'area_of_interest_metro': None,
                'area_of_interest_region': None,
                'area_of_interest_most_specific': city,
                
                # Location of Presence
                'location_city': city,
                'location_country': country,
                'location_metro': None,
                'location_region': None,
                'location_most_specific': city,
                
                # Segments
                'date': date_str,
                'ad_network_type': random.choice(['SEARCH', 'SEARCH_PARTNERS']),
                'click_type': random.choice(['HEADLINE', 'SITELINK', 'URL_CLICKS']),
                'device': random.choice(['DESKTOP', 'MOBILE', 'TABLET']),
                'month_of_year': single_date.strftime('%B').upper(),
                'slot': random.choice(['SEARCH_TOP', 'SEARCH_SIDE', 'SEARCH_OTHER']),
                
                # Metrics
                'clicks': 1,
                
                # Metadata
                '_extracted_at': datetime.utcnow().isoformat(),
            }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_click_view",
        destination="filesystem",
        dataset_name="google_ads"
    )
    
    load_info = pipeline.run(
        click_view(),
        loader_file_format="parquet"
    )
    
    print("\n✓ Click view data generated")
    print(f"Dataset: {load_info.dataset_name}")
    print(f"Loads: {len(load_info.loads_ids)}")
