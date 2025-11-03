# google_ads_click_view.py
"""
Google Ads Click View Data Generator - 365 days
"""
import dlt
from datetime import datetime, timedelta
import random
import string

random.seed(54321)

def generate_gclid():
    return 'Cj0KCQiA' + ''.join(random.choices(string.ascii_letters + string.digits + '_-', k=20))

CITIES = [
    ('1014221', '2840', 'San Francisco'),
    ('1023191', '2840', 'New York'),
    ('1013962', '2840', 'Los Angeles'),
    ('1014044', '2840', 'Chicago'),
    ('1022183', '2840', 'Austin')
]

KEYWORDS = ['b2b software', 'enterprise solutions', 'business platform', 'saas product', 'marketing tool']

@dlt.resource(write_disposition="replace")
def click_view():
    """Generate 365 days of click data"""
    
    customer_id = 1234567890
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    for single_date in pd.date_range(start_date, end_date):
        date_str = single_date.strftime('%Y-%m-%d')
        num_clicks = random.randint(30, 120)  # More clicks per day
        
        for _ in range(num_clicks):
            campaign_id = random.randint(1001, 1008)
            ad_group_id = random.randint(5001, 5040)
            ad_id = random.randint(20001, 20100)
            
            city_id, country_id, city_name = random.choice(CITIES)
            
            yield {
                'gclid': generate_gclid(),
                'resource_name': f"customers/{customer_id}/clickViews/{date_str}~{generate_gclid()}",
                'ad_group_ad': f"customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}",
                'campaign_location_target': None,
                'keyword': f"customers/{customer_id}/adGroupCriteria/{ad_group_id}~{random.randint(10001, 10999)}",
                'user_list': None,
                'keyword_text': random.choice(KEYWORDS),
                'keyword_match_type': random.choice(['EXACT', 'PHRASE', 'BROAD']),
                'page_number': random.randint(1, 3),
                'area_of_interest_city': f"geoTargetConstants/{city_id}",
                'area_of_interest_country': f"geoTargetConstants/{country_id}",
                'area_of_interest_metro': None,
                'area_of_interest_region': None,
                'area_of_interest_most_specific': f"geoTargetConstants/{city_id}",
                'location_city': f"geoTargetConstants/{city_id}",
                'location_country': f"geoTargetConstants/{country_id}",
                'location_metro': None,
                'location_region': None,
                'location_most_specific': f"geoTargetConstants/{city_id}",
                'date': date_str,
                'ad_network_type': random.choice(['SEARCH', 'SEARCH_PARTNERS']),
                'click_type': random.choice(['HEADLINE', 'SITELINK', 'URL_CLICKS']),
                'device': random.choice(['DESKTOP', 'MOBILE', 'TABLET']),
                'month_of_year': single_date.strftime('%B').upper(),
                'slot': random.choice(['SEARCH_TOP', 'SEARCH_SIDE', 'SEARCH_OTHER']),
                'clicks': 1,
                '_extracted_at': datetime.utcnow().isoformat(),
            }

if __name__ == "__main__":
    import pandas as pd
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_click_view",
        destination="filesystem",
        dataset_name="google_ads"
    )
    load_info = pipeline.run(click_view(), loader_file_format="parquet")
    print("✓ Generated 365 days of click data (~15,000-40,000 clicks)")
