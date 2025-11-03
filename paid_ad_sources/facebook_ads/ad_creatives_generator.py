# ad_creatives_generator.py
"""
Facebook Ads Ad Creative Data Generator
Generates ad creative data following Facebook Ads ad_creatives schema
"""
import dlt
from datetime import datetime
import random
from faker import Faker
import hashlib

fake = Faker()
Faker.seed(42)
random.seed(42)

@dlt.resource(write_disposition="replace", table_name="ad_creatives")
def ad_creatives():
    """Generate Facebook Ads ad creative data"""
    
    account_id = "act_1234567890"
    object_types = ['SHARE', 'PHOTO', 'VIDEO', 'LINK']
    call_to_actions = ['LEARN_MORE', 'SHOP_NOW', 'SIGN_UP', 'DOWNLOAD', 'CONTACT_US']
    
    # Generate 80 creatives matching 80 ads
    for i in range(80):
        creative_id = f"23857{40000000 + i}"
        object_type = random.choice(object_types)
        has_video = object_type == 'VIDEO'
        
        yield {
            'id': creative_id,
            'account_id': account_id,
            'name': f"Creative {i+1} - {object_type}",
            'title': fake.sentence(nb_words=6),
            'body': fake.text(max_nb_chars=150),
            'status': 'ACTIVE',
            'object_type': object_type,
            'image_url': fake.image_url() if not has_video else None,
            'image_hash': hashlib.md5(f"image_{i}".encode()).hexdigest() if not has_video else None,
            'thumbnail_url': fake.image_url(),
            'thumbnail_data_url': f"data:image/png;base64,{fake.sha256()[:100]}",
            'video_id': f"23857{50000000 + i}" if has_video else None,
            'link_url': fake.url(),
            'object_url': fake.url(),
            'template_url': None,
            'url_tags': f"utm_source=facebook&utm_medium=paid_social&utm_campaign=campaign_{i//10}",
            'call_to_action_type': random.choice(call_to_actions),
            'object_id': f"23857{60000000 + i}",
            'object_story_id': f"{random.randint(100000000000, 999999999999)}_{random.randint(100000000000, 999999999999)}",
            'effective_object_story_id': f"{random.randint(100000000000, 999999999999)}_{random.randint(100000000000, 999999999999)}",
            'link_og_id': None,
            'actor_id': f"1000{i:04d}",
            'instagram_actor_id': f"2000{i:04d}" if random.random() > 0.5 else None,
            'instagram_story_id': None,
            'effective_instagram_story_id': None,
            'instagram_permalink_url': None,
            'source_instagram_media_id': None,
            'object_story_spec': {
                'page_id': '123456789',
                'link_data': {
                    'link': fake.url(),
                    'message': fake.sentence(),
                    'name': fake.catch_phrase(),
                    'call_to_action': {
                        'type': random.choice(call_to_actions),
                        'value': {'link': fake.url()}
                    }
                }
            } if random.random() > 0.5 else {},
            'asset_feed_spec': {},
            'template_url_spec': {},
            'image_crops': {
                '100x100': [[0, 0], [100, 100]],
                '191x100': [[0, 0], [191, 100]],
            } if not has_video else {},
            'applink_treatment': None,
            'product_set_id': None,
            'adlabels': [],
            '_generated_at': datetime.now().isoformat(),
        }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="facebook_ads_creatives",
        destination="filesystem",
        dataset_name="facebook_ads"
    )
    
    load_info = pipeline.run(ad_creatives(), loader_file_format="parquet")
    print(f"✓ Generated 80 ad creatives")