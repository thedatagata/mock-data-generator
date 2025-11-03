"""
Amplitude Event Stream Data Generator
Generates realistic user journeys with engagement-based return behavior
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
import uuid
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from shared_config import *

from faker import Faker

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

print("Loading lead data...")
byd_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_bookyourdata/bookyourdata/1762095548.474671.701ad8b601.parquet')
uplead_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_upleads/uplead/1762095612.4264941.2fb362f699.parquet')

leads_df = pd.concat([byd_df, uplead_df], ignore_index=True)
print(f"Loaded {len(leads_df)} leads for identified user pool")

# ==========================================
# ENGAGEMENT SCORING & RETURN BEHAVIOR
# ==========================================

ENGAGEMENT_WEIGHTS = {
    'page_view': 1,
    'button_click': 1,
    'search': 2,
    'pricing_page_view': 8,
    'features_page_view': 5,
    'add_to_cart': 12,
    'remove_from_cart': -2,
    'checkout_start': 15,
    'whitepaper_download': 12,
    'demo_requested': 20,
    'trial_started': 25,
    'contact_sales_clicked': 18,
    'pricing_form_submit': 15,
    'contact_form_submit': 10,
    'newsletter_subscribe': 5,
}

RETURN_PROBABILITY = {
    'bounce': {'return_rate': 0.08, 'days_until_return': (14, 60)},
    'low_engagement': {'return_rate': 0.18, 'days_until_return': (7, 30)},
    'medium_engagement': {'return_rate': 0.40, 'days_until_return': (3, 14)},
    'high_engagement': {'return_rate': 0.65, 'days_until_return': (1, 7)},
    'very_high_engagement': {'return_rate': 0.85, 'days_until_return': (1, 3)},
}

def calculate_engagement_score(session_events):
    """Calculate cumulative engagement score"""
    score = 0
    for event in session_events:
        weight = ENGAGEMENT_WEIGHTS.get(event.get('event_type'), 0)
        score += weight
    return max(0, score)

def get_engagement_tier(score):
    """Categorize engagement level"""
    if score <= 1:
        return 'bounce'
    elif score <= 4:
        return 'low_engagement'
    elif score <= 15:
        return 'medium_engagement'
    elif score <= 25:
        return 'high_engagement'
    else:
        return 'very_high_engagement'

# ==========================================
# EVENT TYPES
# ==========================================

BASE_EVENT_TYPES = [
    'page_view',
    'button_click',
    'search',
    'pricing_page_view',
    'features_page_view',
]

DEVICES = [
    {'type': 'Android', 'family': 'Samsung Galaxy', 'carrier': 'Verizon', 'os': 'Android', 'version': '13.0'},
    {'type': 'Android', 'family': 'Google Pixel', 'carrier': 'T-Mobile', 'os': 'Android', 'version': '14.0'},
    {'type': 'iOS', 'family': 'iPhone', 'carrier': 'AT&T', 'os': 'iOS', 'version': '17.2'},
    {'type': 'iOS', 'family': 'iPad', 'carrier': 'Verizon', 'os': 'iOS', 'version': '17.1'},
    {'type': 'Web', 'family': 'Chrome', 'carrier': None, 'os': 'Windows', 'version': '11'},
    {'type': 'Web', 'family': 'Firefox', 'carrier': None, 'os': 'macOS', 'version': '14.2'},
]

# ==========================================
# USER STATE TRACKING
# ==========================================

class UserState:
    """Track user state across sessions"""
    def __init__(self, device_id):
        self.device_id = device_id
        self.user_id = None
        self.session_count = 0
        self.total_engagement_score = 0
        self.is_identified = False
        self.form_type = None
        self.trial_path = None
        self.trial_start_date = None
        self.converted_to_paid = False
        self.product_sku = None
        self.last_session_date = None
        self.scheduled_return_date = None
        self.engagement_tier = 'bounce'
    
    def is_returning_user(self):
        return self.session_count > 1
    
    def days_since_last_session(self, current_date):
        if self.last_session_date:
            return (current_date - self.last_session_date).days
        return 0

# Global user registry
user_registry = {}

def get_or_create_user(device_id):
    """Get existing user or create new one"""
    if device_id not in user_registry:
        user_registry[device_id] = UserState(device_id)
    return user_registry[device_id]

# ==========================================
# EVENT GENERATION HELPERS
# ==========================================

def should_fill_form(user, engagement_score):
    """Determine if user fills out a form this session"""
    if user.is_identified:
        return random.random() < 0.10  # 10% chance of secondary form
    
    tier = get_engagement_tier(engagement_score)
    form_probability = {
        'bounce': 0.0,
        'low_engagement': 0.05,
        'medium_engagement': 0.20,
        'high_engagement': 0.45,
        'very_high_engagement': 0.70,
    }
    
    return random.random() < form_probability.get(tier, 0)

def generate_session_events(user, current_date, source_info, campaign):
    """Generate events for a single session"""
    events = []
    
    # Determine number of page views based on engagement tier
    if user.is_returning_user():
        # Returning users are more engaged
        page_views = random.randint(3, 12)
    else:
        # New users - varied engagement
        page_view_dist = [
            (1, 0.08),   # Bounce
            (3, 0.25),   # Low
            (7, 0.40),   # Medium
            (12, 0.20),  # High
            (15, 0.07),  # Very high
        ]
        page_views = random.choices([p[0] for p in page_view_dist], 
                                    weights=[p[1] for p in page_view_dist])[0]
    
    # Generate page view events
    for _ in range(page_views):
        event_type = random.choices(
            BASE_EVENT_TYPES,
            weights=[0.60, 0.15, 0.10, 0.10, 0.05]
        )[0]
        events.append({'event_type': event_type})
    
    # Calculate engagement score
    engagement_score = calculate_engagement_score(events)
    
    # Check if user fills out a form
    if should_fill_form(user, engagement_score):
        form_type = select_form_type()
        form_config = FORM_TYPES[form_type]
        
        # Identify the user
        if not user.is_identified:
            user.user_id = fake.email()
            user.is_identified = True
            user.form_type = form_type
            
            # If trial signup, determine path
            if form_type == 'trial_signup':
                user.trial_path = get_trial_path()
                user.trial_start_date = current_date
        
        # Add form submission event
        events.append({
            'event_type': form_config['event_name'],
            'is_form_fill': True,
            'form_type': form_type,
            'trial_path': user.trial_path if form_type == 'trial_signup' else None,
        })
        
        engagement_score = calculate_engagement_score(events)
    
    # Check for trial conversion
    if user.trial_start_date and not user.converted_to_paid:
        days_in_trial = (current_date - user.trial_start_date).days
        
        if 10 <= days_in_trial <= 16:
            path_config = TRIAL_CONVERSION_PATHS[user.trial_path]
            
            # Check if they convert
            if random.random() < (path_config['conversion_rate_to_paid'] / 7):  # Spread over 7 days
                product = select_product_tier(user.trial_path)
                user.converted_to_paid = True
                user.product_sku = product['sku']
                
                # Add conversion event
                events.append({
                    'event_type': 'trial_converted',
                    'is_conversion': True,
                    'trial_path': user.trial_path,
                    'product_sku': product['sku'],
                    'conversion_day': days_in_trial,
                })
            
            elif days_in_trial >= 14:
                events.append({
                    'event_type': 'trial_expired',
                    'trial_path': user.trial_path,
                })
    
    # Update user engagement
    tier = get_engagement_tier(engagement_score)
    user.total_engagement_score += engagement_score
    user.engagement_tier = tier
    
    return events, engagement_score

def schedule_return_session(user, current_date, engagement_score):
    """Determine if and when user will return"""
    tier = get_engagement_tier(engagement_score)
    config = RETURN_PROBABILITY[tier]
    
    if random.random() < config['return_rate']:
        days_until = random.randint(*config['days_until_return'])
        user.scheduled_return_date = current_date + timedelta(days=days_until)
        return True
    
    return False

def create_event_record(event_data, user, session_id, event_time, device, geo, source_info, campaign, event_id):
    """Create a complete event record"""
    event_type = event_data['event_type']
    
    # Build event properties
    event_properties = {
        'source': source_info['source'],
        'medium': source_info['medium'],
        'engagement_tier': user.engagement_tier,
    }
    
    # Add campaign if paid traffic
    if campaign:
        event_properties['campaign_name'] = campaign['name']
        event_properties['campaign_id'] = campaign['id']
        event_properties['campaign_type'] = campaign['type']
    
    # Add form-specific properties
    if event_data.get('is_form_fill'):
        event_properties['form_type'] = event_data['form_type']
        if event_data.get('trial_path'):
            event_properties['trial_path'] = event_data['trial_path']
    
    # Add conversion properties
    if event_data.get('is_conversion'):
        event_properties['product_sku'] = event_data['product_sku']
        event_properties['trial_path'] = event_data['trial_path']
        event_properties['conversion_day'] = event_data['conversion_day']
    
    # Build user properties
    user_properties = {
        'user_type': 'identified' if user.is_identified else 'anonymous',
        'engagement_tier': user.engagement_tier,
        'total_sessions': user.session_count,
        'is_returning': user.is_returning_user(),
    }
    
    if user.is_identified:
        user_properties['lifecycle_stage'] = 'Trial' if user.trial_start_date else 'Lead'
        if user.converted_to_paid:
            user_properties['lifecycle_stage'] = 'Customer'
    
    return {
        'server_received_time': (event_time + timedelta(milliseconds=random.randint(10, 500))).isoformat(),
        'event_time': event_time.isoformat(),
        'processed_time': (event_time + timedelta(milliseconds=random.randint(100, 2000))).isoformat(),
        'client_upload_time': (event_time + timedelta(milliseconds=random.randint(5, 100))).isoformat(),
        'client_event_time': event_time.isoformat(),
        'server_upload_time': (event_time + timedelta(milliseconds=random.randint(200, 1000))).isoformat(),
        
        'user_id': user.user_id,
        'device_id': user.device_id,
        'uuid': str(uuid.uuid4()),
        'amplitude_id': random.randint(100000000, 999999999),
        'session_id': session_id,
        'event_id': event_id,
        '$insert_id': str(uuid.uuid4()),
        
        'event_type': event_type,
        'app': random.randint(100000, 999999),
        'version_name': f"{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 20)}",
        'start_version': f"{random.randint(1, 5)}.{random.randint(0, 9)}.0",
        
        'platform': device['type'] if device['type'] != 'Web' else 'Web',
        'os_name': device['os'],
        'os_version': device['version'],
        'device_type': device['type'],
        'device_family': device['family'],
        'device_carrier': device['carrier'],
        
        'country': geo['country'],
        'region': fake.state(),
        'city': fake.city(),
        'dma': fake.city(),
        'location_lat': float(fake.latitude()),
        'location_lng': float(fake.longitude()),
        
        'ip_address': fake.ipv4(),
        'library': f"amplitude-{device['type'].lower()}/3.{random.randint(0, 9)}.{random.randint(0, 9)}",
        'language': fake.language_code(),
        
        'paying': user.converted_to_paid,
        
        'event_properties': event_properties,
        'user_properties': user_properties,
        'group_properties': {},
        'groups': {},
        'data': {},
        'amplitude_attribution_ids': None,
        'sample_rate': None,
        
        '_generated_at': datetime.now().isoformat(),
    }

# ==========================================
# MAIN EVENT STREAM GENERATOR
# ==========================================

@dlt.resource(write_disposition="append", table_name="event_stream")
def event_stream():
    """Generate realistic event stream with engagement-based behavior"""
    
    event_id = 1
    users_to_schedule = []  # Track users who need to return
    
    for day in range(DAYS_OF_DATA):
        current_date = START_DATE + timedelta(days=day)
        daily_metrics = get_daily_metrics(day)
        
        # Calculate daily user counts
        new_users_today = int(daily_metrics['new_users'] * 0.9)  # 90% actually create sessions
        
        # Process returning users scheduled for today
        returning_users_today = [u for u in users_to_schedule 
                                if u.scheduled_return_date and u.scheduled_return_date.date() == current_date.date()]
        
        print(f"Day {day}: {new_users_today} new users, {len(returning_users_today)} returning users")
        
        # Generate sessions for new users
        for _ in range(new_users_today):
            # Create new user
            device_id = str(uuid.uuid4())
            user = get_or_create_user(device_id)
            user.session_count = 1
            user.last_session_date = current_date
            
            # Select traffic source
            source_info = random.choices(TRAFFIC_SOURCES, weights=[s['weight'] for s in TRAFFIC_SOURCES])[0]
            
            # Get campaign attribution (new users only get acquisition campaigns)
            campaign = get_campaign_for_traffic(source_info['source'], source_info['medium'], False)
            
            # Generate session
            session_id = int(current_date.timestamp() * 1000) + random.randint(0, 86400000)
            session_start = current_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))
            device = random.choice(DEVICES)
            geo = random.choices(GEO_DISTRIBUTION, weights=[g['weight'] for g in GEO_DISTRIBUTION])[0]
            
            events, engagement_score = generate_session_events(user, current_date, source_info, campaign)
            
            # Create event records
            for event_data in events:
                event_time = session_start + timedelta(seconds=random.randint(0, 1800))
                
                record = create_event_record(
                    event_data, user, session_id, event_time, device, geo, 
                    source_info, campaign, event_id
                )
                
                yield record
                event_id += 1
            
            # Schedule return session
            if schedule_return_session(user, current_date, engagement_score):
                users_to_schedule.append(user)
        
        # Generate sessions for returning users
        for user in returning_users_today:
            user.session_count += 1
            user.last_session_date = current_date
            user.scheduled_return_date = None  # Clear scheduled return
            
            # Select traffic source (may be different from first visit)
            source_info = random.choices(TRAFFIC_SOURCES, weights=[s['weight'] for s in TRAFFIC_SOURCES])[0]
            
            # Get campaign attribution (returning users can see re-engagement)
            campaign = get_campaign_for_traffic(source_info['source'], source_info['medium'], True)
            
            # Generate session
            session_id = int(current_date.timestamp() * 1000) + random.randint(0, 86400000)
            session_start = current_date + timedelta(hours=random.randint(6, 22), minutes=random.randint(0, 59))
            device = random.choice(DEVICES)
            geo = random.choices(GEO_DISTRIBUTION, weights=[g['weight'] for g in GEO_DISTRIBUTION])[0]
            
            events, engagement_score = generate_session_events(user, current_date, source_info, campaign)
            
            # Create event records
            for event_data in events:
                event_time = session_start + timedelta(seconds=random.randint(0, 1800))
                
                record = create_event_record(
                    event_data, user, session_id, event_time, device, geo, 
                    source_info, campaign, event_id
                )
                
                yield record
                event_id += 1
            
            # May schedule another return
            if schedule_return_session(user, current_date, engagement_score):
                if user not in users_to_schedule:
                    users_to_schedule.append(user)
        
        # Clean up users who won't return (keep list manageable)
        users_to_schedule = [u for u in users_to_schedule 
                            if u.scheduled_return_date and u.scheduled_return_date > current_date]


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="amplitude_events",
        destination="filesystem",
        dataset_name="amplitude"
    )
    
    load_info = pipeline.run(event_stream(), loader_file_format="parquet")
    
    print(f"\n✓ Event stream generated:")
    print(f"  - {DAYS_OF_DATA} days of data")
    print(f"  - Engagement-based return behavior")
    print(f"  - Multiple form types with proper distribution")
    print(f"  - Campaign attribution (re-engagement for returning users only)")
    print(f"  - Trial conversion tracking")
