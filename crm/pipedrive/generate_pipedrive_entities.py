"""
Generate Pipedrive Persons, Organizations, and Activities
Completes the Amplitude → Stripe → Pipedrive data pipeline

ENHANCED: Journey-specific activity sequences based on user behavior
"""
import polars as pl
from datetime import datetime, timedelta
import random
import os
import sys
from faker import Faker

# Add amplitude directory to path for shared_config
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'digital_analytics', 'amplitude'))

fake = Faker()
Faker.seed(42)
random.seed(42)

print("="*80)
print("PIPEDRIVE ENTITIES GENERATOR: PERSONS, ORGANIZATIONS, ACTIVITIES")
print("="*80)

# Load existing data
print("\nLoading existing data...")
amplitude_output = os.path.join(os.path.dirname(__file__), '..', '..', 'digital_analytics', 'amplitude', 'output')
users = pl.read_parquet(os.path.join(amplitude_output, 'user_funnel_state.parquet'))
leads = pl.read_parquet(os.path.join(amplitude_output, 'pipedrive', 'leads.parquet'))
deals = pl.read_parquet(os.path.join(amplitude_output, 'pipedrive', 'deals.parquet'))

print(f"Loaded {len(users):,} users")
print(f"Loaded {len(leads):,} leads")
print(f"Loaded {len(deals):,} deals")

# Sales team (matching the existing script)
SALES_REPS = [
    {'id': 1, 'name': 'Sarah Johnson'},
    {'id': 2, 'name': 'Mike Chen'},
    {'id': 3, 'name': 'Emily Rodriguez'},
    {'id': 4, 'name': 'David Kim'},
    {'id': 5, 'name': 'Jessica Taylor'},
]

# Activity types
ACTIVITY_TYPES = [
    {'id': 1, 'name': 'Call', 'key_string': 'call', 'icon_key': 'call'},
    {'id': 2, 'name': 'Meeting', 'key_string': 'meeting', 'icon_key': 'meeting'},
    {'id': 3, 'name': 'Email', 'key_string': 'email', 'icon_key': 'email'},
    {'id': 4, 'name': 'Task', 'key_string': 'task', 'icon_key': 'task'},
    {'id': 5, 'name': 'Demo', 'key_string': 'demo', 'icon_key': 'presentation'},
    {'id': 6, 'name': 'Follow-up', 'key_string': 'followup', 'icon_key': 'clock'},
]

# Journey-specific activity sequences
# Each journey type has different sales cycle patterns

SALES_LED_SEQUENCE = [
    # Demo requesters - traditional enterprise sales cycle
    {'type_id': 3, 'type': 'Email', 'subject': 'Demo request received', 'duration': '00:05:00', 'note': 'Initial response to demo request, scheduling call', 'day_offset': (0, 1)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Discovery call', 'duration': '00:45:00', 'note': 'Understanding business requirements, pain points, and use cases', 'day_offset': (1, 3)},
    {'type_id': 5, 'type': 'Demo', 'subject': 'Product demonstration', 'duration': '01:00:00', 'note': 'Live product demo tailored to their specific use case', 'day_offset': (3, 7)},
    {'type_id': 3, 'type': 'Email', 'subject': 'Demo follow-up and resources', 'duration': '00:15:00', 'note': 'Sent recording, case studies, and ROI calculator', 'day_offset': (3, 8)},
    {'type_id': 2, 'type': 'Meeting', 'subject': 'Technical deep-dive with team', 'duration': '01:30:00', 'note': 'Engineering team review - integration requirements and architecture discussion', 'day_offset': (7, 14)},
    {'type_id': 3, 'type': 'Email', 'subject': 'Proposal and pricing sent', 'duration': '00:20:00', 'note': 'Custom proposal with tiered pricing options and implementation plan', 'day_offset': (10, 18)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Pricing discussion', 'duration': '00:45:00', 'note': 'Reviewed proposal, addressed budget questions, discussed contract terms', 'day_offset': (14, 22)},
    {'type_id': 2, 'type': 'Meeting', 'subject': 'Security and compliance review', 'duration': '01:00:00', 'note': 'Security team review - SOC2, GDPR compliance, data handling', 'day_offset': (18, 28)},
    {'type_id': 4, 'type': 'Task', 'subject': 'Contract sent for legal review', 'duration': '00:30:00', 'note': 'MSA and order form sent to legal team for approval', 'day_offset': (21, 30)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Final negotiation and close', 'duration': '00:30:00', 'note': 'Addressed final concerns, confirmed start date, closed deal!', 'day_offset': None},  # Won date
]

PRODUCT_LED_SEQUENCE = [
    # Self-service trial users - lightweight PLG motion
    {'type_id': 3, 'type': 'Email', 'subject': 'Welcome to your trial', 'duration': '00:05:00', 'note': 'Automated welcome email with onboarding resources', 'day_offset': (0, 1)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Trial check-in call', 'duration': '00:30:00', 'note': 'Quick check-in to see how trial is going and offer help', 'day_offset': (3, 7)},
    {'type_id': 3, 'type': 'Email', 'subject': 'Trial tips and best practices', 'duration': '00:10:00', 'note': 'Sent curated resources based on their usage patterns', 'day_offset': (5, 10)},
    {'type_id': 6, 'type': 'Follow-up', 'subject': 'Trial conversion discussion', 'duration': '00:20:00', 'note': 'Discussed paid plans, answered questions about features and pricing', 'day_offset': (10, 18)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Closing call', 'duration': '00:25:00', 'note': 'Confirmed plan selection, processed payment, welcomed as customer!', 'day_offset': None},  # Won date
]

ENTERPRISE_SEQUENCE = [
    # Large deals with complex buying committees
    {'type_id': 3, 'type': 'Email', 'subject': 'Enterprise inquiry response', 'duration': '00:10:00', 'note': 'Initial outreach to understand enterprise requirements', 'day_offset': (0, 2)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Executive discovery call', 'duration': '01:00:00', 'note': 'Discovery with VP/C-level - strategic initiatives and business objectives', 'day_offset': (2, 5)},
    {'type_id': 5, 'type': 'Demo', 'subject': 'Executive product overview', 'duration': '01:00:00', 'note': 'High-level demo focused on business outcomes and ROI', 'day_offset': (5, 10)},
    {'type_id': 2, 'type': 'Meeting', 'subject': 'Technical architecture review', 'duration': '02:00:00', 'note': 'Deep-dive with engineering - architecture, scalability, integrations', 'day_offset': (10, 17)},
    {'type_id': 5, 'type': 'Demo', 'subject': 'Department-specific demo', 'duration': '01:30:00', 'note': 'Customized demo for end-user teams and stakeholders', 'day_offset': (14, 21)},
    {'type_id': 3, 'type': 'Email', 'subject': 'Enterprise proposal delivered', 'duration': '00:30:00', 'note': 'Comprehensive proposal with custom pricing, SLAs, and implementation roadmap', 'day_offset': (17, 25)},
    {'type_id': 2, 'type': 'Meeting', 'subject': 'Security and compliance audit', 'duration': '02:00:00', 'note': 'InfoSec team review - penetration testing results, compliance certifications', 'day_offset': (21, 30)},
    {'type_id': 1, 'type': 'Call', 'subject': 'ROI analysis and business case', 'duration': '01:00:00', 'note': 'Presented financial model showing cost savings and productivity gains', 'day_offset': (25, 35)},
    {'type_id': 2, 'type': 'Meeting', 'subject': 'Proof of concept kickoff', 'duration': '01:30:00', 'note': 'Started 30-day POC with key use cases and success criteria', 'day_offset': (28, 40)},
    {'type_id': 1, 'type': 'Call', 'subject': 'POC results review', 'duration': '01:00:00', 'note': 'Reviewed POC outcomes, demonstrated value achieved', 'day_offset': (50, 65)},
    {'type_id': 2, 'type': 'Meeting', 'subject': 'Legal and procurement review', 'duration': '01:30:00', 'note': 'Contract negotiations with legal and procurement teams', 'day_offset': (55, 70)},
    {'type_id': 4, 'type': 'Task', 'subject': 'Final contract preparation', 'duration': '01:00:00', 'note': 'Prepared final MSA, DPA, and order forms with custom terms', 'day_offset': (60, 75)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Executive sign-off and close', 'duration': '00:45:00', 'note': 'Final approval from decision makers, contract signed, deal closed!', 'day_offset': None},  # Won date
]

HIGH_TOUCH_PLG_SEQUENCE = [
    # High engagement trial users who need more guidance
    {'type_id': 3, 'type': 'Email', 'subject': 'Trial welcome and onboarding', 'duration': '00:05:00', 'note': 'Personalized welcome with getting started guide', 'day_offset': (0, 1)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Onboarding kickoff call', 'duration': '00:45:00', 'note': 'Guided setup session, configured initial workspace', 'day_offset': (1, 3)},
    {'type_id': 3, 'type': 'Email', 'subject': 'Advanced features guide', 'duration': '00:10:00', 'note': 'Shared advanced tutorials based on their use case', 'day_offset': (4, 7)},
    {'type_id': 6, 'type': 'Follow-up', 'subject': 'Mid-trial check-in', 'duration': '00:30:00', 'note': 'Reviewed usage, identified blockers, provided optimization tips', 'day_offset': (7, 12)},
    {'type_id': 2, 'type': 'Meeting', 'subject': 'Success planning session', 'duration': '00:45:00', 'note': 'Mapped out their goals and created success plan for rollout', 'day_offset': (12, 18)},
    {'type_id': 3, 'type': 'Email', 'subject': 'Pricing options overview', 'duration': '00:15:00', 'note': 'Sent personalized pricing based on their expected usage', 'day_offset': (14, 20)},
    {'type_id': 1, 'type': 'Call', 'subject': 'Plan selection and conversion', 'duration': '00:30:00', 'note': 'Discussed options, answered billing questions, converted to paid plan!', 'day_offset': None},  # Won date
]

def determine_journey_type(user, deal_duration_days):
    """
    Determine which sales motion to use based on user characteristics
    Returns: (sequence, journey_name)
    """
    demo_requested = user.get('demo_requested_date') is not None
    trial_started = user.get('trial_started_date') is not None
    engagement = user.get('engagement_tier', 'low_engagement')
    total_sessions = user.get('total_sessions', 0)
    
    # Get deal value to determine enterprise vs SMB
    deal_value = user.get('arr', 0) if 'arr' in user else 0
    
    # Enterprise deals (high value or long sales cycle)
    if deal_value >= 10000 or deal_duration_days >= 60:
        return ENTERPRISE_SEQUENCE, 'enterprise'
    
    # Sales-led motion (demo requested)
    elif demo_requested:
        return SALES_LED_SEQUENCE, 'sales_led'
    
    # High-touch PLG (high engagement trial users)
    elif trial_started and engagement in ['high_engagement', 'very_high_engagement'] and total_sessions >= 5:
        return HIGH_TOUCH_PLG_SEQUENCE, 'high_touch_plg'
    
    # Low-touch PLG (self-service)
    elif trial_started:
        return PRODUCT_LED_SEQUENCE, 'product_led'
    
    # Default to product-led for others
    else:
        return PRODUCT_LED_SEQUENCE, 'product_led_default'

# Company size distribution
COMPANY_SIZES = [
    ('1-10', 0.40),
    ('11-50', 0.30),
    ('51-200', 0.15),
    ('201-500', 0.08),
    ('501-1000', 0.04),
    ('1000+', 0.03),
]

# Industry distribution
INDUSTRIES = [
    'Technology', 'Software', 'SaaS', 'E-commerce', 'Finance', 
    'Healthcare', 'Education', 'Marketing', 'Consulting', 'Manufacturing',
    'Retail', 'Real Estate', 'Media', 'Telecommunications', 'Transportation'
]

# ==========================================
# 1. GENERATE ORGANIZATIONS (from email domains)
# ==========================================

print("\n" + "="*80)
print("GENERATING ORGANIZATIONS")
print("="*80)

# Extract unique email domains from identified users
identified_users = users.filter(pl.col('email') != '')

# Parse email domains
users_with_domains = identified_users.with_columns([
    pl.col('email').str.split('@').list.get(1).alias('email_domain')
])

# Get unique domains with user counts
domain_stats = (users_with_domains
    .group_by('email_domain')
    .agg([
        pl.count().alias('user_count'),
        pl.col('device_id').first().alias('first_device_id'),  # For linking
        pl.col('acquisition_channel').first().alias('acquisition_channel'),
        pl.col('engagement_tier').first().alias('engagement_tier'),
    ])
    .sort('user_count', descending=True)
)

print(f"\nFound {len(domain_stats):,} unique organizations (email domains)")

organizations_list = []
org_id = 1
domain_to_org_id = {}  # Map domain → org_id

for domain_row in domain_stats.iter_rows(named=True):
    domain = domain_row['email_domain']
    
    # Generate realistic company name from domain
    domain_clean = domain.replace('.com', '').replace('.io', '').replace('.ai', '')
    domain_clean = domain_clean.replace('.', ' ').title()
    
    # Assign random industry and size
    industry = random.choice(INDUSTRIES)
    company_size = random.choices(
        [s[0] for s in COMPANY_SIZES],
        weights=[s[1] for s in COMPANY_SIZES]
    )[0]
    
    # Assign owner (sales rep)
    owner = random.choice(SALES_REPS)
    
    # Generate address
    address = fake.address().replace('\n', ', ')
    
    # Calculate value estimate based on company size and user count
    if company_size == '1000+':
        value_multiplier = 10
    elif company_size.startswith('501'):
        value_multiplier = 7
    elif company_size.startswith('201'):
        value_multiplier = 5
    elif company_size.startswith('51'):
        value_multiplier = 3
    elif company_size.startswith('11'):
        value_multiplier = 2
    else:
        value_multiplier = 1
    
    base_value = random.randint(5000, 20000)
    estimated_value = base_value * value_multiplier * domain_row['user_count']
    
    organizations_list.append({
        'id': org_id,
        'name': domain_clean,
        'owner_id': owner['id'],
        'visible_to': '3',  # 3 = Entire company
        'address': address,
        'label': random.choice(['Hot', 'Warm', 'Cold', None]),
        
        # Custom fields
        'company_domain': domain,
        'company_size': company_size,
        'industry': industry,
        'user_count': domain_row['user_count'],
        'estimated_value': estimated_value,
        'acquisition_channel': domain_row['acquisition_channel'],
        
        # Timestamps
        'add_time': datetime.now().isoformat(),
        'update_time': datetime.now().isoformat(),
        
        # Counts (will be calculated)
        'people_count': domain_row['user_count'],
        'open_deals_count': 0,  # Calculate later
        'closed_deals_count': 0,  # Calculate later
        'won_deals_count': 0,  # Calculate later
        'activities_count': 0,  # Calculate later
        
        '_generated_at': datetime.now().isoformat(),
        '_source': 'amplitude',
    })
    
    domain_to_org_id[domain] = org_id
    org_id += 1

organizations_df = pl.DataFrame(organizations_list)
pipedrive_output_dir = os.path.join(amplitude_output, 'pipedrive')
os.makedirs(pipedrive_output_dir, exist_ok=True)
organizations_df.write_parquet(os.path.join(pipedrive_output_dir, 'organizations.parquet'))
print(f"✅ Saved {len(organizations_list):,} organizations")

# ==========================================
# 2. GENERATE PERSONS (from identified users)
# ==========================================

print("\n" + "="*80)
print("GENERATING PERSONS")
print("="*80)

persons_list = []
person_id = 1
device_to_person_id = {}  # Map device_id → person_id

identified_users_with_domain = identified_users.with_columns([
    pl.col('email').str.split('@').list.get(1).alias('email_domain')
])

print(f"\nCreating {len(identified_users):,} persons...")

for user in identified_users_with_domain.iter_rows(named=True):
    # Get organization ID from domain
    org_id = domain_to_org_id.get(user['email_domain'])
    
    # Parse name from email
    email_local = user['email'].split('@')[0]
    name_parts = email_local.replace('.', ' ').replace('_', ' ').split()
    
    if len(name_parts) >= 2:
        first_name = name_parts[0].title()
        last_name = name_parts[-1].title()
    else:
        first_name = name_parts[0].title() if name_parts else 'User'
        last_name = ''
    
    # Assign owner (sales rep)
    owner = random.choice(SALES_REPS)
    
    # Generate phone
    phone = fake.phone_number()
    
    # Determine label based on engagement
    if user['engagement_tier'] in ['very_high_engagement', 'high_engagement']:
        label = 'Hot'
    elif user['engagement_tier'] == 'medium_engagement':
        label = 'Warm'
    else:
        label = 'Cold'
    
    persons_list.append({
        'id': person_id,
        'company_id': org_id,
        'owner_id': owner['id'],
        'org_id': org_id,
        'name': f"{first_name} {last_name}".strip(),
        'first_name': first_name,
        'last_name': last_name,
        'email': [{'value': user['email'], 'primary': True, 'label': 'work'}],
        'phone': [{'value': phone, 'primary': True, 'label': 'work'}],
        'visible_to': '3',  # Entire company
        'label': label,
        
        # Tracking fields (from Amplitude)
        'user_id': user['user_id'],
        'device_id': user['device_id'],
        'current_stage': user['current_stage'],
        'engagement_tier': user['engagement_tier'],
        'total_sessions': user['total_sessions'],
        'acquisition_channel': user['acquisition_channel'],
        'first_visit_date': user['first_visit_date'],
        'last_active_date': user['last_active_date'],
        
        # Timestamps
        'add_time': user['first_visit_date'],
        'update_time': user['last_active_date'],
        
        # Counts (will calculate)
        'open_deals_count': 0,
        'closed_deals_count': 0,
        'won_deals_count': 0,
        'activities_count': 0,
        
        '_generated_at': datetime.now().isoformat(),
        '_source': 'amplitude',
    })
    
    device_to_person_id[user['device_id']] = person_id
    person_id += 1

persons_df = pl.DataFrame(persons_list)
persons_df.write_parquet(os.path.join(pipedrive_output_dir, 'persons.parquet'))
print(f"✅ Saved {len(persons_list):,} persons")

# ==========================================
# 3. GENERATE ACTIVITIES (realistic sales timeline)
# ==========================================

print("\n" + "="*80)
print("GENERATING ACTIVITIES")
print("="*80)

activities_list = []
activity_id = 1

# Generate activities for each deal (won customers)
print(f"\nCreating activities for {len(deals):,} deals...")

# Track journey type distribution
journey_counts = {}

for deal in deals.iter_rows(named=True):
    device_id = deal['device_id']
    person_id = device_to_person_id.get(device_id)
    
    if not person_id:
        continue
    
    # Get org_id from person
    matching_person = [p for p in persons_list if p['device_id'] == device_id]
    if not matching_person:
        continue
    org_id = matching_person[0]['org_id']
    
    # Get user data for journey determination
    matching_user = users.filter(pl.col('device_id') == device_id)
    if matching_user.height == 0:
        continue
    user_data = matching_user.to_dicts()[0]
    
    # Add deal data to user context
    user_data['arr'] = deal.get('arr', 0)
    
    # Timeline: from add_time to won_time
    deal_start = datetime.fromisoformat(deal['add_time'])
    deal_won = datetime.fromisoformat(deal['won_time'])
    deal_duration_days = (deal_won - deal_start).days
    
    # Assign owner
    owner = random.choice(SALES_REPS)
    
    # Determine journey-specific sequence
    activity_sequence_template, journey_type = determine_journey_type(user_data, deal_duration_days)
    
    # Track journey distribution
    journey_counts[journey_type] = journey_counts.get(journey_type, 0) + 1
    
    # Generate activities from template
    activity_sequence = []
    
    for activity_template in activity_sequence_template:
        # Calculate due date
        if activity_template['day_offset'] is None:
            # Closing activity - use won date
            due_date = deal_won.date().isoformat()
        else:
            # Calculate offset from deal start
            min_offset, max_offset = activity_template['day_offset']
            # Ensure offset doesn't exceed deal duration
            max_offset = min(max_offset, deal_duration_days - 1) if deal_duration_days > 1 else 0
            min_offset = min(min_offset, max_offset)
            
            if min_offset <= max_offset:
                offset_days = random.randint(min_offset, max_offset)
                due_date = (deal_start + timedelta(days=offset_days)).date().isoformat()
            else:
                # Skip this activity if timing doesn't work
                continue
        
        activity_sequence.append({
            'type_id': activity_template['type_id'],
            'type_name': activity_template['type'],
            'subject': activity_template['subject'],
            'done': True,
            'due_date': due_date,
            'duration': activity_template['duration'],
            'note': activity_template['note'],
        })
    
    # Create activity records
    for activity_data in activity_sequence:
        activity_time = datetime.fromisoformat(activity_data['due_date'])
        
        activities_list.append({
            'id': activity_id,
            'company_id': deal['owner_id'],  # Company that owns the activity
            'type': activity_data['type_name'],
            'type_id': activity_data['type_id'],
            'subject': activity_data['subject'],
            'done': activity_data['done'],
            'due_date': activity_data['due_date'],
            'due_time': '14:00',  # Default to 2pm
            'duration': activity_data['duration'],
            'user_id': owner['id'],
            'owner_id': owner['id'],
            'deal_id': deal['id'],
            'person_id': person_id,
            'org_id': org_id,
            'note': activity_data['note'],
            'marked_as_done_time': activity_time.isoformat() if activity_data['done'] else None,
            
            # Timestamps
            'add_time': activity_time.isoformat(),
            'update_time': activity_time.isoformat(),
            
            # Tracking
            'device_id': device_id,
            'deal_title': deal['title'],
            'product_sku': deal['product_sku'],
            'journey_type': journey_type,  # Track which journey this came from
            
            '_generated_at': datetime.now().isoformat(),
            '_source': 'amplitude',
        })
        
        activity_id += 1
    
    # Add a few ongoing activities (not done) for active customers
    if random.random() < 0.3:  # 30% of customers have future activities
        future_date = datetime.now() + timedelta(days=random.randint(7, 30))
        
        # Future activity type depends on journey
        if journey_type == 'enterprise':
            future_subject = 'Quarterly business review with executive team'
            future_note = 'Scheduled QBR to review adoption metrics, ROI, and discuss expansion opportunities'
        elif journey_type in ['sales_led', 'high_touch_plg']:
            future_subject = 'Success check-in call'
            future_note = 'Quarterly check-in to discuss usage, satisfaction, and identify upsell opportunities'
        else:
            future_subject = 'Customer health check'
            future_note = 'Quick check-in to ensure customer is getting value and address any questions'
        
        activities_list.append({
            'id': activity_id,
            'company_id': deal['owner_id'],
            'type': 'Follow-up',
            'type_id': 6,
            'subject': future_subject,
            'done': False,
            'due_date': future_date.date().isoformat(),
            'due_time': '14:00',
            'duration': '01:00:00',
            'user_id': owner['id'],
            'owner_id': owner['id'],
            'deal_id': deal['id'],
            'person_id': person_id,
            'org_id': org_id,
            'note': future_note,
            'marked_as_done_time': None,
            'add_time': datetime.now().isoformat(),
            'update_time': datetime.now().isoformat(),
            'device_id': device_id,
            'deal_title': deal['title'],
            'product_sku': deal['product_sku'],
            'journey_type': journey_type,
            '_generated_at': datetime.now().isoformat(),
            '_source': 'amplitude',
        })
        activity_id += 1

activities_df = pl.DataFrame(activities_list)
activities_df.write_parquet(os.path.join(pipedrive_output_dir, 'activities.parquet'))
print(f"✅ Saved {len(activities_list):,} activities")

# Journey distribution
print("\n🎯 Journey Type Distribution:")
for journey_type, count in sorted(journey_counts.items(), key=lambda x: x[1], reverse=True):
    pct = count / len(deals) * 100
    print(f"   {journey_type}: {count:,} deals ({pct:.1f}%)")

# Activity type distribution
activity_type_dist = activities_df.group_by('type').agg(pl.count().alias('count')).sort('count', descending=True)
print("\n📋 Activity Type Distribution:")
print(activity_type_dist)

# Activities per journey type
activities_by_journey = activities_df.group_by('journey_type').agg([
    pl.count().alias('total_activities')
])
print("\n📊 Activities by Journey Type:")
for row in activities_by_journey.iter_rows(named=True):
    # Calculate average (since groupby doesn't give us deal count directly)
    avg_activities = journey_counts.get(row['journey_type'], 1)
    if avg_activities > 0:
        avg = row['total_activities'] / avg_activities
        print(f"   {row['journey_type']}: {row['total_activities']:,} activities ({avg:.1f} avg per deal)")
    else:
        print(f"   {row['journey_type']}: {row['total_activities']:,} activities")

# ==========================================
# 4. UPDATE COUNTS IN ORGANIZATIONS AND PERSONS
# ==========================================

print("\n" + "="*80)
print("UPDATING ENTITY COUNTS")
print("="*80)

# Count deals and activities per organization
org_deal_counts = {}
org_activity_counts = {}

for deal in deals.iter_rows(named=True):
    device_id = deal['device_id']
    matching_persons = [p for p in persons_list if p['device_id'] == device_id]
    if matching_persons:
        org_id = matching_persons[0]['org_id']
        if org_id not in org_deal_counts:
            org_deal_counts[org_id] = {'open': 0, 'closed': 0, 'won': 0}
        
        if deal['status'] == 'won':
            org_deal_counts[org_id]['closed'] += 1
            org_deal_counts[org_id]['won'] += 1

for activity in activities_list:
    org_id = activity.get('org_id')
    if org_id:
        org_activity_counts[org_id] = org_activity_counts.get(org_id, 0) + 1

# Update organizations
for org in organizations_list:
    org_id = org['id']
    counts = org_deal_counts.get(org_id, {'open': 0, 'closed': 0, 'won': 0})
    org['closed_deals_count'] = counts['closed']
    org['won_deals_count'] = counts['won']
    org['activities_count'] = org_activity_counts.get(org_id, 0)

# Count per person
person_activity_counts = {}
for activity in activities_list:
    person_id = activity.get('person_id')
    if person_id:
        person_activity_counts[person_id] = person_activity_counts.get(person_id, 0) + 1

for person in persons_list:
    person_id = person['id']
    device_id = person['device_id']
    
    # Count deals for this person
    person_deals = [d for d in deals.iter_rows(named=True) if d['device_id'] == device_id]
    if person_deals:
        person['won_deals_count'] = len([d for d in person_deals if d['status'] == 'won'])
        person['closed_deals_count'] = len(person_deals)
    
    person['activities_count'] = person_activity_counts.get(person_id, 0)

# Re-save with updated counts
organizations_df = pl.DataFrame(organizations_list)
organizations_df.write_parquet(os.path.join(output_dir, 'organizations.parquet'))

persons_df = pl.DataFrame(persons_list)
persons_df.write_parquet(os.path.join(output_dir, 'persons.parquet'))

print("✅ Updated organization and person counts")

# ==========================================
# SUMMARY
# ==========================================

print("\n" + "="*80)
print("SUMMARY")
print("="*80)

print(f"\n📊 Pipedrive Entities Generated:")
print(f"   Organizations: {len(organizations_list):,}")
print(f"   Persons: {len(persons_list):,}")
print(f"   Activities: {len(activities_list):,}")
print(f"   Leads: {len(leads):,} (existing)")
print(f"   Deals: {len(deals):,} (existing)")

print(f"\n🔗 Data Relationships:")
print(f"   Persons per Organization: {len(persons_list) / len(organizations_list):.1f} avg")
print(f"   Activities per Deal: {len(activities_list) / len(deals):.1f} avg")
print(f"   Activities per Person: {len(activities_list) / len(persons_list):.1f} avg")

total_activities_done = len([a for a in activities_list if a['done']])
total_activities_pending = len(activities_list) - total_activities_done
print(f"\n📋 Activity Status:")
print(f"   Completed: {total_activities_done:,}")
print(f"   Pending: {total_activities_pending:,}")

print("\n" + "="*80)
print("OUTPUT FILES")
print("="*80)
print(f"✅ {os.path.join(output_dir, 'organizations.parquet')}")
print(f"✅ {os.path.join(output_dir, 'persons.parquet')}")
print(f"✅ {os.path.join(output_dir, 'activities.parquet')}")
print(f"✅ {os.path.join(output_dir, 'leads.parquet')} (existing)")
print(f"✅ {os.path.join(output_dir, 'deals.parquet')} (existing)")

print("\n🎯 Complete Pipedrive Data Model:")
print("   Organizations → Persons → Deals → Activities (journey-specific)")
print("   All entities trace back to Amplitude device_id/user_id")
print("   Ready to load into Pipedrive CRM via API")
print("\n💡 Activity Journeys:")
print("   - Enterprise: 10-13 activities (POC, security review, multi-stakeholder)")
print("   - Sales-led: 8-10 activities (demo-driven, traditional B2B sales)")
print("   - High-touch PLG: 5-7 activities (guided trial, success planning)")
print("   - Product-led: 3-5 activities (lightweight, self-service)")
print("   Activities personalized based on: demo vs trial, engagement, deal value")
