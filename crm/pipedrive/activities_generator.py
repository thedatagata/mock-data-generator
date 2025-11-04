"""
Pipedrive Activities Generator
Generates CRM activities based on lead form type and trial path
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from shared_config import *

from faker import Faker

fake = Faker()
Faker.seed(SEED)
random.seed(SEED)

SALES_REPS = [1, 2, 3, 4, 5]

# Activity subject templates by type
ACTIVITY_SUBJECTS = {
    'trial_started': "Trial Account Created - {company}",
    'initial_outreach': "Initial Outreach - {company}",
    'demo_scheduled': "Product Demo Scheduled - {company}",
    'demo_completed': "Product Demo Completed - {company}",
    'demo_requested': "Demo Request Received - {company}",
    'follow_up_email': "Follow-up Email - {company}",
    'follow_up_call': "Follow-up Call - {company}",
    'pricing_discussion': "Pricing Discussion - {company}",
    'pricing_inquiry_received': "Pricing Inquiry - {company}",
    'technical_questions': "Technical Q&A - {company}",
    'contract_sent': "Contract Sent - {company}",
    'content_downloaded': "Whitepaper Downloaded - {company}",
    'nurture_email_sent': "Nurture Email - {company}",
    'newsletter_subscribed': "Newsletter Signup - {company}",
    'contact_form_received': "Contact Form Submitted - {company}",
    'cold_email': "Cold Email Outreach - {company}",
    'cold_call': "Cold Call - {company}",
    'linkedin_outreach': "LinkedIn Connection - {company}",
}

# Activity type mapping (for Pipedrive's activity type field)
ACTIVITY_TYPE_MAPPING = {
    'trial_started': 'task',
    'initial_outreach': 'email',
    'demo_scheduled': 'meeting',
    'demo_completed': 'meeting',
    'demo_requested': 'task',
    'follow_up_email': 'email',
    'follow_up_call': 'call',
    'pricing_discussion': 'call',
    'pricing_inquiry_received': 'task',
    'technical_questions': 'call',
    'contract_sent': 'email',
    'content_downloaded': 'task',
    'nurture_email_sent': 'email',
    'newsletter_subscribed': 'task',
    'contact_form_received': 'task',
    'cold_email': 'email',
    'cold_call': 'call',
    'linkedin_outreach': 'task',
}

# Activity duration by type
ACTIVITY_DURATIONS = {
    'call': '00:15:00',
    'email': '00:05:00',
    'meeting': '00:30:00',
    'task': '00:10:00',
}


def get_activity_template_for_lead(lead):
    """Determine which activity template to use based on lead type"""
    form_type = lead.get('form_type', lead.get('_form_type'))
    trial_path = lead.get('trial_path')
    
    # Trial leads
    if form_type == 'trial_signup':
        if trial_path == 'self_service':
            return 'trial_self_service'
        else:
            return 'trial_sales_assisted'
    
    # Other form types
    elif form_type in CRM_ACTIVITY_TYPES:
        return form_type
    
    # Purchased leads
    elif form_type == 'purchased_lead' or lead.get('_source') == 'purchased_list':
        return 'purchased_lead'
    
    # Default
    return None


def generate_activities_for_lead(lead, activity_id_start):
    """Generate all activities for a single lead"""
    activities = []
    
    # Get activity template
    template_key = get_activity_template_for_lead(lead)
    if not template_key or template_key not in CRM_ACTIVITY_TYPES:
        return activities
    
    activity_template = CRM_ACTIVITY_TYPES[template_key]
    
    # Lead details
    lead_id = lead.get('id')
    company = lead.get('company', fake.company())
    owner_id = lead.get('owner_id', random.choice(SALES_REPS))
    person_id = lead.get('person_id', random.randint(1000, 999999))
    org_id = lead.get('organization_id', random.randint(1000, 999999))
    deal_id = None  # Will be set in deals generator
    
    # Parse lead add_time
    try:
        lead_add_time = datetime.fromisoformat(lead.get('add_time').replace('+00:00', '').replace('Z', ''))
    except:
        lead_add_time = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 30))
    
    # Generate activities based on template
    activity_id = activity_id_start
    
    for activity_config in activity_template:
        # Check probability
        if random.random() > activity_config['probability']:
            continue
        
        # Calculate due date
        days_offset = activity_config['day_offset']
        due_datetime = lead_add_time + timedelta(days=days_offset, hours=random.randint(9, 17))
        
        # Determine if activity is done (if due date is in the past)
        is_done = due_datetime < datetime.now()
        marked_time = due_datetime + timedelta(hours=random.randint(0, 4)) if is_done else None
        
        # Get activity type details
        activity_key = activity_config['type']
        activity_type = ACTIVITY_TYPE_MAPPING.get(activity_key, 'task')
        subject = ACTIVITY_SUBJECTS.get(activity_key, f"{activity_key} - {{company}}").format(company=company)
        duration = ACTIVITY_DURATIONS.get(activity_type, '00:10:00')
        
        activities.append({
            'id': activity_id,
            'subject': subject,
            'type': activity_type,
            'owner_id': owner_id,
            'creator_user_id': owner_id,
            'is_deleted': False,
            'add_time': lead_add_time.isoformat(),
            'update_time': (marked_time or due_datetime).isoformat(),
            
            'deal_id': deal_id,
            'lead_id': lead_id,
            'person_id': person_id,
            'org_id': org_id,
            'project_id': None,
            
            'due_date': due_datetime.strftime('%Y-%m-%d'),
            'due_time': due_datetime.strftime('%H:%M:%S'),
            'duration': duration,
            'busy': activity_type == 'meeting',
            
            'done': is_done,
            'marked_as_done_time': marked_time.isoformat() if marked_time else None,
            
            'location': 'Video Call' if activity_type == 'meeting' else None,
            'participants': [{'person_id': person_id, 'primary': True}],
            'attendees': [],
            
            'conference_meeting_client': 'zoom' if activity_type == 'meeting' and random.random() < 0.8 else None,
            'conference_meeting_url': f"https://zoom.us/j/{random.randint(100000000, 999999999)}" if activity_type == 'meeting' else None,
            'conference_meeting_id': None,
            
            'public_description': fake.sentence() if random.random() < 0.3 else None,
            'priority': 2 if lead.get('sales_priority') == 'high' else 1,
            'note': fake.paragraph() if random.random() < 0.4 else None,
            
            '_generated_at': datetime.now().isoformat(),
            '_activity_key': activity_key,
            '_form_type': lead.get('form_type'),
            '_trial_path': lead.get('trial_path'),
        })
        
        activity_id += 1
    
    return activities


@dlt.resource(write_disposition="append", table_name="activities")
def activities():
    """Generate CRM activities based on lead form types and trial paths"""
    
    print("\nGenerating activities from leads...")
    
    # In production, this would read from the leads table
    # For now, we'll generate a sample of leads to create activities
    
    activity_id = 1
    total_activities = 0
    
    # Calculate total leads for each type
    total_form_fills = sum(get_daily_metrics(d)['identified_leads'] for d in range(DAYS_OF_DATA))
    
    lead_counts = {
        'trial_signup': int(total_form_fills * 0.35),
        'demo_request': int(total_form_fills * 0.25),
        'pricing_inquiry': int(total_form_fills * 0.15),
        'contact_us': int(total_form_fills * 0.15),
        'whitepaper_download': int(total_form_fills * 0.07),
        'newsletter_signup': int(total_form_fills * 0.03),
    }
    
    # Also add purchased leads
    print("Loading purchased lead data...")
    try:
        byd_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_bookyourdata/bookyourdata/1762095548.474671.701ad8b601.parquet')
        uplead_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_upleads/uplead/1762095612.4264941.2fb362f699.parquet')
        purchased_leads_count = len(byd_df) + len(uplead_df)
    except:
        purchased_leads_count = 10000  # Fallback
    
    print(f"\nGenerating activities for:")
    print(f"  - Purchased leads: {purchased_leads_count}")
    for form_type, count in lead_counts.items():
        print(f"  - {form_type}: {count}")
    
    # Generate activities for purchased leads
    for i in range(purchased_leads_count):
        mock_lead = {
            'id': f"purchased_{i}",
            'form_type': 'purchased_lead',
            'company': fake.company(),
            'owner_id': random.choice(SALES_REPS),
            'person_id': random.randint(1000, 999999),
            'organization_id': random.randint(1000, 999999),
            'add_time': (START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 30))).isoformat(),
            'sales_priority': 'low',
            '_source': 'purchased_list',
        }
        
        lead_activities = generate_activities_for_lead(mock_lead, activity_id)
        for activity in lead_activities:
            yield activity
            activity_id += 1
            total_activities += 1
    
    # Generate activities for each form type
    for form_type, count in lead_counts.items():
        for i in range(count):
            # Determine trial path if trial signup
            trial_path = None
            if form_type == 'trial_signup':
                trial_path = get_trial_path()
            
            mock_lead = {
                'id': f"{form_type}_{i}",
                'form_type': form_type,
                'trial_path': trial_path,
                'company': fake.company(),
                'owner_id': random.choice(SALES_REPS),
                'person_id': random.randint(1000, 999999),
                'organization_id': random.randint(1000, 999999),
                'add_time': (START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA - 1))).isoformat(),
                'sales_priority': FORM_TYPES[form_type].get('sales_priority', 'medium'),
            }
            
            lead_activities = generate_activities_for_lead(mock_lead, activity_id)
            for activity in lead_activities:
                yield activity
                activity_id += 1
                total_activities += 1
    
    print(f"\n✓ Total activities generated: {total_activities}")
    print(f"\nActivity breakdown:")
    print(f"  - Purchased leads (~2 activities each): ~{purchased_leads_count * 2}")
    print(f"  - Trial self-service (~1 activity): ~{int(lead_counts['trial_signup'] * 0.65)}")
    print(f"  - Trial sales-assisted (~12 activities): ~{int(lead_counts['trial_signup'] * 0.35 * 12)}")
    print(f"  - Demo requests (~8 activities): ~{lead_counts['demo_request'] * 8}")
    print(f"  - Pricing inquiries (~6 activities): ~{lead_counts['pricing_inquiry'] * 6}")
    print(f"  - Contact forms (~4 activities): ~{lead_counts['contact_us'] * 4}")
    print(f"  - Whitepaper downloads (~2 activities): ~{lead_counts['whitepaper_download'] * 2}")
    print(f"  - Newsletter signups (~1 activity): ~{lead_counts['newsletter_signup']}")


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="pipedrive_activities",
        destination="filesystem",
        dataset_name="pipedrive"
    )
    
    load_info = pipeline.run(activities(), loader_file_format="parquet")
    
    print(f"\n✓ Activities: generated based on form types and trial paths")