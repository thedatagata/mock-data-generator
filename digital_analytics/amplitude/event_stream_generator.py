"""
Daily event generator with flow patterns and DuckDB summaries
"""
import dlt
import duckdb
import polars as pl
from datetime import datetime, timedelta
import random
import uuid
import os
from dataclasses import dataclass, asdict
from typing import List, Optional

from event_taxonomy import (
    SAAS_EVENT_TAXONOMY, 
    EVENT_FLOW_PATTERNS,
    EVENT_ENGAGEMENT_SCORES
)

ALL_EVENTS = []
for events in SAAS_EVENT_TAXONOMY.values():
    ALL_EVENTS.extend(events)

DB_PATH = "user_registry.duckdb"
START_DATE = datetime(2024, 1, 1)
DAYS_TO_GENERATE = 365

@dataclass
class UserState:
    device_id: str
    user_id: str
    email: str = ""
    is_identified: bool = False
    is_customer: bool = False
    session_count: int = 0
    lifecycle_stage: str = "awareness"
    last_event_type: str = ""
    
    @classmethod
    def new_anonymous(cls, current_date):
        device_id = str(uuid.uuid4())
        return cls(device_id=device_id, user_id=device_id)
    
    @classmethod
    def new_lead(cls, current_date):
        device_id = str(uuid.uuid4())
        user_uuid = str(uuid.uuid4())
        return cls(
            device_id=device_id,
            user_id=user_uuid,
            email=f"user_{device_id[:8]}@example.com",
            is_identified=True,
            lifecycle_stage="engaged"
        )

@dataclass
class DailySummary:
    device_id: str
    user_id: str
    activity_date: str
    email: str = ""
    is_identified: bool = False
    is_customer: bool = False
    events_today: int = 0
    engagement_score_today: int = 0
    lifecycle_stage: str = "awareness"
    return_probability: float = 0.5
    total_sessions: int = 0
    last_event_today: str = ""

@dlt.resource(name="daily_user_activity", write_disposition="append", primary_key=["device_id", "activity_date"])
def daily_summary_resource(summaries: List[DailySummary]):
    if not summaries:
        return
    yield [asdict(s) for s in summaries]

def get_next_event_from_flow(last_event: str) -> Optional[str]:
    if last_event in EVENT_FLOW_PATTERNS:
        possible = EVENT_FLOW_PATTERNS[last_event]
        if random.random() < 0.6:
            events = list(possible.keys())
            weights = list(possible.values())
            return random.choices(events, weights=weights)[0]
    return None

def update_lifecycle_stage(user: UserState, event_type: str):
    if event_type in ['payment_completed', 'subscription_created', 'trial_converted']:
        user.lifecycle_stage = "customer"
        user.is_customer = True
    elif event_type in ['subscription_cancelled', 'payment_failed']:
        user.lifecycle_stage = "churn_risk"
    elif event_type in ['trial_started', 'account_created']:
        user.lifecycle_stage = "trial"
        user.is_identified = True
    elif event_type in ['first_project_created', 'onboarding_completed']:
        user.lifecycle_stage = "self_service"
    elif event_type in ['pricing_page_view', 'demo_requested']:
        user.lifecycle_stage = "engaged"

def generate_event_sequence(user: UserState, current_date) -> List[dict]:
    """Generate events following flow patterns"""
    events = []
    num_events = random.randint(1, 10)
    session_id = int(current_date.timestamp() * 1000) + random.randint(0, 86400000)
    
    # Start event based on lifecycle stage
    if user.last_event_type and random.random() < 0.7:
        current_event = get_next_event_from_flow(user.last_event_type)
        if not current_event:
            current_event = random.choice(ALL_EVENTS)
    else:
        stage_events = {
            "awareness": SAAS_EVENT_TAXONOMY['awareness'],
            "engaged": SAAS_EVENT_TAXONOMY['interest'] + SAAS_EVENT_TAXONOMY['consideration'],
            "trial": SAAS_EVENT_TAXONOMY['trial_signup'] + SAAS_EVENT_TAXONOMY['activation'],
            "self_service": SAAS_EVENT_TAXONOMY['product_usage'],
            "customer": SAAS_EVENT_TAXONOMY['product_usage'] + SAAS_EVENT_TAXONOMY['retention'],
            "churn_risk": SAAS_EVENT_TAXONOMY['churn_risk']
        }
        current_event = random.choice(stage_events.get(user.lifecycle_stage, ALL_EVENTS))
    
    for i in range(num_events):
        event_time = current_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=i * 10
        )
        
        # Identify user on certain events
        if not user.is_identified and current_event in ['trial_started', 'account_created', 'demo_requested']:
            if random.random() < 0.7:
                user.user_id = str(uuid.uuid4())
                user.email = f"user_{user.device_id[:8]}@example.com"
                user.is_identified = True
        
        update_lifecycle_stage(user, current_event)
        
        events.append({
            'event_id': str(uuid.uuid1()),
            'event_time': event_time.isoformat(),
            'event_date': event_time.date().isoformat(),
            'event_type': current_event,
            'device_id': user.device_id,
            'user_id': user.user_id,
            'email': user.email,
            'is_identified': user.is_identified,
            'is_customer': user.is_customer,
            'lifecycle_stage': user.lifecycle_stage,
            'session_id': session_id,
        })
        
        user.last_event_type = current_event
        
        # Get next event from flow
        next_event = get_next_event_from_flow(current_event)
        if next_event:
            current_event = next_event
        else:
            current_event = random.choice(stage_events.get(user.lifecycle_stage, ALL_EVENTS))
    
    user.session_count += 1
    return events

def create_daily_summary(user: UserState, events: List[dict], current_date) -> DailySummary:
    """Create summary from events"""
    event_types = [e['event_type'] for e in events]
    engagement = sum(EVENT_ENGAGEMENT_SCORES.get(et, 1) for et in event_types)
    
    if engagement > 50:
        return_prob = random.uniform(0.7, 0.95)
    elif engagement > 20:
        return_prob = random.uniform(0.4, 0.7)
    else:
        return_prob = random.uniform(0.1, 0.4)
    
    return DailySummary(
        device_id=user.device_id,
        user_id=user.user_id,
        activity_date=current_date.date().isoformat(),
        email=user.email,
        is_identified=user.is_identified,
        is_customer=user.is_customer,
        events_today=len(events),
        engagement_score_today=engagement,
        lifecycle_stage=user.lifecycle_stage,
        return_probability=return_prob,
        total_sessions=user.session_count,
        last_event_today=event_types[-1] if event_types else ""
    )

def get_returning_users(target_date) -> List[UserState]:
    """Query returning users from DuckDB"""
    try:
        db = duckdb.connect(DB_PATH, read_only=True)
        threshold = random.uniform(0.3, 0.7)
        limit = random.randint(500, 1500)
        
        result = db.sql(f"""
            SELECT DISTINCT ON (device_id)
                device_id, user_id, email, is_identified, is_customer,
                lifecycle_stage, last_event_today, return_probability, total_sessions
            FROM daily_user_activity 
            WHERE return_probability > {threshold}
            AND activity_date >= '{(target_date - timedelta(days=7)).date()}'
            ORDER BY device_id, activity_date DESC
            LIMIT {limit}
        """).fetchdf()
        
        users = []
        for _, row in result.iterrows():
            users.append(UserState(
                device_id=row['device_id'],
                user_id=row['user_id'],
                email=row.get('email', ''),
                is_identified=row.get('is_identified', False),
                is_customer=row.get('is_customer', False),
                session_count=int(row.get('total_sessions', 1)),
                lifecycle_stage=row.get('lifecycle_stage', 'awareness'),
                last_event_type=row.get('last_event_today', '')
            ))
        
        db.close()
        return users
    except:
        return []

def generate_day(day_num: int):
    """Generate events for one day"""
    current_date = START_DATE + timedelta(days=day_num)
    month_num = day_num // 30
    
    print(f"Day {day_num}: {current_date.date()}")
    
    # Get users
    users = get_returning_users(current_date)
    
    if day_num == 0:
        users.extend([UserState.new_lead(current_date) for _ in range(2000)])
    
    users.extend([UserState.new_anonymous(current_date) for _ in range(random.randint(2000,4000))])
    
    # Select active users (target 1-3k events)
    target = random.randint(1000, 3000)
    active = random.sample(users, min(target // 3, len(users)))
    
    # Generate events
    events = []
    summaries = []
    
    for user in active:
        user_events = generate_event_sequence(user, current_date)
        events.extend(user_events)
        
        if user_events:
            summaries.append(create_daily_summary(user, user_events, current_date))
    
    print(f"  Events: {len(events)}, Summaries: {len(summaries)}")
    
    # Save summaries to DuckDB
    pipeline = dlt.pipeline(
        pipeline_name="event_stream",
        destination=dlt.destinations.duckdb(DB_PATH),
        dataset_name="events",
        dev_mode=False
    )
    pipeline.run(daily_summary_resource(summaries))
    
    # Save events to parquet
    event_count = len(events)
    if events:
        events_df = pl.DataFrame(events)
        month_folder = f"data/month_{month_num}"
        os.makedirs(month_folder, exist_ok=True)
        events_df.write_parquet(f"{month_folder}/day_{day_num:03d}.parquet")
        del events_df
    
    # CLEAR
    del users, active, events, summaries
    
    return event_count
    

if __name__ == '__main__':
    print("Generating events...")
    
    total = 0
    for day in range(DAYS_TO_GENERATE):
        total += generate_day(day)
        
        if day % 30 == 29:
            print(f"✓ Month {day // 30}")
