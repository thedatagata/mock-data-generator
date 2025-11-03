# Phase 3 Implementation: Amplitude Event Stream Generator

## Status: IN PROGRESS

### Key Changes Required

The event_stream_generator.py file needs a complete architectural rewrite to support:

1. **Engagement-based return behavior**
2. **Multiple form types with proper distribution**
3. **Campaign attribution based on returning user status**
4. **Product SKU tracking on purchases**
5. **Trial conversion funnel tracking**

### Core Architecture Changes

#### 1. User State Tracking
```python
class UserState:
    """Track user across multiple sessions"""
    - device_id (persistent cookie)
    - user_id (set on form fill)
    - session_count
    - total_engagement_score
    - is_identified (became identified via form)
    - form_type (which form they filled)
    - trial_path (self_service vs sales_assisted)
    - trial_start_date
    - converted_to_paid
    - product_sku
    - last_session_date
    - scheduled_return_date
```

#### 2. Event Generation Flow
```
For each day:
  For each active user:
    Session 1 (if new user):
      - Generate 1-15 page views
      - Calculate engagement score
      - Determine return probability
      - 35% chance of form fill (if high engagement)
      - If form fill:
         * Identify user (assign user_id)
         * Select form type (trial, demo, etc.)
         * If trial: assign trial path
      - Schedule return date if applicable
    
    Session 2+ (if returning user):
      - Get campaign attribution (may see re-engagement)
      - Continue journey based on previous state
      - If in trial period: may convert
      - If high intent: may fill form or purchase
```

#### 3. Event Types Distribution

**Base Events** (browsing):
- page_view (most common)
- button_click
- search
- pricing_page_view (high intent)
- features_page_view

**Form Events** (identification):
- trial_started (35% of forms)
- demo_requested (25% of forms)
- pricing_form_submit (15% of forms)
- contact_form_submit (15% of forms)
- whitepaper_download (7% of forms)
- newsletter_subscribe (3% of forms)

**Conversion Events**:
- purchase (includes product_sku)
- trial_converted
- trial_expired
- upgrade_button_clicked (self-service signal)
- contact_sales_clicked (sales-assisted trigger)

#### 4. Campaign Attribution Logic

```python
def get_campaign_attribution(user, source, medium):
    """
    Get appropriate campaign based on user state
    """
    if medium != 'cpc':
        return None  # Organic traffic, no campaign
    
    # NEW USERS: Can only see acquisition campaigns
    if not user.is_returning_user():
        campaigns = [c for c in PAID_CAMPAIGNS[f'{source}_cpc'] 
                    if c['targeting'] in ['new_users', 'all_users']]
    
    # RETURNING USERS: Can see all campaigns including re-engagement
    else:
        campaigns = PAID_CAMPAIGNS[f'{source}_cpc']
    
    return random.choices(campaigns, weights=[c['weight'] for c in campaigns])[0]
```

#### 5. Form Fill Logic

```python
def should_fill_form(user, engagement_score):
    """Determine if user fills out a form this session"""
    
    # Already identified? Can still fill additional forms
    if user.is_identified:
        return random.random() < 0.10  # 10% chance of secondary form
    
    # Not identified - varies by engagement
    tier = get_engagement_tier(engagement_score)
    form_probability = {
        'bounce': 0.0,
        'low_engagement': 0.05,
        'medium_engagement': 0.20,
        'high_engagement': 0.45,
        'very_high_engagement': 0.70,
    }
    
    return random.random() < form_probability[tier]

def generate_form_fill_event(user):
    """Generate a form submission event"""
    
    # Select form type
    form_type = select_form_type()
    form_config = FORM_TYPES[form_type]
    
    # Identify the user
    user.user_id = fake.email()
    user.is_identified = True
    user.form_type = form_type
    
    # If trial signup, determine path
    if form_type == 'trial_signup':
        user.trial_path = get_trial_path()
        user.trial_start_date = current_date
    
    return {
        'event_type': form_config['event_name'],
        'user_id': user.user_id,
        'event_properties': {
            'form_type': form_type,
            'trial_path': user.trial_path if form_type == 'trial_signup' else None,
        }
    }
```

#### 6. Trial Conversion Logic

```python
def check_trial_conversion(user, current_date):
    """Check if trial user converts to paid"""
    
    if not user.trial_start_date:
        return None
    
    days_in_trial = (current_date - user.trial_start_date).days
    
    # Only check conversion between days 10-16
    if days_in_trial < 10 or days_in_trial > 16:
        return None
    
    # Get conversion probability for this path
    path_config = TRIAL_CONVERSION_PATHS[user.trial_path]
    
    # Check if they convert
    if random.random() < path_config['conversion_rate_to_paid']:
        # Select product tier based on path
        product = select_product_tier(user.trial_path)
        user.converted_to_paid = True
        user.product_sku = product['sku']
        
        return {
            'event_type': 'trial_converted',
            'event_properties': {
                'trial_path': user.trial_path,
                'product_sku': product['sku'],
                'conversion_day': days_in_trial,
            }
        }
    
    # Trial expired without conversion
    elif days_in_trial >= 14:
        return {
            'event_type': 'trial_expired',
            'event_properties': {
                'trial_path': user.trial_path,
            }
        }
    
    return None
```

### Implementation Approach

Given the complexity, I recommend:

**Option A: Incremental Rewrite**
1. Keep current generator running
2. Create new generator file: `event_stream_generator_v2.py`
3. Implement new logic piece by piece
4. Test with small date range first
5. Compare outputs
6. Replace old generator when confident

**Option B: Full Rewrite**
1. Back up current generator
2. Completely rewrite with new architecture
3. Test thoroughly before running

### Testing Checklist

After implementation, validate:

- [ ] Bounce rate ~8% return
- [ ] High engagement ~65% return
- [ ] Re-engagement campaigns only for returning users
- [ ] Form fills create identified users
- [ ] Trial conversions at correct rates (28% self-service, 45% sales-assisted)
- [ ] Product SKUs present on purchases
- [ ] ~35% of forms are trial signups
- [ ] User journeys make logical sense (anonymous → identified → trial → paid)

### Next Steps

**Would you like me to:**
1. Create the full rewritten generator as a new file (_v2.py)?
2. Continue with incremental append operations?
3. Provide the complete code as a downloadable artifact?

The full rewrite is ~500-600 lines, so chunking into 25-line segments would take 20-24 operations. A new file or artifact might be more efficient.

### Estimated Data Volumes (After Implementation)

- **Total sessions**: ~27.4M
- **Form fills**: ~82K
  - Trial signups: ~29K
  - Identified users: ~82K
- **Returning users**: ~41% of users
- **Trial conversions**: ~10K paid subscriptions
- **Re-engagement campaign impressions**: Only to returning users (~11M eligible sessions)

