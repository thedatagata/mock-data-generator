# Data Pipeline Refactor Implementation Plan

## ✅ Completed Changes

### 1. Foundational Configuration (shared_config.py)
**File**: `data_swamp/digital_analytics/shared_config.py`

**New Features Added**:
- ✅ Stripe Product Catalog with SKUs (STARTER, PRO, BUSINESS, ENTERPRISE)
- ✅ Campaign attribution logic with proper targeting
- ✅ UTM-friendly campaign names (acq-trial-signup, reeng-lapsed-users, etc.)
- ✅ Form types (trial_signup, demo_request, pricing_inquiry, contact_us, whitepaper_download, newsletter_signup)
- ✅ Trial conversion paths (self_service vs sales_assisted)
- ✅ CRM activity types by lifecycle stage
- ✅ Helper functions: `get_campaign_for_traffic()`, `select_product_tier()`, `select_form_type()`, `get_trial_path()`

**Key Configurations**:
```python
STRIPE_PRODUCTS = [
    {'id': 'prod_starter', 'sku': 'STARTER', 'price_monthly': 2900, ...},
    # ... 4 tiers total
]

PAID_CAMPAIGNS = {
    'google_cpc': [
        {'id': 1001, 'name': 'acq-trial-signup', 'targeting': 'new_users', ...},
        {'id': 1008, 'name': 'reeng-lapsed-users', 'targeting': 'returning_users', ...},
    ],
    'facebook_cpc': [...],
}

TRIAL_CONVERSION_PATHS = {
    'self_service': {
        'weight': 0.65,
        'crm_activities': 1,
        'conversion_rate_to_paid': 0.28,
        'tier_distribution': {'starter': 0.50, 'professional': 0.35, ...}
    },
    'sales_assisted': {
        'weight': 0.35,
        'crm_activities': 12,
        'conversion_rate_to_paid': 0.45,
        'tier_distribution': {'starter': 0.15, 'professional': 0.40, ...}
    }
}
```

### 2. Campaign Attribution Updates

**Files Updated**:
- ✅ `paid_ad_sources/google_ads/google_ads_campaigns.py`
- ✅ `paid_ad_sources/facebook_ads/campaigns_generator.py`

**Changes**:
- Campaign names now UTM-friendly (lowercase with hyphens)
- Campaigns include metadata: `_campaign_type` and `_targeting`
- Re-engagement campaigns explicitly marked for returning_users only
- Campaign IDs consistent with shared_config

**Example Campaign**:
```python
{
    'campaign_id': 1008,
    'campaign_name': 'reeng-lapsed-users',
    '_campaign_type': 'REENGAGEMENT',
    '_targeting': 'returning_users',  # CRITICAL: Only returning users
}
```

---

## 🚧 Still To Implement

### Phase 3: Amplitude Event Stream Updates
**File**: `data_swamp/digital_analytics/amplitude/event_stream_generator.py`

**Changes Needed**:
1. Add new form event types to EVENT_TYPES:
   ```python
   EVENT_TYPES = [
       'page_view', 'button_click', 'search',
       'add_to_cart', 'remove_from_cart', 
       'checkout_start', 'purchase',
       
       # NEW: Form events
       'trial_started',
       'demo_requested',
       'contact_form_submit',
       'pricing_form_submit',
       'content_download',
       'newsletter_subscribe',
       
       # NEW: Trial conversion events
       'upgrade_button_clicked',  # Self-service conversion
       'contact_sales_clicked',   # Triggers sales-assisted path
       'trial_converted',          # Successful conversion
       'trial_expired',            # Trial ended without conversion
       
       'login', 'logout',
   ]
   ```

2. Use `get_campaign_for_traffic()` to assign campaigns:
   ```python
   # OLD (wrong):
   source = random.choice(TRAFFIC_SOURCES)
   
   # NEW (correct):
   source = random.choices(TRAFFIC_SOURCES, weights=[s['weight'] for s in TRAFFIC_SOURCES])[0]
   is_returning_user = # ... determine based on user history
   campaign = get_campaign_for_traffic(source['source'], source['medium'], is_returning_user)
   ```

3. Add product SKU to purchase events:
   ```python
   if event_type == 'purchase':
       product = select_product_tier(trial_path)
       event_properties['product_sku'] = product['sku']
       event_properties['product_name'] = product['name']
       event_properties['amount'] = product['price_monthly'] / 100  # Convert cents to dollars
   ```

4. Add form submissions with proper distribution:
   ```python
   # When user fills out form:
   form_type = select_form_type()  # Uses distribution weights
   event_type = FORM_TYPES[form_type]['event_name']
   
   if form_type == 'trial_signup':
       trial_path = get_trial_path()
       event_properties['trial_path'] = trial_path
   ```

5. Track trial conversions:
   ```python
   # For trial users on days 10-14:
   if random.random() < conversion_rate:
       yield {
           'event_type': 'trial_converted',
           'event_properties': {
               'trial_path': trial_path,
               'product_sku': product['sku'],
               'conversion_day': day_since_trial_start,
           }
       }
   ```

### Phase 4: CRM Leads Generator Updates
**File**: `data_swamp/crm/pipedrive/leads_generator.py`

**Changes Needed**:
1. Create leads from ALL form fills (not just trials):
   ```python
   # For each form submission event from amplitude:
   form_type = select_form_type()
   form_config = FORM_TYPES[form_type]
   
   # Determine trial path if applicable
   trial_path = get_trial_path() if form_type == 'trial_signup' else None
   
   yield {
       'id': uuid,
       'title': f"{name} - {company}",
       'form_type': form_type,
       'lifecycle_stage': form_config['lifecycle_stage'],
       'trial_path': trial_path,
       'trial_start_date': today if form_type == 'trial_signup' else None,
       'sales_priority': form_config.get('sales_priority', 'low'),
       'expected_activities': determine_activity_count(form_type, trial_path),
       # ...
   }
   ```

2. Differentiate lead sources:
   ```python
   LEAD_SOURCE_MAPPING = {
       'trial_signup': 'Trial Signup Form',
       'demo_request': 'Demo Request Form',
       'pricing_inquiry': 'Pricing Inquiry Form',
       'contact_us': 'Contact Us Form',
       'whitepaper_download': 'Content Download',
       'newsletter_signup': 'Newsletter Signup',
       'purchased_lead': 'Purchased List - BookYourData/UpLead',
   }
   ```

3. Scale lead volume appropriately:
   - Current: ~260K leads (purchased lists + 25% form fills)
   - Update: Still ~260K but distributed across all form types
   - Trial signups should be 35% of form fills
   - Demo requests should be 25% of form fills
   - etc.

### Phase 5: CRM Activities Generator
**File**: `data_swamp/crm/pipedrive/activities_generator.py`

**Changes Needed**:
1. Generate activities based on lead form_type and trial_path:
   ```python
   def generate_activities_for_lead(lead):
       form_type = lead['form_type']
       trial_path = lead.get('trial_path')
       
       if form_type == 'trial_signup':
           if trial_path == 'self_service':
               activity_template = CRM_ACTIVITY_TYPES['trial_self_service']
           else:
               activity_template = CRM_ACTIVITY_TYPES['trial_sales_assisted']
       else:
           activity_template = CRM_ACTIVITY_TYPES[form_type]
       
       lead_created_date = lead['add_time']
       
       for activity in activity_template:
           if random.random() < activity['probability']:
               yield {
                   'type': activity['type'],
                   'subject': generate_subject(activity['type']),
                   'due_date': lead_created_date + timedelta(days=activity['day_offset']),
                   'lead_id': lead['id'],
                   # ...
               }
   ```

2. Activity volume should reflect:
   - Self-service trials: 1 activity (trial_started)
   - Sales-assisted trials: 10-12 activities
   - Demo requests: 8 activities
   - Purchased leads: 1-2 activities (cold outreach)
   - Newsletter signups: 1 activity

### Phase 6: CRM Deals Generator Updates
**File**: `data_swamp/crm/pipedrive/deals_generator.py`

**Changes Needed**:
1. Link deals to product SKUs:
   ```python
   product = select_product_tier(trial_path)
   
   yield {
       'title': f"{company} - {product['name']} Plan",
       'value': product['price_monthly'] / 100,  # Convert cents to dollars
       'product_sku': product['sku'],
       'product_name': product['name'],
       'trial_path': trial_path,
       'source_form_type': form_type,
       # ...
   }
   ```

2. Create deals at the right time:
   - Self-service trials: Deal created AFTER conversion (day 12 on average)
   - Sales-assisted trials: Deal created ON trial start (day 0)
   - Demo requests: Deal created after demo (day 3-4)

3. Different deal stages by path:
   ```python
   if trial_path == 'self_service':
       # Deal created post-conversion, immediately won
       stage_id = STAGES['closed_won']['id']
   elif trial_path == 'sales_assisted':
       # Deal progresses through stages
       stage_progression = [
           (0, 'qualification'),
           (5, 'needs_analysis'),
           (9, 'proposal'),
           (12, 'negotiation'),
           (14, 'closed_won' if converts else 'closed_lost'),
       ]
   ```

### Phase 7: Stripe Subscriptions Updates
**File**: `data_swamp/transactions/stripe/subscriptions_generator.py`

**Changes Needed**:
1. Link subscriptions to actual product IDs:
   ```python
   product = select_product_tier(trial_path)
   billing_interval = random.choice(['month', 'year'], weights=[0.85, 0.15])
   
   amount = product['price_annual'] if billing_interval == 'year' else product['price_monthly']
   
   yield {
       'items': {
           'data': [{
               'plan': {
                   'id': f"plan_{product['sku'].lower()}_{billing_interval}ly",
                   'product': product['id'],
                   'amount': amount,
                   'interval': billing_interval,
                   # ...
               }
           }]
       },
       '_product_sku': product['sku'],
       '_trial_path': trial_path,
       # ...
   }
   ```

2. Trial period tracking:
   ```python
   # All subscriptions start with trial
   trial_start = subscription_create_date
   trial_end = trial_start + timedelta(days=14)
   
   # Determine if trial converts
   path_config = TRIAL_CONVERSION_PATHS[trial_path]
   converts = random.random() < path_config['conversion_rate_to_paid']
   
   if converts:
       conversion_day = int(random.gauss(path_config['avg_conversion_day'], 2))
       paid_start = trial_start + timedelta(days=conversion_day)
       status = 'active'
   else:
       paid_start = None
       status = 'canceled'
       canceled_at = trial_end
   ```

3. Align subscription counts:
   - Total trials = ~35% of form fills (~32K trials from ~92K form fills)
   - Total paid = trials * weighted_conversion_rate
     - Self-service: 65% * 28% = 18.2% of trials
     - Sales-assisted: 35% * 45% = 15.75% of trials
     - Combined: ~34% of trials convert (~10.8K paid subscriptions)

---

## 📊 Expected Data Volumes (12 Months)

### Traffic & Events
- **Sessions**: ~27.4M (75K/day * 365 days)
- **Active Users**: ~18.3M (50K/day)
- **New Users**: ~2.9M (8K/day)

### Lead Generation
- **Purchased Leads**: ~10K (BookYourData + UpLead)
- **Form Fill Leads**: ~82K (25% of new users)
  - Trial Signups: ~29K (35%)
  - Demo Requests: ~21K (25%)
  - Pricing Inquiries: ~12K (15%)
  - Contact Us: ~12K (15%)
  - Whitepaper Downloads: ~6K (7%)
  - Newsletter Signups: ~2K (3%)
- **Total CRM Leads**: ~92K

### Trial Conversion
- **Total Trials**: ~29K (from trial_signup forms)
- **Self-Service Trials**: ~19K (65%)
- **Sales-Assisted Trials**: ~10K (35%)

### Paid Conversions
- **Self-Service Conversions**: ~5.3K (19K * 28%)
- **Sales-Assisted Conversions**: ~4.5K (10K * 45%)
- **Other Form Conversions**: ~2K (demo requests, pricing inquiries)
- **Anonymous Conversions**: ~1K (direct checkout)
- **Total Paid Customers**: ~12.8K

### Revenue
- **Self-Service Avg Deal**: $59/month (lower tiers)
- **Sales-Assisted Avg Deal**: $149/month (higher tiers)
- **Annual Recurring Revenue**: ~$1.1M
- **Total Contracts (12 months)**: ~$12.8M

### CRM Activities
- **Self-Service Trial Activities**: ~19K (1 per trial)
- **Sales-Assisted Trial Activities**: ~120K (12 per trial)
- **Demo Request Activities**: ~168K (8 per lead)
- **Other Form Activities**: ~40K
- **Purchased Lead Activities**: ~10K
- **Total CRM Activities**: ~357K

---

## 🎯 Implementation Order

1. ✅ **Phase 1**: Update shared_config.py (DONE)
2. ✅ **Phase 2**: Update campaign generators (DONE)
3. **Phase 3**: Update Amplitude event_stream_generator.py (NEXT)
4. **Phase 4**: Update CRM leads_generator.py
5. **Phase 5**: Create/update CRM activities_generator.py
6. **Phase 6**: Update CRM deals_generator.py
7. **Phase 7**: Update Stripe subscriptions_generator.py
8. **Phase 8**: Update Stripe products_generator.py (link to shared catalog)
9. **Phase 9**: Update Stripe plans_generator.py (link to shared catalog)

---

## 🔍 Key Validation Checks

After implementation, verify:

1. **Campaign Attribution**:
   - ✅ Re-engagement campaigns only appear for returning users
   - ✅ New users never see re-engagement campaigns
   - ✅ Campaign names are UTM-friendly

2. **Form → CRM Pipeline**:
   - [ ] Every form fill creates a CRM lead
   - [ ] Trial signups are 35% of form fills
   - [ ] Self-service trials have 1 CRM activity
   - [ ] Sales-assisted trials have 10-12 CRM activities

3. **Trial Conversion**:
   - [ ] 65% of trials are self-service
   - [ ] Self-service converts at 28%
   - [ ] Sales-assisted converts at 45%
   - [ ] Product tier distribution matches path (self-service → lower tiers)

4. **Stripe Subscriptions**:
   - [ ] All subscriptions link to product SKUs
   - [ ] Trial periods are 14 days
   - [ ] Conversion timing aligns (self-service: day 12, sales: day 14)
   - [ ] ~34% of trials result in paid subscriptions

5. **Revenue Attribution**:
   - [ ] Purchase events include product_sku
   - [ ] Deal values match Stripe subscription amounts
   - [ ] Self-service deals average lower than sales-assisted

---

## 📝 Notes for Next Session

**Current State**:
- Shared config is fully updated with all new structures
- Campaign generators updated with UTM-friendly names and targeting metadata
- Ready to implement Amplitude event stream changes

**Recommended Next Step**:
Start with Phase 3 (Amplitude event_stream_generator.py) because:
1. It's the source of truth for form fills
2. Other generators (CRM, Stripe) will pull from Amplitude events
3. It establishes the user journey (anonymous → form fill → trial → paid)

**Key Decision Points**:
- Should anonymous users be able to purchase without signing up for trial?
- Should we track returning users explicitly or infer from user_id presence?
- What percentage of users should be "returning" to qualify for re-engagement campaigns?
