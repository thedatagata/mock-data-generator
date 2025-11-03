# Amplitude Mock Data Generators

Mock data generators for Amplitude analytics matching their export schemas.

## Files

### 1. event_stream_generator.py
Generates raw event stream data matching Amplitude's export schema.

**Output:** `event_stream` table  
**Schema:** Full Amplitude event format with timestamps, user IDs, device info, location (via Faker), and event properties  
**Volume:** 30 days × 500 users × multiple sessions  

**Key fields:**
- All timestamps (server_received_time, event_time, processed_time, etc.)
- User/device identifiers (user_id, device_id, amplitude_id, session_id)
- Device properties (platform, os_version, device_family, carrier)
- Location data using Faker (country, region, city, lat/lng, IP address)
- Event and user properties

### 2. funnel_report_generator.py
Generates funnel conversion analysis data.

**Output:** `funnel_report` table  
**Funnels included:**
- Purchase Funnel: page_view → add_to_cart → checkout_start → purchase
- Sign Up Funnel: landing_page → form_view → form_submit → sign_up

**Metrics:**
- Step-by-step conversion rates
- Cumulative conversion with raw user counts
- Median and average transition times (in milliseconds)
- Daily time series for all metrics (30 days)
- Histogram distributions for transition times

### 3. retention_cohorts_generator.py
Generates user retention cohort data.

**Output:** `retention_cohorts` table  
**Dimensions:** 12 weeks × 5 acquisition channels  
**Channels:** Google Ads, Facebook Ads, Organic Search, Email, Direct

**Metrics:**
- Retention rates by week (with decay curve)
- Cohort size and retained user counts
- Churn metrics
- Engagement (events/session, session length, sessions per user)
- Platform distribution (iOS/Android/Web)

### 4. revenue_ltv_report_generator.py
Generates cohort-based lifetime value revenue data.

**Output:** `revenue_ltv_report` table  
**Format:** Cohort analysis with cumulative revenue tracking

**Structure:**
- 90 daily cohorts
- Revenue tracked for r1d, r2d, ... r90d
- Cohort metrics: count, paid users, total_amount
- Cumulative revenue growth with diminishing returns

## Usage

Run each generator independently:

```bash
python event_stream_generator.py
python funnel_report_generator.py
python retention_cohorts_generator.py
python revenue_ltv_report_generator.py
```

## Dependencies

- dlt (with filesystem destination)
- pandas
- faker
- Access to lead data parquet files in GCS

## Output

All generators use:
- **Destination:** filesystem
- **Dataset:** amplitude
- **Format:** parquet
- **Write disposition:** append

Data syncs to the configured filesystem destination (see `.dlt/config.toml`).
