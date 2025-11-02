"""
Google Ads Campaigns Data Generator
Generates campaign-level data following Google Ads campaign schema
"""
import dlt
import pandas as pd
from datetime import datetime, timedelta
import random


# Set seed for reproducibility
random.seed(54321)

# Load customer and lead data
print("Loading customer and lead data...")
customer_df = pd.read_parquet('gs://mock-source-data/paid_advertising/google_ads/customer/1762099054.8781552.0ea9195f72.parquet')
byd_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_bookyourdata/bookyourdata/1762095548.474671.701ad8b601.parquet')
uplead_df = pd.read_parquet('gs://mock-source-data/customer_data_population/mock_upleads/uplead/1762095612.4264941.2fb362f699.parquet')
leads_df = pd.concat([byd_df, uplead_df], ignore_index=True)

# Extract customer_id
customer_id = int(customer_df['customer_id'].iloc[0])

# Analyze lead data for campaign targeting
top_departments = leads_df['department'].value_counts().head(8).index.tolist() if 'department' in leads_df.columns else []
top_industries = leads_df['byd_industries'].value_counts().head(8).index.tolist() if 'byd_industries' in leads_df.columns else []

print(f"Customer ID: {customer_id}")
print(f"Lead count: {len(leads_df)}")
print(f"Top departments: {', '.join(top_departments[:3])}")


@dlt.resource(write_disposition="replace")
def campaigns():
    """Generate campaign records based on lead demographics"""
    
    campaign_id = 1001
    start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
    end_date = '2037-12-30'  # Google Ads indefinite end date
    
    # Create campaigns targeting key departments/industries
    campaign_types = ['SEARCH', 'DISPLAY', 'SEARCH', 'DISPLAY', 'SEARCH', 'DISPLAY', 'SEARCH', 'SEARCH']
    
    for i, dept in enumerate(top_departments[:8]):
        industry = top_industries[i % len(top_industries)] if top_industries else 'Technology'
        campaign_type = campaign_types[i]
        
        yield {
            # Identifiers
            'customer_id': customer_id,
            'campaign_id': campaign_id,
            'campaign_name': f"{dept} Solutions - {industry[:30]}",
            'resource_name': f"customers/{customer_id}/campaigns/{campaign_id}",
            
            # Status
            'campaign_status': 'ENABLED' if i < 7 else 'PAUSED',
            'serving_status': 'SERVING' if i < 7 else 'SUSPENDED',
            'ad_serving_optimization_status': 'OPTIMIZE',
            
            # Campaign type
            'advertising_channel_type': campaign_type,
            'advertising_channel_sub_type': 'UNSPECIFIED',
            
            # Budget
            'campaign_budget': f"customers/{customer_id}/campaignBudgets/{campaign_id}",
            
            # Bidding
            'bidding_strategy_type': 'TARGET_CPA',
            'target_cpa_target_cpa_micros': int(random.uniform(65.00, 100.00) * 1_000_000),
            'target_cpa_cpc_bid_ceiling_micros': int(random.uniform(15.00, 25.00) * 1_000_000),
            'target_cpa_cpc_bid_floor_micros': int(random.uniform(2.00, 5.00) * 1_000_000),
            
            # Dates
            'start_date': start_date,
            'end_date': end_date,
            
            # Network settings
            'network_settings_target_google_search': True,
            'network_settings_target_search_network': True if campaign_type == 'SEARCH' else False,
            'network_settings_target_content_network': True if campaign_type == 'DISPLAY' else False,
            'network_settings_target_partner_search_network': False,
            
            # Tracking
            'tracking_url_template': customer_df['tracking_url_template'].iloc[0] if 'tracking_url_template' in customer_df.columns else None,
            'final_url_suffix': customer_df['final_url_suffix'].iloc[0] if 'final_url_suffix' in customer_df.columns else None,
            
            # Performance
            'optimization_score': round(random.uniform(0.70, 0.95), 3),
            
            # Experiment
            'experiment_type': 'BASE',
            'base_campaign': f"customers/{customer_id}/campaigns/{campaign_id}",
            
            # Metadata
            '_target_department': dept,
            '_target_industry': industry,
            '_generated_at': datetime.now().isoformat()
        }
        
        campaign_id += 1


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="google_ads_campaigns",
        destination="filesystem",
        dataset_name="google_ads"
    )
    
    load_info = pipeline.run(
        campaigns(),
        loader_file_format="parquet"
    )
    
    print("\n✓ Campaigns data generated")
    print(f"Dataset: {load_info.dataset_name}")
    print(f"Loads: {len(load_info.loads_ids)}")
