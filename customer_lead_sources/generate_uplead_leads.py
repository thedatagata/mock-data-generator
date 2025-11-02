import dlt
from faker import Faker
import uuid
import random
import logging

# Enable DLT logging
logging.basicConfig(level=logging.INFO)
dlt.config["runtime.log_level"] = "INFO"

fake = Faker()

# Reference data for realistic values
JOB_FUNCTIONS = ["engineering", "product", "marketing", "sales", "finance", "operations", "support", "partnerships"]
JOB_SUB_FUNCTIONS = ["engineering other", "product management", "digital marketing", "business development", 
                     "financial analysis", "customer success", "recruiting", "wellness"]
MANAGEMENT_LEVELS = ["VP", "D", "M", "C", "I"]
EMAIL_STATUS = ["Valid", "Deliverable", "Risky", "Unknown"]
REVENUE_RANGES = ["1M-10M", "10M-50M", "50M-100M", "100M-200M", "200M-1B", "1B+"]
EMPLOYEE_RANGES = ["1-10", "11-50", "51-200", "201-500", "501-1000", "1001-5000", "5001-10000", "10001+"]

def generate_uplead_lead():
    """Generate a single UpLead-style lead"""
    first_name = fake.first_name()
    last_name = fake.last_name()
    company_name = fake.company()
    company_clean = company_name.replace(",", "").replace(" Inc.", "").replace(" LLC", "")
    domain = f"{company_clean.lower().replace(' ', '')}.com"
    
    return {
        "CompanyName": company_name,
        "CompanyCleanName": company_clean,
        "CompanyAddress": fake.street_address(),
        "CompanyAddressLine2": f"Suite {random.randint(100, 999)}" if random.random() > 0.7 else "",
        "CompanyCity": fake.city(),
        "CompanyState": fake.state(),
        "CompanyCounty": fake.city(),
        "CompanyCountry": "US",
        "CompanyZip": fake.zipcode(),
        "ContactCity": fake.city(),
        "ContactState": fake.state(),
        "ContactCounty": fake.city(),
        "ContactCountry": "US",
        "ContactZip": fake.zipcode(),
        "Website": domain,
        "PhoneNumber": f"+1{random.randint(2000000000, 9999999999)}",
        "MobileDirectDial": f"+1{random.randint(2000000000, 9999999999)}",
        "FaxNumber": f"+1{random.randint(2000000000, 9999999999)}" if random.random() > 0.7 else "",
        "SICCode": f"{random.randint(1000, 9999)}",
        "6DigitSICCode": f"{random.randint(100000, 999999)}",
        "IndustryCodes": ",".join([str(random.randint(500000, 600000)) for _ in range(3)]),
        "IndustryTags": ",".join(fake.words(nb=5)),
        "Revenue": random.choice(REVENUE_RANGES),
        "NumberEmployees": random.choice(EMPLOYEE_RANGES),
        "YrFounded": random.randint(1950, 2023),
        "FirstName": first_name,
        "LastName": last_name,
        "Title": fake.job(),
        "Email": f"{first_name.lower()}.{last_name.lower()}@{domain}",
        "EmailStatus": random.choice(EMAIL_STATUS),
        "JobFunction": random.choice(JOB_FUNCTIONS),
        "JobSubFunction": random.choice(JOB_SUB_FUNCTIONS),
        "ManagementLevel": random.choice(MANAGEMENT_LEVELS),
        "ContactLinkedIn": f"https://www.linkedin.com/in/{first_name.lower()}{last_name.lower()}",
        "CompanyLinkedIn": f"https://www.linkedin.com/company/{company_clean.lower().replace(' ', '-')}",
        "CompanyFacebook": f"https://www.facebook.com/{company_clean.lower().replace(' ', '')}",
        "CompanyTwitter": f"https://www.twitter.com/{company_clean.lower().replace(' ', '')[:15]}",
        "CompanyInstagram": f"https://instagram.com/{company_clean.lower().replace(' ', '')[:15]}" if random.random() > 0.5 else "",
        "CompanyYelp": f"http://www.yelp.com/biz/{company_clean.lower().replace(' ', '-')}" if random.random() > 0.5 else "",
        "CompanyYouTube": f"https://youtube.com/{company_clean.lower().replace(' ', '')}" if random.random() > 0.5 else "",
        "CompanyCrunchbase": f"https://www.crunchbase.com/organization/{company_clean.lower().replace(' ', '-')}" if random.random() > 0.5 else "",
        "CompanyInternalId": str(uuid.uuid4()),
        "ContactInternalId": str(uuid.uuid4()),
        "LeadSource": "UpLead",
    }

@dlt.resource(name="uplead", write_disposition="append")
def generate_uplead_leads(num_leads: int = 50000):
    """Generate UpLead leads in batches"""
    batch_size = 10000
    for i in range(0, num_leads, batch_size):
        batch = [generate_uplead_lead() for _ in range(min(batch_size, num_leads - i))]
        print(f"Generated {i + len(batch)}/{num_leads} UpLead leads")
        yield batch

if __name__ == "__main__":
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="uplead_leads",
        destination="filesystem",
        dataset_name="mock_upleads"
    )
    
    # Run pipeline
    print("Starting UpLead lead generation...")
    load_info = pipeline.run(
        generate_uplead_leads(50000),
        loader_file_format="parquet"
    )
    
    # Print load info
    print("\n" + "="*80)
    print("UPLEAD LOAD INFO")
    print("="*80)
    print(load_info)
    print("="*80 + "\n")
