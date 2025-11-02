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
DEPARTMENTS = ["Engineering & Technical", "Marketing", "Sales", "Finance", "Operations", "HR", "Consulting", "IT"]
JOB_FUNCTIONS = ["Engineering", "Product Management", "Marketing", "Sales", "Finance", "Operations", "Consulting", "IT"]
MANAGEMENT_LEVELS = ["VP", "Director", "Manager", "Individual Contributor", "C-Level", "Owner"]
EMAIL_STATUS = ["Deliverable", "Valid", "Risky", "Unknown"]
INDUSTRIES = ["Technology", "Healthcare", "Finance", "Retail", "Manufacturing", "Consulting", "Education"]
COMPANY_TYPES = ["PRIVATE", "PUBLIC", "NON-PROFIT", "PARTNERSHIP"]
GENDERS = ["MALE", "FEMALE", "NON-BINARY"]

def generate_bookyourdata_lead():
    """Generate a single BookYourData-style lead"""
    first_name = fake.first_name()
    last_name = fake.last_name()
    company_name = fake.company()
    domain = f"{company_name.lower().replace(' ', '').replace(',', '')}.com"
    
    return {
        "First Name": first_name,
        "Last Name": last_name,
        "Email": f"{first_name.lower()}.{last_name.lower()}@{domain}",
        "Email Status": random.choice(EMAIL_STATUS),
        "Contact Job Title": fake.job(),
        "Management Level": random.choice(MANAGEMENT_LEVELS),
        "Department": random.choice(DEPARTMENTS),
        "Job Function": random.choice(JOB_FUNCTIONS),
        "Contact City": fake.city(),
        "Contact State": fake.state(),
        "Contact Country": "United States",
        "Contact Continent": "North America",
        "Contact County": fake.city(),
        "Contact Metro Area": fake.city(),
        "Company": company_name,
        "Company Address": fake.street_address(),
        "Company City": fake.city(),
        "Company State": fake.state(),
        "Company Country": "United States",
        "Company Continent": "North America",
        "Company Postal Code": fake.zipcode(),
        "Company County": fake.city(),
        "Company Metro Area": fake.city(),
        "BYD_Industries": random.choice(INDUSTRIES),
        "Major Division Description": "Services",
        "SIC 2 Code": str(random.randint(10, 99)),
        "SIC 2 Code Description": fake.catch_phrase(),
        "SIC 4 Code": str(random.randint(1000, 9999)),
        "SIC 4 Code Description": fake.catch_phrase(),
        "Company Phone": fake.phone_number(),
        "Local Phone": fake.phone_number(),
        "Contact Direct Dial": fake.phone_number(),
        "Fax Number": fake.phone_number() if random.random() > 0.5 else "",
        "Company Domain": domain,
        "Employees": random.choice([3, 10, 50, 100, 500, 1000, 5000]),
        "Revenue": random.randint(100000, 10000000),
        "Type": random.choice(COMPANY_TYPES),
        "Founded Year": random.randint(1980, 2023),
        "Similar Companies": "",
        "Contact LinkedIn": f"https://www.linkedin.com/in/{first_name.lower()}{last_name.lower()}",
        "Contact Facebook": "",
        "Contact Twitter": "",
        "Fortune": "",
        "Technologies": "",
        "Categories of Technologies": random.choice(INDUSTRIES),
        "Gender": random.choice(GENDERS),
        "Contact Skills": ",".join(fake.words(nb=5)),
        "Contact Interests": random.choice(INDUSTRIES),
        "Company LinkedIn": f"https://www.linkedin.com/company/{company_name.lower().replace(' ', '-')}",
        "Company Facebook": "",
        "Company Twitter": "",
        "Company Instagram": "",
        "Company Youtube": "",
        "Company Tiktok": "",
        "SEO Title": "",
        "SEO Description": "",
        "Company Specialties": fake.catch_phrase(),
        "Last Raised At": "",
        "Latest Funding Type": "",
        "Latest Funding Amount $": "",
        "Total Funding Amount $": "",
        "Stock Symbol": "",
        "Bookyourdata Company Id": str(uuid.uuid4()),
        "Bookyourdata Contact Id": str(uuid.uuid4()),
    }

@dlt.resource(name="bookyourdata", write_disposition="append")
def generate_bookyourdata_leads(num_leads: int = 50000):
    """Generate BookYourData leads in batches"""
    batch_size = 10000
    for i in range(0, num_leads, batch_size):
        batch = [generate_bookyourdata_lead() for _ in range(min(batch_size, num_leads - i))]
        print(f"Generated {i + len(batch)}/{num_leads} BookYourData leads")
        yield batch

if __name__ == "__main__":
    # Create pipeline
    pipeline = dlt.pipeline(
        pipeline_name="bookyourdata_leads",
        destination="filesystem",
        dataset_name="mock_bookyourdata"
    )
    
    # Run pipeline
    print("Starting BookYourData lead generation...")
    load_info = pipeline.run(
        generate_bookyourdata_leads(50000),
        loader_file_format="parquet"
    )
    
    # Print load info
    print("\n" + "="*80)
    print("BOOKYOURDATA LOAD INFO")
    print("="*80)
    print(load_info)
    print("="*80 + "\n")
