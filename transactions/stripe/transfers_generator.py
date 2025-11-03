"""
Stripe Transfers Generator
Platform fee transfers to connected accounts
"""
import dlt
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


@dlt.resource(write_disposition="append", table_name="transfers")
def transfers():
    """Generate platform transfers for marketplace transactions"""
    
    # ~5% of transactions involve partner/marketplace transfers
    total_transactions = sum(get_daily_metrics(d)['transactions'] for d in range(DAYS_OF_DATA))
    transfer_count = int(total_transactions * 0.05)
    
    for _ in range(transfer_count):
        transfer_date = START_DATE + timedelta(days=random.randint(0, DAYS_OF_DATA))
        amount = int(AVERAGE_TRANSACTION_VALUE * random.uniform(0.1, 0.3) * 100)  # 10-30% platform fee
        
        yield {
            'id': f"tr_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'object': 'transfer',
            'amount': amount,
            'amount_reversed': 0,
            'balance_transaction': f"txn_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'created': int(transfer_date.timestamp()),
            'currency': 'usd',
            'description': random.choice(['Platform fee', 'Partner payout', 'Referral commission', None]),
            'destination': f"acct_{fake.bothify(text='????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'destination_payment': f"py_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'livemode': False,
            'metadata': {},
            'reversals': {
                'object': 'list',
                'data': [],
                'has_more': False,
                'total_count': 0,
                'url': f"/v1/transfers/tr_{fake.bothify(text='??????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ')}/reversals",
            },
            'reversed': False,
            'source_transaction': f"ch_{fake.bothify(text='??????????????????', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789')}",
            'source_type': random.choice(['card', 'bank_account']),
            'transfer_group': f"ORDER_{random.randint(1000, 99999)}",
            '_generated_at': datetime.now().isoformat(),
        }


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="stripe_transfers",
        destination="filesystem",
        dataset_name="stripe"
    )
    
    load_info = pipeline.run(transfers(), loader_file_format="parquet")
    
    print(f"\n✓ Stripe transfers: ~{int(sum(get_daily_metrics(d)['transactions'] for d in range(DAYS_OF_DATA)) * 0.05)} platform payouts")
