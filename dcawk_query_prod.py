#from college_records import college_records_list 
#import cbas_module
import requests
import csv
from collections import Counter
import datetime
import math
import os
import sys
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def load_api_config():
    """Load API configuration from JSON file"""
    config_file = os.path.join(os.path.dirname(__file__), 'api_config.json')
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {config_file}")
        print("Please create api_config.json with your API keys")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in configuration file: {e}")
        sys.exit(1)

def get_api_key():
    """Get production API key from config file, environment variable, or prompt"""
    # First try environment variable (backward compatibility)
    api_key = os.getenv('ELLUCIAN_API_KEY_PROD') or os.getenv('ELLUCIAN_API_KEY')
    
    if api_key:
        print(f"✅ Using production API key from environment variable")
        return api_key
    
    # Load from JSON config file
    config = load_api_config()
    api_key = config.get('prod_api_key')
    
    if api_key and api_key.strip():
        print(f"✅ Using production API key from config file")
        return api_key.strip()
    
    # Fallback to user prompt
    print(f"⚠️  No production API key found in config file or environment")
    api_key = input(f"Enter PRODUCTION Ellucian API Key: ").strip()
    if not api_key:
        print("❌ API key is required")
        sys.exit(1)
    
    # Optionally save to config file
    save_to_config = input("Save this key to config file? (y/n): ").lower().strip()
    if save_to_config == 'y':
        try:
            config['prod_api_key'] = api_key
            config_file = os.path.join(os.path.dirname(__file__), 'api_config.json')
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
            print(f"✅ Production API key saved to config file")
        except Exception as e:
            print(f"⚠️  Failed to save to config file: {e}")
    
    return api_key

def get_token(api_key):
    """Get bearer token with error handling"""
    url = f"https://integrate.elluciancloud.com/auth"
    headers = {'Authorization' : 'Basic ' + api_key, 'Content-Type' : 'text/plain'}
    
    try:
        response = requests.post(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to get authentication token: {e}")
        sys.exit(1)

def query_table(offset, bearer_token, session=None):
    """Query table with session reuse and error handling"""
    url = "https://integrate.elluciancloud.com/api/x-xfdcawk"
    querystring = {"limit": "1000", "offset": f"{str(offset*1000)}"}
    headers = {
        'content-type': 'application/json', 
        'Accept': 'application/json', 
        "Authorization": f"Bearer {bearer_token}"
    }
    
    # Use provided session or requests module
    requester = session if session else requests
    
    try:
        response = requester.get(url, headers=headers, params=querystring, timeout=60)
        response.raise_for_status()
        print(f"✅ Retrieved 1,000 records starting at {str(offset*1000)}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to retrieve records at offset {offset*1000}: {e}")
        return None

def query_count(bearer_token):
    """Query total count with error handling"""
    url = "https://integrate.elluciancloud.com/api/x-xfdcawk"
    headers = {
        'content-type': 'application/json', 
        'Accept': 'application/json', 
        "Authorization": f"Bearer {bearer_token}"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return int(response.headers['x-total-count'])
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to get record count: {e}")
        sys.exit(1)
    except (KeyError, ValueError) as e:
        print(f"❌ Invalid response format: {e}")
        sys.exit(1)

def safe_get_field(line, field_name, default_value=""):
    """Safely extract field from API response"""
    try:
        return line.get(field_name, default_value)
    except (KeyError, AttributeError):
        return default_value

def process_record(line):
    """Process a single record and return CSV row"""
    return [
        safe_get_field(line, 'xfdcawkAltbranch'),
        safe_get_field(line, 'xfdcawkBankacct'),
        safe_get_field(line, 'xfdcawkBankcity'),
        safe_get_field(line, 'xfdcawkBankname'),
        safe_get_field(line, 'xfdcawkBranch'),
        safe_get_field(line, 'xfdcawkCaprefund'),
        safe_get_field(line, 'xfdcawkCreatedon'),
        safe_get_field(line, 'xfdcawkCurrefund'),
        safe_get_field(line, 'xfdcawkDcasubmitted'),
        safe_get_field(line, 'xfdcawkDepaddoper'),
        safe_get_field(line, 'xfdcawkDepdate'),
        safe_get_field(line, 'xfdcawkDepno'),
        safe_get_field(line, 'xfdcawkErrormessage'),
        safe_get_field(line, 'xfdcawkErrorstatus'),
        safe_get_field(line, 'xfdcawkFilename'),
        safe_get_field(line, 'xfdcawkFiscalperiod'),
        safe_get_field(line, 'xfdcawkFiscalyear'),
        safe_get_field(line, 'xfdcawkFiscalyearendon'),
        safe_get_field(line, 'xfdcawkFiscalyearstarton'),
        safe_get_field(line, 'xfdcawkInstname'),
        safe_get_field(line, 'xfdcawkIsjvprocesseddate'),
        safe_get_field(line, 'xfdcawkIsprocessed'),
        safe_get_field(line, 'xfdcawkIsprocesseddate'),
        safe_get_field(line, 'xfdcawkJvnumber'),
        safe_get_field(line, 'xfdcawkKeyeddate'),
        safe_get_field(line, 'xfdcawkNspsubmitted'),
        safe_get_field(line, 'xfdcawkPyrlrefund'),
        safe_get_field(line, 'xfdcawkRecdate'),
        safe_get_field(line, 'xfdcawkTotaldep'),
        safe_get_field(line, 'xfdcawkTotalrev'),
        safe_get_field(line, 'id')
    ]

def fetch_batch(offset, bearer_token, session):
    """Fetch a single batch of records"""
    try:
        data = query_table(offset, bearer_token, session)
        if data is None:
            return None
        
        batch_rows = []
        for line in data:
            batch_rows.append(process_record(line))
        
        return batch_rows
    except Exception as e:
        print(f"❌ Error processing batch {offset}: {e}")
        return None

def main():
    """Main execution function with performance optimizations"""
    print(f"🚀 Starting PRODUCTION Data Query...")
    start_time = time.time()
    
    # Get production API key
    api_key = get_api_key()
    bearer_token = get_token(api_key)
    print("✅ Authentication successful")
    
    # Get total count
    total_count = query_count(bearer_token)
    offset = math.ceil(int(total_count) / 1000)
    print(f"📊 Total records: {total_count}")
    print(f"📦 Batches to fetch: {offset}")
    
    # Setup CSV file for production
    write_file = f"xdcawk_2025_prod.csv"
    csv_header = [
        "xfdcawkAltbranch", "xfdcawkBankacct", "xfdcawkBankcity", "xfdcawkBankname", 
        "xfdcawkBranch", "xfdcawkCaprefund", "xfdcawkCreatedon", "xfdcawkCurrefund", 
        "xfdcawkDcasubmitted", "xfdcawkDepaddoper", "xfdcawkDepdate", "xfdcawkDepno", 
        "xfdcawkErrormessage", "xfdcawkErrorstatus", "xfdcawkFilename", "xfdcawkFiscalperiod", 
        "xfdcawkFiscalyear", "xfdcawkFiscalyearendon", "xfdcawkFiscalyearstarton", 
        "xfdcawkInstname", "xfdcawkIsjvprocesseddate", "xfdcawkIsprocessed", 
        "xfdcawkIsprocesseddate", "xfdcawkJvnumber", "xfdcawkKeyeddate", "xfdcawkNspsubmitted", 
        "xfdcawkPyrlrefund", "xfdcawkRecdate", "xfdcawkTotaldep", "xfdcawkTotalrev", "id"
    ]
    
    with open(write_file, 'w', newline='') as f_write:
        csvwrite = csv.writer(f_write)
        csvwrite.writerow(csv_header)
        
        record_count = 0
        
        # Use session for connection pooling
        with requests.Session() as session:
            # Configure session for better performance
            session.headers.update({
                'content-type': 'application/json',
                'Accept': 'application/json',
                'Authorization': f'Bearer {bearer_token}'
            })
            
            # Option 1: Sequential processing (more reliable)
            if offset <= 10:  # Use sequential for smaller datasets
                print("📥 Using sequential processing...")
                for i in range(offset):
                    batch_rows = fetch_batch(i, bearer_token, session)
                    if batch_rows:
                        csvwrite.writerows(batch_rows)
                        record_count += len(batch_rows)
                        print(f"✅ Processed batch {i+1}/{offset} ({len(batch_rows)} records)")
                    else:
                        print(f"⚠️  Skipped batch {i+1} due to error")
            
            # Option 2: Parallel processing (faster for large datasets)
            else:
                print("🚀 Using parallel processing...")
                max_workers = min(5, offset)  # Limit concurrent requests
                
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    # Submit all batch requests
                    future_to_offset = {
                        executor.submit(fetch_batch, i, bearer_token, session): i 
                        for i in range(offset)
                    }
                    
                    # Process completed batches in order
                    batch_results = {}
                    for future in as_completed(future_to_offset):
                        batch_offset = future_to_offset[future]
                        try:
                            batch_rows = future.result()
                            if batch_rows:
                                batch_results[batch_offset] = batch_rows
                                print(f"✅ Fetched batch {batch_offset+1}/{offset} ({len(batch_rows)} records)")
                            else:
                                print(f"⚠️  Failed to fetch batch {batch_offset+1}")
                        except Exception as e:
                            print(f"❌ Error in batch {batch_offset+1}: {e}")
                    
                    # Write batches in order
                    for i in range(offset):
                        if i in batch_results:
                            csvwrite.writerows(batch_results[i])
                            record_count += len(batch_results[i])
    
    # Performance summary
    end_time = time.time()
    duration = end_time - start_time
    
    print(f"\n🎉 PRODUCTION data query completed!")
    print(f"📊 Total records processed: {record_count}")
    print(f"📁 Output file: {write_file}")
    print(f"⏱️  Total time: {duration:.2f} seconds")
    print(f"🚀 Average speed: {record_count/duration:.1f} records/second")

if __name__ == "__main__":
    main()

# Legacy code for backward compatibility (will be removed)
# Keeping the old variables for any scripts that might import them
april_count = 0
april_string = "2025-04"
june_string = "2025-06"
july_string = "2025-07"