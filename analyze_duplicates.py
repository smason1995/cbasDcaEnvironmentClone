import csv
from collections import Counter

def analyze_duplicates_detailed():
    """
    Detailed analysis of duplicates in both files to identify the exact source of discrepancy
    """
    
    # Analyze PROD file duplicates
    print("=== ANALYZING PROD FILE DUPLICATES ===")
    prod_ids = []
    prod_rows = []
    
    with open('xdcawk_2025_prod.csv', 'r') as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader):
            prod_id = row['xfdcawkFilename'] + '|' + row['xfdcawkFiscalyear']
            prod_id = prod_id.strip()
            prod_ids.append(prod_id)
            prod_rows.append((i+1, row, prod_id))
    
    prod_counter = Counter(prod_ids)
    prod_duplicates = {k: v for k, v in prod_counter.items() if v > 1}
    
    print(f"PROD file: {len(prod_ids)} total rows, {len(set(prod_ids))} unique IDs")
    print(f"PROD duplicates: {len(prod_duplicates)} duplicate IDs, {sum(prod_duplicates.values()) - len(prod_duplicates)} extra rows")
    
    # Show detailed duplicate info for PROD
    for duplicate_id, count in prod_duplicates.items():
        print(f"\nDuplicate PROD ID '{duplicate_id}' appears {count} times:")
        for row_num, row, row_id in prod_rows:
            if row_id == duplicate_id:
                print(f"  Row {row_num}: \"xfdcawkFilename\":\"{row['xfdcawkFilename']}\", \"xfdcawkFiscalyear\":\"{row['xfdcawkFiscalyear']}\"")

    # Analyze TEST file duplicates
    print("\n=== ANALYZING TEST FILE DUPLICATES ===")
    test_ids = []
    test_rows = []
    
    with open('xdcawk_2025_test.csv', 'r') as file:
        reader = csv.DictReader(file)
        for i, row in enumerate(reader):
            test_id = row['xfdcawkFilename'] + '|' + row['xfdcawkFiscalyear']
            test_id = test_id.strip()
            test_ids.append(test_id)
            test_rows.append((i+1, row, test_id))
    
    test_counter = Counter(test_ids)
    test_duplicates = {k: v for k, v in test_counter.items() if v > 1}
    
    print(f"TEST file: {len(test_ids)} total rows, {len(set(test_ids))} unique IDs")
    print(f"TEST duplicates: {len(test_duplicates)} duplicate IDs, {sum(test_duplicates.values()) - len(test_duplicates)} extra rows")
    
    # Show detailed duplicate info for TEST
    for duplicate_id, count in test_duplicates.items():
        print(f"\nDuplicate TEST ID '{duplicate_id}' appears {count} times:")
        for row_num, row, row_id in test_rows:
            if row_id == duplicate_id:
                print(f"  Row {row_num}: \"xfdcawkFilename\":\"{row['xfdcawkFilename']}\", \"xfdcawkFiscalyear\":\"{row['xfdcawkFiscalyear']}\"")

    # Calculate expected vs actual differences
    unique_prod = len(set(prod_ids))
    unique_test = len(set(test_ids))
    expected_diff = unique_prod - unique_test
    
    print(f"\n=== SUMMARY ===")
    print(f"Unique PROD IDs: {unique_prod}")
    print(f"Unique TEST IDs: {unique_test}")
    
    # Check for IDs that appear in both files but with different frequencies
    print(f"\n=== CHECKING FOR FREQUENCY MISMATCHES ===")
    common_ids = set(prod_ids) & set(test_ids)
    frequency_mismatches = []
    
    for common_id in common_ids:
        prod_count = prod_counter[common_id]
        test_count = test_counter[common_id]
        if prod_count != test_count:
            frequency_mismatches.append((common_id, prod_count, test_count))
    
    if frequency_mismatches:
        print(f"Found {len(frequency_mismatches)} IDs with different frequencies:")
        for id_val, p_count, t_count in frequency_mismatches:
            print(f"  ID '{id_val}': PROD={p_count}, TEST={t_count}, diff={p_count - t_count}")
    else:
        print("No frequency mismatches found.")

if __name__ == "__main__":
    analyze_duplicates_detailed()
