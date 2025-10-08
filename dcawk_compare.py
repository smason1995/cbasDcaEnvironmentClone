import csv

with open('xdcawk_2025_prod.csv', 'r') as prodFile, open('xdcawk_2025_test.csv', 'r') as testFile, open('xdcawk_2025_diff.csv', 'w', newline='') as diffFile:
    prodData = csv.DictReader(prodFile)
    testData = csv.DictReader(testFile)
    diffData = csv.DictWriter(diffFile, fieldnames=prodData.fieldnames)
    diffData.writeheader()

    # Create a set of test IDs for much faster lookup
    testIds = set()
    testCount = 0
    for i, testRow in enumerate(testData):
        testCount += 1
        testId = testRow['xfdcawkFilename'] + '|' + testRow['xfdcawkFiscalyear']

        testId = testId.strip()  # Remove whitespace
        
        # Check for duplicates in test data
        if testId in testIds:
            print(f"WARNING: Duplicate TEST ID found: \"xfdcawkFilename\":\"{testRow['xfdcawkFilename']}\", \"xfdcawkFiscalyear\":\"{testRow['xfdcawkFiscalyear']}\" at row {i+1}")
        else:
            testIds.add(testId)
    
    print(f"Test file: {testCount} rows, {len(testIds)} unique IDs")
    
    totalCount = 0
    diffCount = 0
    prodIds = set()  # Track prod IDs for duplicate detection

    for i, prodRow in enumerate(prodData):
        totalCount += 1
        prodId = prodRow['xfdcawkFilename'] + '|' + prodRow['xfdcawkFiscalyear']
        prodId = prodId.strip()  # Remove whitespace
        
        # Check for duplicates in prod data
        if prodId in prodIds:
            print(f"WARNING: Duplicate PROD ID found: \"xfdcawkFilename\":\"{prodRow['xfdcawkFilename']}\", \"xfdcawkFiscalyear\":\"{prodRow['xfdcawkFiscalyear']}\" at row {i+1}")
        else:
            prodIds.add(prodId)
        
        # Check if this prod ID exists in test (much faster with set lookup)
        if prodId not in testIds:
            print(f"Row {i+1}: PROD ID='{prodId}' (missing in test)")
            diffData.writerow(prodRow)
            diffCount += 1
    
    print(f"Prod file: {totalCount} rows, {len(prodIds)} unique IDs")
    print(f"Differences found: {diffCount} out of {totalCount} total rows")
    
    # Additional validation
    if len(prodIds) != totalCount:
        print(f"WARNING: Found {totalCount - len(prodIds)} duplicate IDs in PROD file")
    
    if len(testIds) != testCount:
        print(f"WARNING: Found {testCount - len(testIds)} duplicate IDs in TEST file")
    
    expected_diff = len(prodIds) - len(testIds)
    print(f"Expected differences based on unique IDs: {expected_diff}")
    
    if diffCount != expected_diff:
        print(f"MISMATCH: Found {diffCount} differences but expected {expected_diff}")
        print("This suggests there might be duplicate IDs or other data issues.")


print("Comparison complete!")