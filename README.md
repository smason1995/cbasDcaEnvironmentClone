# DCA Data Migration - Complete Workflow Documentation

## Overview
The DCA Data Migration system is a comprehensive Python-based workflow for querying, comparing, and analyzing DCA (Deposit Credit Account) data from Ellucian's Ethos API. The system provides robust data extraction, comparison, and duplicate analysis capabilities with automated logging and user interaction.

## System Architecture

### Core Scripts
1. **`dcawk_query_prod.py`** - Production data extraction
2. **`dcawk_query_test.py`** - Test data extraction  
3. **`dcawk_compare.py`** - Data comparison and difference detection
4. **`analyze_duplicates.py`** - Detailed duplicate analysis
5. **`dca_workflow.py`** - Master workflow orchestrator
6. **`dcawk_create_test.py`** - Test data creation utility

### Workflow Orchestration
The `dca_workflow.py` script provides a complete automated workflow:
1. Executes production data query (`dcawk_query_prod.py`)
2. Executes test data query (`dcawk_query_test.py`)  
3. Runs data comparison (`dcawk_compare.py`)
4. Validates differences against expected counts
5. Provides interactive menu for additional analysis
6. Logs all operations and outputs to `dca_workflow.log`

## API Configuration

### Configuration File Structure
Create an `api_config.json` file in the same directory as the scripts:

```json
{
  "prod_api_key": "your_production_api_key_here",
  "test_api_key": "your_test_api_key_here"
}
```

### API Key Priority System
The scripts follow this priority order for API key resolution:

1. **Environment Variables** (highest priority)
   - `ELLUCIAN_API_KEY_PROD` for production scripts
   - `ELLUCIAN_API_KEY_TEST` for test scripts
   - `ELLUCIAN_API_KEY` (fallback for backward compatibility)

2. **JSON Configuration File**
   - `api_config.json` in the script directory
   - `prod_api_key` for production environment
   - `test_api_key` for test environment

3. **Interactive Prompt** (if no key found)
   - Prompts user to enter the required API key
   - Offers to save the key to the configuration file
   - Provides clear error messages if no key is provided

## Usage Instructions

### Quick Start - Complete Workflow
```bash
python dca_workflow.py
```
This runs the complete end-to-end workflow with logging and interactive options.

### Individual Script Usage

#### Production Data Query
```bash
python dcawk_query_prod.py
```
- Extracts data from production Ellucian API
- Uses only production API key (`prod_api_key`)
- Outputs: `xdcawk_2025_prod.csv`

#### Test Data Query
```bash
python dcawk_query_test.py
```
- Extracts data from test Ellucian API
- Uses only test API key (`test_api_key`)  
- Outputs: `xdcawk_2025_test.csv`

#### Data Comparison
```bash
python dcawk_compare.py
```
- Compares production and test CSV files
- Outputs: `dcawk_2025_diff.csv` (records only in production)
- Reports duplicate detection and statistics

#### Duplicate Analysis
```bash
python analyze_duplicates.py
```
- Detailed analysis of duplicates in both files
- Outputs: `dca_duplicates.txt` with comprehensive duplicate report
- Shows exact duplicate entries and row numbers

## Performance Features

### Optimized Data Extraction
- **Parallel Processing**: Multi-threaded batch fetching for large datasets
- **Session Reuse**: HTTP connection pooling for improved performance
- **Smart Processing**: Automatic selection between sequential/parallel based on dataset size
- **Error Handling**: Comprehensive timeout and retry logic
- **Progress Reporting**: Real-time status updates with emoji indicators

### Performance Metrics
Both query scripts provide detailed performance reporting:
- Total records processed
- Execution time
- Average processing speed (records/second)
- Batch processing statistics

## Data Processing Features

### Robust Field Extraction
- Safe field extraction with default values for missing fields
- Handles optional fields gracefully (e.g., `xfdcawkErrormessage`, `xfdcawkFiscalperiod`)
- Consistent data type handling across all records

### Duplicate Detection
- **Set-based Lookups**: Efficient O(1) duplicate detection
- **Combined Keys**: Uses `xfdcawkFilename|xfdcawkFiscalyear` for unique identification
- **Whitespace Handling**: Automatic trimming to prevent false duplicates
- **Detailed Reporting**: Shows exact duplicate entries with row numbers

### Data Comparison
- **Fast Comparison**: Set-based comparison for optimal performance
- **Difference Tracking**: Identifies records present in production but missing in test
- **Statistics**: Comprehensive counts and duplicate analysis
- **Validation**: Automatic verification against expected difference counts

## Output Files

### Generated Files
- **`xdcawk_2025_prod.csv`** - Production data export
- **`xdcawk_2025_test.csv`** - Test data export
- **`dcawk_2025_diff.csv`** - Records only in production (differences)
- **`dca_duplicates.txt`** - Detailed duplicate analysis report
- **`dca_workflow.log`** - Complete workflow execution log

### CSV Structure
All CSV files contain the following fields:
```
xfdcawkAltbranch, xfdcawkBankacct, xfdcawkBankcity, xfdcawkBankname,
xfdcawkBranch, xfdcawkCaprefund, xfdcawkCreatedon, xfdcawkCurrefund,
xfdcawkDcasubmitted, xfdcawkDepaddoper, xfdcawkDepdate, xfdcawkDepno,
xfdcawkErrormessage, xfdcawkErrorstatus, xfdcawkFilename, xfdcawkFiscalperiod,
xfdcawkFiscalyear, xfdcawkFiscalyearendon, xfdcawkFiscalyearstarton,
xfdcawkInstname, xfdcawkIsjvprocesseddate, xfdcawkIsprocessed,
xfdcawkIsprocesseddate, xfdcawkJvnumber, xfdcawkKeyeddate, xfdcawkNspsubmitted,
xfdcawkPyrlrefund, xfdcawkRecdate, xfdcawkTotaldep, xfdcawkTotalrev, id
```

## Interactive Workflow Features

### Menu-Driven Operations
The workflow orchestrator provides an interactive menu after completing the main workflow:
1. **Run Duplicate Analysis** - Execute detailed duplicate detection
2. **Create Test Data** - Generate test data files  
3. **Open Files in Editor** - View CSV files using nano editor
4. **Exit** - Complete the workflow

### Comprehensive Logging
All operations are logged to `dca_workflow.log` with:
- Timestamp-based entries
- Script execution details
- Output capture from all scripts
- User interaction history
- Error messages and status codes

## Error Handling & Troubleshooting

### Authentication Issues
- Clear error messages for missing API keys
- Guidance on configuration setup
- Fallback options for different authentication methods

### Network & API Issues
- Timeout handling with configurable limits
- Automatic retry logic for failed requests
- Detailed error reporting with HTTP status codes
- Session management for connection stability

### Data Processing Issues
- Safe handling of missing or malformed data fields
- Graceful degradation for incomplete records
- Validation of CSV file structure and content
- Clear reporting of data quality issues

## Security Best Practices

### API Key Management
1. **Never commit** `api_config.json` to version control
2. **Add** `api_config.json` to your `.gitignore` file
3. **Use environment variables** in production deployments
4. **Rotate API keys** regularly for security
5. **Restrict access** to configuration files

### Sample .gitignore Entry
```gitignore
# API Configuration
api_config.json
*.log
```

## System Requirements

### Dependencies
- Python 3.6+
- `requests` library for HTTP operations
- `csv` module (built-in)
- `json` module (built-in)
- `concurrent.futures` for parallel processing

### Installation
```bash
pip install requests
```

### File Permissions
Ensure the script directory has write permissions for:
- CSV output files
- Log files
- Configuration file updates (if saving API keys)

## Advanced Usage

### Environment-Specific Execution
Each script is dedicated to its specific environment:
- `dcawk_query_prod.py` - Production only, cannot access test keys
- `dcawk_query_test.py` - Test only, cannot access production keys
- No cross-environment contamination possible

### Batch Processing Optimization
- Datasets ≤ 10 batches: Sequential processing (more reliable)
- Datasets > 10 batches: Parallel processing (faster)
- Configurable worker limits (default: 5 concurrent requests)
- Automatic batch size optimization (1000 records per batch)

### Custom Configuration
Scripts automatically detect and adapt to:
- Dataset size for processing strategy selection
- Available system resources for worker allocation
- Network conditions for timeout adjustment
- API response patterns for optimization
