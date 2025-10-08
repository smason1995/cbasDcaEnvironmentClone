#!/usr/bin/env python3
"""
DCA Workflow Orchestrator
This script runs the complete DCA comparison workflow:
1. Runs dcawk_query_prod.py
2. Runs dcawk_query_test.py  
3. Runs dcawk_compare.py
4. Checks if differences match expected count
5. Prompts user for next action (analyze_duplicates or dcawk_create_test)
"""

import subprocess
import sys
import os
import csv
from pathlib import Path
from datetime import datetime

def write_log_header():
    """Initialize the log file with header information"""
    log_file = 'dca_workflow.log'
    with open(log_file, 'w') as f:
        f.write("="*80 + "\n")
        f.write("DCA WORKFLOW ORCHESTRATOR LOG\n")
        f.write("="*80 + "\n")
        f.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Working Directory: {os.getcwd()}\n")
        f.write("="*80 + "\n\n")
    return log_file

def log_section(log_file, section_title, content=""):
    """Add a section to the log file"""
    with open(log_file, 'a') as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"{section_title}\n")
        f.write(f"{'='*60}\n")
        f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"{'='*60}\n")
        if content:
            f.write(content)
            f.write("\n")

def run_script(script_name, description, log_file):
    """Run a Python script and return success status with logging"""
    print(f"\n{'='*60}")
    print(f"Running {description}...")
    print(f"{'='*60}")
    
    # Log the section start
    log_section(log_file, f"RUNNING: {description} ({script_name})")
    
    try:
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        
        # Log the output
        with open(log_file, 'a') as f:
            f.write("STDOUT:\n")
            f.write("-" * 40 + "\n")
            f.write(result.stdout if result.stdout else "(No output)\n")
            f.write("-" * 40 + "\n")
            
            if result.stderr:
                f.write("\nSTDERR:\n")
                f.write("-" * 40 + "\n")
                f.write(result.stderr)
                f.write("-" * 40 + "\n")
            
            f.write(f"\nExit Code: {result.returncode}\n")
            f.write(f"Status: SUCCESS\n")
        
        print(f"✅ {description} completed successfully")
        
        # Also display the output to console
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
            
        return True
        
    except subprocess.CalledProcessError as e:
        # Log the error
        with open(log_file, 'a') as f:
            f.write("STDOUT:\n")
            f.write("-" * 40 + "\n")
            f.write(e.stdout if e.stdout else "(No output)\n")
            f.write("-" * 40 + "\n")
            
            if e.stderr:
                f.write("\nSTDERR:\n")
                f.write("-" * 40 + "\n")
                f.write(e.stderr)
                f.write("-" * 40 + "\n")
            
            f.write(f"\nExit Code: {e.returncode}\n")
            f.write(f"Status: FAILED\n")
        
        print(f"❌ {description} failed with exit code {e.returncode}")
        if e.stderr:
            print("Error:", e.stderr)
        return False
        
    except FileNotFoundError:
        # Log the error
        with open(log_file, 'a') as f:
            f.write(f"ERROR: Script {script_name} not found\n")
            f.write(f"Status: FAILED\n")
        
        print(f"❌ Script {script_name} not found")
        return False

def count_csv_rows(filename):
    """Count rows in CSV file"""
    try:
        with open(filename, 'r') as file:
            return sum(1 for row in csv.DictReader(file))
    except FileNotFoundError:
        print(f"❌ File {filename} not found")
        return 0
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return 0

def check_expected_differences(log_file):
    """Check if the number of differences matches expected"""
    print(f"\n{'='*60}")
    print("Checking difference counts...")
    print(f"{'='*60}")
    
    # Log this section
    log_section(log_file, "CHECKING DIFFERENCE COUNTS")
    
    # Count rows in each file
    prod_count = count_csv_rows('xdcawk_2025_prod.csv')
    test_count = count_csv_rows('xdcawk_2025_test.csv')
    diff_count = count_csv_rows('xdcawk_2025_diff.csv')
    
    expected_diff = prod_count - test_count
    
    output = f"PROD file rows: {prod_count}\n"
    output += f"TEST file rows: {test_count}\n"
    output += f"Expected differences: {expected_diff}\n"
    output += f"Actual differences found: {diff_count}\n"
    
    print(output.strip())
    
    # Log the results
    with open(log_file, 'a') as f:
        f.write(output)
        
        if diff_count == expected_diff:
            f.write("Status: MATCH - Difference count matches expected!\n")
        else:
            f.write(f"Status: MISMATCH - Expected {expected_diff}, Found {diff_count}\n")
    
    if diff_count == expected_diff:
        print("✅ Difference count matches expected!")
        return True, expected_diff, diff_count
    else:
        print(f"⚠️  Difference count mismatch: Expected {expected_diff}, Found {diff_count}")
        return False, expected_diff, diff_count

def get_user_choice(matches_expected, expected_diff, actual_diff):
    """Get user choice for next action"""
    print(f"\n{'='*60}")
    print("What would you like to do next?")
    print(f"{'='*60}")
    
    if not matches_expected:
        print(f"⚠️  Note: Found {actual_diff} differences but expected {expected_diff}")
        print("You may want to analyze duplicates to understand the discrepancy.")
        print()
    
    print("1. Run analyze_duplicates.py (investigate duplicate IDs)")
    print("2. Run dcawk_create_test.py (create test data)")
    print("3. Open PROD CSV in nano editor")
    print("4. Open TEST CSV in nano editor")
    print("5. Open DIFF CSV in nano editor")
    print("6. Exit")
    
    while True:
        try:
            choice = input("\nEnter your choice (1-6): ").strip()
            if choice in ['1', '2', '3', '4', '5', '6']:
                return choice
            else:
                print("Invalid choice. Please enter 1, 2, 3, 4, 5, or 6.")
        except KeyboardInterrupt:
            print("\n\nExiting...")
            return '6'

def open_file_in_nano(filename):
    """Open a file in nano editor"""
    if not os.path.exists(filename):
        print(f"❌ File {filename} not found")
        return False
    
    try:
        print(f"📝 Opening {filename} in nano editor...")
        print("💡 Press Ctrl+X to exit nano and return to menu")
        subprocess.run(['nano', filename], check=True)
        print(f"✅ Closed {filename}")
        return True
    except subprocess.CalledProcessError:
        print(f"❌ Failed to open {filename} in nano")
        return False
    except FileNotFoundError:
        print("❌ nano editor not found. Please install nano or use a different editor.")
        return False

def run_analyze_duplicates(log_file):
    """Run analyze_duplicates.py and save output to file, then open in nano"""
    output_file = 'xdca_duplicates.txt'
    
    print(f"\n{'='*60}")
    print("Running Duplicate Analysis...")
    print(f"{'='*60}")
    
    # Log this section
    log_section(log_file, "RUNNING: Duplicate Analysis (analyze_duplicates.py)")
    
    try:
        # Run the script and capture output
        result = subprocess.run([sys.executable, 'analyze_duplicates.py'], 
                              capture_output=True, 
                              text=True, 
                              check=True)
        
        # Log the results
        with open(log_file, 'a') as f:
            f.write("STDOUT:\n")
            f.write("-" * 40 + "\n")
            f.write(result.stdout if result.stdout else "(No output)\n")
            f.write("-" * 40 + "\n")
            
            if result.stderr:
                f.write("\nSTDERR:\n")
                f.write("-" * 40 + "\n")
                f.write(result.stderr)
                f.write("-" * 40 + "\n")
            
            f.write(f"\nOutput saved to: {output_file}\n")
            f.write(f"Status: SUCCESS\n")
        
        # Save output to file
        with open(output_file, 'w') as f:
            f.write("=== DCA DUPLICATE ANALYSIS RESULTS ===\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(result.stdout)
            if result.stderr:
                f.write("\n=== ERRORS/WARNINGS ===\n")
                f.write(result.stderr)
        
        print(f"✅ Duplicate analysis completed. Results saved to {output_file}")
        
        # Open the file in nano
        print(f"📝 Opening {output_file} in nano editor...")
        print("💡 Press Ctrl+X to exit nano and return to menu")
        subprocess.run(['nano', output_file], check=True)
        print(f"✅ Closed {output_file}")
        return True
        
    except subprocess.CalledProcessError as e:
        # Log the error
        with open(log_file, 'a') as f:
            f.write("STDOUT:\n")
            f.write("-" * 40 + "\n")
            f.write(e.stdout if e.stdout else "(No output)\n")
            f.write("-" * 40 + "\n")
            
            if e.stderr:
                f.write("\nSTDERR:\n")
                f.write("-" * 40 + "\n")
                f.write(e.stderr)
                f.write("-" * 40 + "\n")
            
            f.write(f"\nStatus: FAILED (Exit code: {e.returncode})\n")
        
        print(f"❌ Duplicate analysis failed with exit code {e.returncode}")
        if e.stderr:
            print(f"Error: {e.stderr}")
        return False
    except FileNotFoundError:
        # Log the error
        with open(log_file, 'a') as f:
            f.write("ERROR: analyze_duplicates.py not found or nano editor not available\n")
            f.write("Status: FAILED\n")
        
        print("❌ analyze_duplicates.py not found or nano editor not available")
        return False

def main():
    """Main orchestration function"""
    print("🚀 Starting DCA Workflow Orchestrator")
    print("=" * 60)
    
    # Initialize log file
    log_file = write_log_header()
    print(f"📋 Logging to: {log_file}")
    
    # Check if we're in the right directory
    if not os.path.exists('dcawk_query_prod.py'):
        error_msg = "dcawk_query_prod.py not found. Please run this script from the DCA Data Migration directory."
        print(f"❌ {error_msg}")
        with open(log_file, 'a') as f:
            f.write(f"\nERROR: {error_msg}\n")
        sys.exit(1)
    
    scripts_to_run = [
        ('dcawk_query_prod.py', 'Production Data Query'),
        ('dcawk_query_test.py', 'Test Data Query'),
        ('dcawk_compare.py', 'Data Comparison')
    ]
    
    # Run the main workflow scripts
    for script_name, description in scripts_to_run:
        success = run_script(script_name, description, log_file)
        if not success:
            error_msg = f"Workflow stopped due to failure in {description}"
            print(f"\n❌ {error_msg}")
            with open(log_file, 'a') as f:
                f.write(f"\nWORKFLOW STOPPED: {error_msg}\n")
            sys.exit(1)
    
    # Check if differences match expected
    matches_expected, expected_diff, actual_diff = check_expected_differences(log_file)
    
    # Log the start of interactive session
    log_section(log_file, "INTERACTIVE MENU SESSION STARTED")
    
    # Interactive menu loop
    while True:
        # Get user choice for next action
        choice = get_user_choice(matches_expected, expected_diff, actual_diff)
        
        # Log user choice
        with open(log_file, 'a') as f:
            f.write(f"\nUser Choice: {choice} at {datetime.now().strftime('%H:%M:%S')}\n")
        
        if choice == '1':
            success = run_analyze_duplicates(log_file)
            if success:
                print("\n✅ Duplicate analysis completed and reviewed.")
            
        elif choice == '2':
            print(f"\n{'='*60}")
            print("Running Test Data Creation...")
            print(f"{'='*60}")
            success = run_script('dcawk_create_test.py', 'Test Data Creation', log_file)
            if success:
                print("\n✅ Test data creation completed.")
            
        elif choice == '3':
            with open(log_file, 'a') as f:
                f.write("Action: Opening PROD CSV in nano\n")
            success = open_file_in_nano('xdcawk_2025_prod.csv')
            
        elif choice == '4':
            with open(log_file, 'a') as f:
                f.write("Action: Opening TEST CSV in nano\n")
            success = open_file_in_nano('xdcawk_2025_test.csv')
            
        elif choice == '5':
            with open(log_file, 'a') as f:
                f.write("Action: Opening DIFF CSV in nano\n")
            success = open_file_in_nano('xdcawk_2025_diff.csv')
            
        elif choice == '6':
            with open(log_file, 'a') as f:
                f.write("Action: User chose to exit\n")
            print("\n👋 Exiting workflow. All comparison files are ready for review.")
            break
        
        # Add a small separator before showing menu again
        print(f"\n{'-'*40}")
        print("Returning to main menu...")
        print(f"{'-'*40}")
    
    # Log workflow completion
    log_section(log_file, "WORKFLOW COMPLETED", f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    print(f"\n{'='*60}")
    print("🎉 DCA Workflow Orchestrator completed!")
    print(f"{'='*60}")
    
    # Summary of generated files
    print("\nGenerated files:")
    files_to_check = [
        'xdcawk_2025_prod.csv',
        'xdcawk_2025_test.csv', 
        'xdcawk_2025_diff.csv',
        'xdca_duplicates.txt',
        'dca_workflow.log'
    ]
    
    file_summary = "Generated files summary:\n"
    for filename in files_to_check:
        if os.path.exists(filename):
            if filename.endswith('.csv'):
                row_count = count_csv_rows(filename)
                line = f"  📄 {filename}: {row_count} rows"
                print(line)
                file_summary += f"{line}\n"
            else:
                # For text files, show file size
                file_size = os.path.getsize(filename)
                line = f"  📄 {filename}: {file_size} bytes"
                print(line)
                file_summary += f"{line}\n"
        else:
            line = f"  ❌ {filename}: Not found"
            print(line)
            file_summary += f"{line}\n"
    
    # Log the file summary
    with open(log_file, 'a') as f:
        f.write(file_summary)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Workflow interrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
