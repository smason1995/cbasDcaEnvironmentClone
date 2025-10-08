#from college_records import college_records_list 
#import cbas_module
import requests
import csv
from collections import Counter
import datetime
import math
import json

def get_token(api_key):
    # Set the URL of the login page
    url = f"https://integrate.elluciancloud.com/auth"

    headers = {'Authorization' : 'Basic ' + api_key, 'Content-Type' : 'text/plain'}

    # Set the login credentials
    #data = {"username": "apiuser", "password": "p*hj3Df4x592"}

    # Send the login request and store the response
    #response = requests.post(url, json=data, headers=headers)
    response = requests.post(url, headers=headers)
    # Get the JSON response body
    #print(response.text)
    #json_response = response.json()

    # Get the AWT token from the JSON response
    awt_token = response.text
    return awt_token

def post_xfdcawk(data, bearer_token):

    url = "https://integrate.elluciancloud.com/api/x-xfdcawk"

    #querystring = {"limit":"1000", "offset":f"{str(offset*1000)}"}
    #print(querystring)

    #headers = {"Authorization": f"Bearer {token}"}
    headers = {'content-type' : 'application/json', 'Accept' : 'application/json', "Authorization": f"Bearer {bearer_token}"}

    response = requests.post(url, headers=headers, data=data)

    #print(response.json())  
    return response.json()

total_count = 0
read_directory_in_str = "./"
read_file = "xdcawk_2025_diff.csv"
csv_header = ["xfdcawkAltbranch","xfdcawkBankacct","xfdcawkBankcity","xfdcawkBankname","xfdcawkBranch","xfdcawkCaprefund","xfdcawkCreatedon","xfdcawkCurrefund","xfdcawkDcasubmitted","xfdcawkDepaddoper",
                "xfdcawkDepdate","xfdcawkDepno","xfdcawkErrormessage","xfdcawkErrorstatus","xfdcawkFilename","xfdcawkFiscalperiod","xfdcawkFiscalyear","xfdcawkFiscalyearendon",
                "xfdcawkFiscalyearstarton","xfdcawkInstname","xfdcawkIsjvprocesseddate","xfdcawkIsprocessed","xfdcawkIsprocesseddate","xfdcawkJvnumber","xfdcawkKeyeddate",
                "xfdcawkNspsubmitted","xfdcawkPyrlrefund","xfdcawkRecdate","xfdcawkTotaldep","xfdcawkTotalrev","id"]


todays_date_str = str(datetime.datetime.now().strftime('%Y-%m-%d'))
#print(todays_date_str)

with open(f"{read_directory_in_str}{read_file}", newline='') as f:
    reader = csv.reader(f)
    data = list(reader)

    for idx, line in enumerate(data): 
            bearer_token = get_token("88ca9670-45d2-4385-a6e1-de1aa1de750d") #TEST
            '''
            if idx%100 == 0: #refresh token every 100 records
                bearer_token = get_token("88ca9670-45d2-4385-a6e1-de1aa1de750d") #TEST
                #bearer_token = get_token("dec4f29a-1f79-4b01-9efd-4265155d9de7") #PPRD 
                print(f"Refreshing token at {idx}")   
            '''
            #skip header line             
            if idx == 0:
                 continue                 
            total_count += 1 
            dcawk_json = dict(zip(csv_header, line))
            #remove JV fields and set id to NULL
            del dcawk_json["xfdcawkJvnumber"]
            del dcawk_json["xfdcawkIsjvprocesseddate"]
            dcawk_json['id'] = '00000000-0000-0000-0000-000000000000'
            dcawk_json['xfdcawkCreatedon'] = todays_date_str            
            json_string = json.dumps(dcawk_json) 
            #print(json_string)
            if 1 == 1:
                #print(json_string)
                try:
                    post_response = post_xfdcawk(json_string, bearer_token)		
                    print(post_response)		
                except Exception as e:
                    print(f"error {e} with record {json_string}")
            #print(line) 
print(f"total count={total_count}")
f.close()

'''
TEST INSERT BODY FOR BRUNO CALL
{
    "xfdcawkBankacct": "1306310042",
    "xfdcawkBankcity": "Forest City",
    "xfdcawkBankname": "BB&T",
    "xfdcawkBranch": "844",
    "xfdcawkCaprefund": "0.00",
    "xfdcawkCreatedon": "2025-03-07",
    "xfdcawkCurrefund": "0.00",
    "xfdcawkDcasubmitted": "N",
    "xfdcawkDepaddoper": "ZMURRAY",
    "xfdcawkDepdate": "2024-06-28",
    "xfdcawkDepno": "227.1X",
    "xfdcawkErrorstatus": "N",
    "xfdcawkFilename": "TOM_844_227.1X_20634.SEQ",
    "xfdcawkFiscalyear": "2324",
    "xfdcawkInstname": "Isothermal CC",
    "xfdcawkIsprocessed": "N",
    "xfdcawkKeyeddate": "2024-07-02",
    "xfdcawkNspsubmitted": "N",
    "xfdcawkPyrlrefund": "0.00",
    "xfdcawkRecdate": "2024-06-28",
    "xfdcawkTotaldep": "0.00",
    "xfdcawkTotalrev": "0.00",
    "id": "00000000-0000-0000-0000-000000000000"
  }
  RETURNS
{
  "xfdcawkBankacct": "1306310042",
  "xfdcawkBankcity": "Forest City",
  "xfdcawkBankname": "BB&T",
  "xfdcawkBranch": "844",
  "xfdcawkCaprefund": "0.00",
  "xfdcawkCreatedon": "2025-03-07",
  "xfdcawkCurrefund": "0.00",
  "xfdcawkDcasubmitted": "N",
  "xfdcawkDepaddoper": "ZMURRAY",
  "xfdcawkDepdate": "2024-06-28",
  "xfdcawkDepno": "227.1X",
  "xfdcawkErrorstatus": "N",
  "xfdcawkFilename": "TOM_844_227.1X_20634.SEQ",
  "xfdcawkFiscalyear": "2324",
  "xfdcawkInstname": "Isothermal CC",
  "xfdcawkIsprocessed": "N",
  "xfdcawkKeyeddate": "2024-07-02",
  "xfdcawkNspsubmitted": "N",
  "xfdcawkPyrlrefund": "0.00",
  "xfdcawkRecdate": "2024-06-28",
  "xfdcawkTotaldep": "0.00",
  "xfdcawkTotalrev": "0.00",
  "id": "73893741-cd1e-4256-a040-a94f27364ca6"
}  
'''