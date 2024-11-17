from dotenv import dotenv_values
import requests
import json
from datetime import datetime
import pymupdf

secrets = dotenv_values(".env")

NOTION_TOKEN = secrets["NOTION_TOKEN"]
FUNDSWITCH_TOKEN = secrets["FUNDSWITCH_TOKEN"]
CLIENTS_DATABASE_ID = secrets["CLIENTS_DATABASE_ID"]
INTERACTIONS_DATABASE_ID = secrets["INTERACTIONS_DATABASE_ID"]
POLICIES_DATABASE_ID = secrets["POLICIES_DATABASE_ID"]
FUNDSWITCH_DATABASE_ID = secrets["FUNDSWITCH_DATABASE_ID"]

# Change the token accordingly
headers = {
    "Authorization": "Bearer " + FUNDSWITCH_TOKEN, 
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}

def get_json_file(data, file_name='db.json'):
    with open(file_name, 'w', encoding='utf8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    return data

def get_notion_database_pages(DATABASE_ID, num_pages=None):
    '''
    If num_pages is None, get all pages in the database, otherwise just the defined number.
    Results will return the information in a list of json.
    '''
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"

    get_all = num_pages is None
    page_size = 100 if get_all else num_pages
    payload = {"page_size": page_size}
    page_count = 1
    
    # Make the first request as usual
    print(f"Requesting page {page_count}")
    response = requests.post(url, json=payload, headers=headers)

    if response.ok:
        response_obj = response.json()		
        data = response_obj.get("results")

        # If there are more pages, request for the next page
        while response_obj.get("has_more"):
            page_count += 1
            print(f"Requesting page {page_count}")
            payload["start_cursor"] = response_obj.get("next_cursor")
            
            search_response = requests.post(url, json=payload, headers=headers)
            if search_response.ok:
                response_obj = search_response.json()
                data.extend(response_obj.get("results"))  

    return data

def extracting_fundswitch_database_page(pages):
    '''
    Extracting information from the database ID and returning a dictionary.
    Dictionary format: key is the page id, value is a dictionary of the page information.
    {
        page_id: {
            "To update form": page_to_update_form,
            "status": page_status,
            "Policy ID": page_policy_number,
            "Client ID": page_client_name,
            "NRIC": page_nric,
            "Switch Out From Fund Name": page_switch_out_fund_name,
            "Switch Out Allocation": page_switch_out_allocation,
            "Switch In To Fund Name": page_switch_in_fund_name,
            "Switch In Allocation": page_switch_in_allocation,
            "Premium Redirection": page_premium_redirection,
            "Premium Redirection New Allocation": page_premium_redirection_new_allocation,
            "url": page_url
        }
    }
    '''
    page_dict = {}
    for page in pages:
        page_id = page["id"]
        props = page["properties"]

        if props["Update PDF"]["checkbox"] == True:
            page_update_pdf = props["Update PDF"]["checkbox"]
            page_status = props["Status"]["status"]["name"]
            page_policy_id = props["Policy Number"]["relation"][0]["id"]
            page_client_id = props.get("Client Name", {}).get("rollup").get("array", [{}])[0].get("relation", [{}])[0].get("id")
            page_nric = props.get("NRIC", {}).get("formula", {}).get("string")

            if not props.get("Switch Out From Fund Name", {}).get("rich_text"):
                page_switch_out_fund_name = None
            else:
                page_switch_out_fund_name = props.get("Switch Out From Fund Name", {}).get("rich_text", [{}])[0].get("text", {}).get("content")
            if not props.get("Switch Out Allocation", {}).get("rich_text"):
                page_switch_out_allocation = None
            else:
                page_switch_out_allocation = props.get("Switch Out Allocation", {}).get("rich_text", [{}])[0].get("text", {}).get("content")
            
            if not props.get("Switch In To Fund Name", {}).get("rich_text"):
                page_switch_in_fund_name = None
            else:
                page_switch_in_fund_name = props.get("Switch In To Fund Name", {}).get("rich_text", [{}])[0].get("text", {}).get("content")

            if not props.get("Switch In Allocation", {}).get("rich_text"):
                page_switch_in_allocation = None
            else:
                page_switch_in_allocation = props.get("Switch In Allocation", {}).get("rich_text", [{}])[0].get("text", {}).get("content")

            if not props.get("Premium Redirection", {}).get("rich_text"):
                page_premium_redirection = None
            else:
               page_premium_redirection = props.get("Premium Redirection", {}).get("rich_text", [{}])[0].get("text", {}).get("content")
    
            if not props.get("Premium Redirection New Allocation", {}).get("rich_text"):
                page_premium_redirection_new_allocation = None
            else:
                page_premium_redirection_new_allocation = props.get("Premium Redirection New Allocation", {}).get("rich_text", [{}])[0].get("text", {}).get("content")

            if not props.get("Filename", {}).get("title", [{}]):
                page_remarks = None
            else:
                page_remarks = props.get("Remarks", {}).get("title", [{}])[0].get("text", {}).get("content")
            
            page_url = page["url"]
        else:
            continue
        
        page_dict[page_id] = {
            "Update PDF": page_update_pdf,
            "Status": page_status,
            "Policy ID": page_policy_id,
            "Client ID": page_client_id,
            "NRIC": page_nric,
            "Switch Out From Fund Name": page_switch_out_fund_name,
            "Switch Out Allocation": page_switch_out_allocation,
            "Switch In To Fund Name": page_switch_in_fund_name,
            "Switch In Allocation": page_switch_in_allocation,
            "Premium Redirection": page_premium_redirection,
            "Premium Redirection New Allocation": page_premium_redirection_new_allocation,
            "Remarks": page_remarks,
            "url": page_url
        }
    if not page_dict: # If page_dict is empty
        print("No pages detected to sync.")

    return page_dict    

def update_notion_database(page_id: str, data: dict):
    url = f"https://api.notion.com/v1/pages/{page_id}"

    payload = {"properties": data}

    res = requests.patch(url, json=payload, headers=headers)
    print(res.status_code)
    return res

def get_all_client_ids_names():
    clients = get_notion_database_pages(CLIENTS_DATABASE_ID)
    client_page_dict = {}
    for client in clients:
        client_id = client["id"]
        props = client["properties"]
        client_name = props["Name"]["title"][0]["text"]["content"]

        client_page_dict[client_id] = {
            "name": client_name
        }
    return client_page_dict

def match_client_id_name(client_page_dict, client_id):
    # Match the client id found in page id to the client name 
    if client_id in client_page_dict:
        client_name = client_page_dict[client_id]['name']
        # print(f"Client ID: {client_id} has client: {client_name}")
    return client_name

def find_all_policy_numbers_ids():
    policies = get_notion_database_pages(POLICIES_DATABASE_ID)
    policies_number_page_dict = {}
    for policy in policies:
        policy_id = policy["id"]
        props = policy["properties"]
        policy_number = props["Policy number"]["title"][0]["text"]["content"]

        policies_number_page_dict[policy_id] = {
            "policy number": policy_number
        }
    return policies_number_page_dict

def match_policy_id_number(policies_number_page_dict, policy_id):
    # Match the client id found in page id to the client name 
    if policy_id in policies_number_page_dict:
        policy_number = policies_number_page_dict[policy_id]['policy number']
        # print(f"Policy ID: {policy_id} has Policy Number: {policy_number}")
    return policy_number

def write_to_pdf(entry_data, source_file_name='FundswitchForm.pdf', output_file_name='FundswitchForm_filled.pdf'):
    doc = pymupdf.open(source_file_name)

    for page in doc:
        for field in page.widgets():
            if field.field_name in entry_data:
                field.field_value = entry_data[field.field_name]
                field.text_fontsize = 11
                field.update()

    doc.save(output_file_name)

fundswitch_pages = get_notion_database_pages(FUNDSWITCH_DATABASE_ID)
fundswitch_page_dict = extracting_fundswitch_database_page(fundswitch_pages)

all_client_ids_names = get_all_client_ids_names()
all_policy_numbers_ids = find_all_policy_numbers_ids()

for page_id, page_info in fundswitch_page_dict.items():
    entry_data = {
    # Page 1
    "Check Box2": True,

    # Page 2
    "Check Box5": True,
    "Check Box6": True,
    "Specified Investment Products that was made in the last 3 years 1": "Buying & Selling Unit Trust more than 6 times in the 3 years",
    "Check Box9": True,

    # Page 3
    "Check Box12": True,

    # Page 5
    "Check Box31": True,

    # Risk Profile Questionnaire
    "Check Box21": True, # Q1
    "Check Box192": True, # Q2
    "Check Box195": True, # Q3
    "Check Box1950": True, # Q4
    "Check Box1955": True, # Q5
    "Check Box19512": True, # Q6
    "Check Box2321": True, # Risk profile score
    "Check Box2200": True, # Declaration
    }

    # Get the client name and policy number
    client_id = page_info['Client ID']
    policy_id = page_info['Policy ID']
    client_name = match_client_id_name(all_client_ids_names, client_id)
    policy_number = match_policy_id_number(all_policy_numbers_ids, policy_id)

    # Switch Out Funds
    all_switch_out_funds = page_info["Switch Out From Fund Name"]
    individual_switch_out_funds = all_switch_out_funds.split(";")
    all_switch_out_funds_allocation = page_info["Switch Out Allocation"]
    individual_switch_out_funds_allocation = all_switch_out_funds_allocation.split(";")

    # Put the funds and allocation into a list (May contain more than 1 fund and allocation)
    switch_out_fund_list = [fund.strip() for fund in individual_switch_out_funds]
    switch_out_allocation_list = [allocation.strip() for allocation in individual_switch_out_funds_allocation]
    # Add the funds and allocation into the entry_data dictionary
    for idx, fund in enumerate(switch_out_fund_list, start=1):
        key = f"Switch Out From Fund Name Or Fund CodeRow{idx}"
        entry_data[key] = fund
    for idx, allocation in enumerate(switch_out_allocation_list, start=1):
        key = f"Allocation Row{idx}"
        entry_data[key] = allocation

    # Switch In Funds
    all_switch_in_funds = page_info["Switch In To Fund Name"]
    individual_switch_in_funds = all_switch_in_funds.split(";") # Split multiple funds into a list
    all_switch_in_funds_allocation = page_info["Switch In Allocation"]
    individual_switch_in_funds_allocation = all_switch_in_funds_allocation.split(";")

    switch_in_fund_list = [fund.strip() for fund in individual_switch_in_funds]
    switch_in_allocation_list = [allocation.strip() for allocation in individual_switch_in_funds_allocation]
    for idx, fund in enumerate(switch_in_fund_list, start=1):
        key = f"Switch In To Fund Name Or Fund CodeRow{idx}"
        entry_data[key] = fund

    for idx, allocation in enumerate(switch_in_allocation_list, start=1):
        key = f"Allocation Row{idx}_2"
        entry_data[key] = allocation

    entry_data.update({
        'Policy Number': policy_number,
        'Name of Policyholder AssigneeTrustee': client_name,
        'NRIC No': page_info["NRIC"],
        'Name of Life Assured': client_name,
        'NRIC No_2': page_info["NRIC"],
        'of NRIC No Passport No' : client_name,
        'acknowledge that I am aware of' : page_info["NRIC"],
        'DATE': datetime.now().strftime("%d/%m/%Y"), # Set to today's date

        # Page 6
        'POLICY NUMBER': policy_number,

        # Page 10
        'Date': datetime.now().strftime("%d/%m/%Y")
    })

    write_to_pdf(entry_data, output_file_name=f'FundswitchForm_{client_name}.pdf')

    # Update the Notion Fundswitch database
    update_notion_data = {
        "Update PDF": {"checkbox": False},
        "Status":  {"status": {"name" : "PDF created"}},
        "Date PDF created": {"date": {"start": datetime.now().strftime("%Y-%m-%d")}},
        "Filename" : {"title": [{"text": {"content": f"FundswitchForm_{client_name}"}}]},
    }
    
    update_notion_database(page_id, update_notion_data)