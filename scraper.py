import requests
import pandas as pd
import time
import os

# Filter: Swedish country, National bank of belgium , status active
json_filter = {"$and": [{"_payload.EntityType": "PSD_AG"}, {"_searchkeys": {"$elemMatch": {"T": "P", "K": "DER_CHI_ENT_AUT", "V": "YES"}}}, {
    "_searchkeys": {"$elemMatch": {"T": "P", "K": "ENT_COU_RES", "V": "SE"}}}, {"_payload.CA_OwnerID": "BE_NBB"}]}

url = "https://euclid.eba.europa.eu/register/pir/search/agents"

parent_url = "https://euclid.eba.europa.eu/register/api/entity/read/"

s =requests.session()
# seems to be ENT_TYP_PAR_ENT+ENT_COD_PAR_ENT

def get_search_key(properties):
    parent_entity_type = ""
    parent_entity_code = ""
    for property in properties:
        if "ENT_TYP_PAR_ENT" in property:
            parent_entity_type = property["ENT_TYP_PAR_ENT"]
        if "ENT_COD_PAR_ENT" in property:
            parent_entity_code = property["ENT_COD_PAR_ENT"]

    if parent_entity_type == "" or parent_entity_code == "":
        raise RuntimeError("Could not find parent entity type or code")
    return f"{parent_entity_type}.{parent_entity_code}"

def get_parent_details(agent):
    search_key = get_search_key(agent["Properties"])
    response = s.get(f"{parent_url}{search_key}",params={"t": f"{int(time.time() * 1000)}"})
    if response.status_code == 200:
        parent = response.json()
        parent = parent[0]['_payload']
        renamed_props =[]
        for prop in parent["Properties"]:
            if len(prop) != 0:
                key, value = list(prop.items())[0]
                renamed_props.append({f"parent_{key}" : value})
        parent["Properties"] = renamed_props
        
    else:
        raise RuntimeError(
            "Error, exiting early: {}".format(response.status_code))
        
    return parent

def format_payload_results(agent):
    updated_properties = {}
    for prop in agent["Properties"]:
        updated_properties.update(prop)
    agent["Properties"] = updated_properties
    return agent
def main():
    response = s.post("https://euclid.eba.europa.eu/register/api/search/entities",
                      params={"t": f"{int(time.time() * 1000)}"}, json=json_filter)
    result = []
    # Check if the response is successful
    if response.status_code == 200:
        psd2_agents = response.json()

        for agent in psd2_agents:
            agent = agent['_payload']
            parent_details = get_parent_details(agent)
            agent = format_payload_results(agent)
            parent_details = format_payload_results(parent_details)
            result.append({**agent["Properties"], **parent_details["Properties"]})
    else:
        # Handle error
        raise RuntimeError(
            "Error, exiting early: {}".format(response.status_code))

    pd.DataFrame(result).to_csv('data.csv')    


if __name__ == '__main__':
    print("Starting scraper")
    try:
        main()
    except Exception as e:
        print("Error while scraping", e)
    print("Finished scraping without errors")
    print(f"Wrote results to {os.getcwd()}/data.csv")
