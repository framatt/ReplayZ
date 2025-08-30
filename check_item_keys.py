import os
from dotenv import load_dotenv
from zabbix_utils import ZabbixAPI

# Load environment variables
load_dotenv()

# Get Zabbix configuration
source_url = os.getenv('SOURCE_ZABBIX_URL')
source_token = os.getenv('SOURCE_ZABBIX_TOKEN')
dest_url = os.getenv('DEST_ZABBIX_URL')
dest_token = os.getenv('DEST_ZABBIX_TOKEN')

source_host_id = '10468'
source_item_id = '39286'
dest_host_id = '10539'
dest_item_id = '128726'

try:
    # Connect to Source Zabbix
    source_zapi = ZabbixAPI(url=source_url)
    source_zapi.login(token=source_token)

    # Fetch source item details
    source_items = source_zapi.item.get(itemids=source_item_id, hostids=source_host_id, output=['key_'])
    source_item_key = source_items[0]['key_'] if source_items else 'Not Found'
    print(f"Source Item (ID: {source_item_id}, Host ID: {source_host_id}) Key: {source_item_key}")

    # Connect to Destination Zabbix
    dest_zapi = ZabbixAPI(url=dest_url)
    dest_zapi.login(token=dest_token)

    # Fetch destination item details
    dest_items = dest_zapi.item.get(itemids=dest_item_id, hostids=dest_host_id, output=['key_'])
    dest_item_key = dest_items[0]['key_'] if dest_items else 'Not Found'
    print(f"Destination Item (ID: {dest_item_id}, Host ID: {dest_host_id}) Key: {dest_item_key}")

except Exception as e:
    print(f"An error occurred: {e}")
