from flask import Flask, render_template, request, jsonify
import logging
import re # Import the re module for regular expressions
from zabbix_utils import ZabbixAPI # Use official zabbix_utils
from faker import Faker # Import Faker for data obfuscation
import random # Import random for IP address generation
# ZabbixAPIException is not directly available, will catch generic Exception
from zabbix_utils import Sender, ItemValue # Correct Sender/ItemValue import
from datetime import datetime, timedelta
import time
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore # Import SQLAlchemyJobStore

# Load environment variables
from dotenv import load_dotenv
import os
# load_dotenv(os.path.join(os.path.dirname(__file__), '.env')) # Loaded in common.py

import json

from jobs import replay_job # Import the replay job function

# Import trigger utility functions
from trigger_utils import perform_mapping_rebuild,create_icmp_trigger, create_icmp_loss_trigger, create_icmp_response_trigger, create_interface_link_down_trigger, create_interface_speed_change_trigger,create_cpu_utilization_trigger,create_temperature_critical_trigger

# Import SQLAlchemy components and config from common.py
from common import ReplicationTask, Base, engine, SessionLocal, config

# Type hints
from typing import Dict, List, Optional, Tuple, Any

# Constants
ZABBIX_ITEM_TYPE_TRAPPER = '2'
ZABBIX_VALUE_TYPES_SUPPORTED = ['0', '1', '2', '3', '4']
DEFAULT_HISTORY_HOURS = 12
REPLAY_INTERVAL_SECONDS = 60
DEFAULT_GROUP_NAME = 'Zabbix servers'
CLONED_GROUP_NAME = 'clonedfordemo'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'

# Basic logging setup
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)

app = Flask(__name__)

# Configure persistent job store using SQLite
jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite') # Store jobs in jobs.sqlite file
}
scheduler = BackgroundScheduler(jobstores=jobstores, daemon=True) # Use the configured job store
replication_tasks = {} # Store details about ongoing replications {source_host_id: ReplicationTask object}

# Initialize Faker
fake = Faker()

# --- Global Configuration for Item Skipping ---
SKIP_KEY_PREFIXES = ['MTR'] # Keys starting with these prefixes will be skipped
from flask import Flask, render_template, request, jsonify
import logging
import re # Import the re module for regular expressions
from zabbix_utils import ZabbixAPI # Use official zabbix_utils
from faker import Faker # Import Faker for data obfuscation
import random # Import random for IP address generation
# ZabbixAPIException is not directly available, will catch generic Exception
from zabbix_utils import Sender, ItemValue # Correct Sender/ItemValue import
from datetime import datetime, timedelta
import time
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore # Import SQLAlchemyJobStore

# Load environment variables
from dotenv import load_dotenv
import os
# load_dotenv(os.path.join(os.path.dirname(__file__), '.env')) # Loaded in common.py

import json

from jobs import replay_job # Import the replay job function

# Import trigger utility functions
from trigger_utils import perform_mapping_rebuild,create_icmp_trigger, create_icmp_loss_trigger, create_icmp_response_trigger, create_interface_link_down_trigger, create_interface_speed_change_trigger,create_cpu_utilization_trigger,create_temperature_critical_trigger

# Import SQLAlchemy components and config from common.py
from common import ReplicationTask, Base, engine, SessionLocal, config

# Basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__)

# Configure persistent job store using SQLite
jobstores = {
    'default': SQLAlchemyJobStore(url='sqlite:///jobs.sqlite') # Store jobs in jobs.sqlite file
}
scheduler = BackgroundScheduler(jobstores=jobstores, daemon=True) # Use the configured job store
replication_tasks = {} # Store details about ongoing replications {source_host_id: ReplicationTask object}

# Initialize Faker
fake = Faker()

# --- Global Configuration for Item Skipping ---
SKIP_KEY_PREFIXES = ['MTR'] # Keys starting with these prefixes will be skipped

def should_skip_item(item_name, item_key):
    """
    Determines if an item should be skipped based on predefined rules.
    """
    # Rule 1: Skip items with '<' in their name (dynamic interfaces)
    if '<' in item_name and '>' in item_name:
        logging.info(f"Skipping item '{item_name}' (key: {item_key}) due to dynamic interface pattern '<...>' in name.")
        return True

    # Rule 2: Skip items whose key starts with any of the defined prefixes
    for prefix in SKIP_KEY_PREFIXES:
        if item_key.startswith(prefix):
            logging.info(f"Skipping item '{item_name}' (key: {item_key}) as its key starts with '{prefix}'.")
            return True
    return False

# --- Obfuscation Functions ---
def generate_fake_hostname():
    """Generates a fake hostname."""
    return fake.word().capitalize() + fake.word().capitalize() + str(random.randint(100, 999))

def generate_fake_ip_address():
    """Generates a fake IP address (IPv4)."""
    return fake.ipv4()

# --- Helper Functions ---

def map_entities_by_name(source_entities, dest_entities_func, entity_name_key='name', entity_id_key='groupid', entity_type='group', zapi=None):
    """Maps source entities (like groups or templates) to destination IDs by name. Creates missing entities if needed."""
    dest_entities = dest_entities_func()
    dest_map = {entity[entity_name_key]: entity[entity_id_key] for entity in dest_entities}
    mapped_ids = []
    missing = []

    for source_entity in source_entities:
        source_name = source_entity[entity_name_key]
        dest_id = dest_map.get(source_name)

        if dest_id:
            mapped_ids.append({entity_id_key: dest_id})
        else:
            # Try to create missing entity if we have API access
            if zapi:
                try:
                    if entity_type == 'group':
                        new_entity = zapi.hostgroup.create({"name": source_name})
                    elif entity_type == 'template':
                        new_entity = zapi.template.create({"host": source_name, "groups": {"groupid": "1"}})
                    else:
                        raise ValueError(f"Unsupported entity type: {entity_type}")

                    new_id = new_entity[f"{entity_type}ids"][0]
                    mapped_ids.append({entity_id_key: new_id})
                    #logging.info(f"Created missing {entity_type}: {source_name} with ID {new_id}")
                    continue
                except Exception as e:
                    logging.error(f"Failed to create {entity_type} '{source_name}': {e}")

            missing.append(source_name)
            logging.warning(f"Destination {entity_type} '{source_name}' not found and could not be created. Skipping.")

    if missing:
        logging.warning(f"Could not find/create the following destination {entity_type}s: {', '.join(missing)}")
    return mapped_ids

def modify_items_to_trapper(source_items):
    """Modifies a list of item configurations to be Zabbix Trapper type."""
    modified_items = []
    # Zabbix item type: 2 = Zabbix trapper
    # Zabbix value types: 0 = numeric float, 1 = character, 2 = log, 3 = numeric unsigned, 4 = text
    allowed_value_types = ['0', '1', '2', '3', '4'] # Trappers support these

    for item in source_items:
        item_name = item.get('name', '')
        item_key = item.get('key_', '')

        # Check if item should be skipped based on predefined rules
        if should_skip_item(item_name, item_key):
            continue

        # Skip items with unsupported value types for trappers if any exist (though most common ones are fine)
        if item.get('value_type') not in allowed_value_types:
            logging.warning(f"Skipping item '{item_name}' (key: {item_key}) due to unsupported value type {item.get('value_type')} for trapper items.")
            continue

        # Create a copy to avoid modifying the original source config dict
        mod_item = item.copy()

        # Change type to Trapper
        mod_item['type'] = '2'

        # Remove fields irrelevant to Trapper items
        mod_item.pop('delay', None)
        mod_item.pop('status', None) # Let destination decide status (usually enabled)
        mod_item.pop('flags', None) # Flags often relate to discovery

        # Ensure essential fields are present (though they should be from the source get)
        mod_item.setdefault('name', 'Unnamed Trapper Item')
        mod_item.setdefault('key_', f"trapper.key.{item.get('itemid', 'unknown')}")
        mod_item.setdefault('value_type', '4') # Default to text if missing? Should exist.
        mod_item.setdefault('hostid', None) # This will be set by the host.create call

        # Remove itemid as it's specific to the source Zabbix
        mod_item.pop('itemid', None)

        modified_items.append(mod_item)

    #logging.info(f"Modified {len(modified_items)} items to Trapper type.")
    return modified_items

# Update function signature to accept all_source_items
def check_and_map_existing_host(host_name, all_source_items, dest_zapi):
    """Checks if the host exists on the destination and builds item mapping if so."""
    source_items_map_by_key = {item['key_']: item['itemid'] for item in all_source_items}
    logging.debug(f"Built source_items_map_by_key with {len(source_items_map_by_key)} entries from all_source_items.")

    existing_hosts = dest_zapi.host.get(filter={"host": [host_name]}, output=['hostid'])
    if not existing_hosts:
        return None, {} # Host does not exist

    existing_host_id = existing_hosts[0]['hostid']
    logging.info(f"Host '{host_name}' already exists on destination with ID {existing_host_id}. Building item mapping including inherited items.")

    existing_dest_items = dest_zapi.item.get(hostids=existing_host_id, output=['itemid', 'key_'])
    item_mapping = {}
    dest_items_map_by_key = {item['key_']: item for item in existing_dest_items}
    logging.info(f"Found {len(existing_dest_items)} total items on existing destination host {existing_host_id} (querying by hostid).")
    logging.info(f"Comparing against {len(source_items_map_by_key)} total source items.")

    for source_key, source_itemid in source_items_map_by_key.items():
        if source_key in dest_items_map_by_key:
            item_mapping[source_itemid] = source_key
            logging.debug(f"Mapped Source Item ID {source_itemid} -> Dest Key '{source_key}' (Existing Host)")
        else:
            logging.warning(f"Source item with key '{source_key}' (ID: {source_itemid}) not found on existing destination host {existing_host_id} (incl. inherited).")

    logging.info(f"Generated item mapping for EXISTING host: {len(item_mapping)} entries.")
    return existing_host_id, item_mapping

def replicate_macros(host_id, source_macros, dest_zapi):
    """Replicates macros for a given host."""
    if not source_macros:
        logging.info(f"No macros defined on source host to replicate for host ID {host_id}.")
        return

    logging.info(f"Attempting to replicate {len(source_macros)} macros for host ID {host_id}...")
    macros_created = 0
    macros_failed = 0

    for macro in source_macros:
        macro_data = {
            "macro": macro['macro'],
            "value": macro['value'],
            "type": macro.get('type', '0'),
            "description": macro.get('description', ''),
            "hostid": host_id
        }

        try:
            macro_result = dest_zapi.usermacro.create(macro_data)
            logging.debug(f"Successfully created macro '{macro['macro']}' for host ID {host_id}: {macro_result}")
            macros_created += 1
        except Exception as macro_e:
            if "already exists" in str(macro_e):
                logging.warning(f"Macro '{macro['macro']}' already exists for host ID {host_id}. Skipping.")
            else:
                logging.error(f"Failed to create macro '{macro['macro']}' for host ID {host_id}: {macro_e}", exc_info=True)
                macros_failed += 1

    logging.info(f"Macro replication completed for host ID {host_id}: {macros_created} created, {macros_failed} failed.")
    if macros_failed > 0:
        logging.warning(f"Some macros failed to replicate for host ID {host_id}. Check logs for details.")

def create_standard_icmp_triggers(dest_zapi, host_id, host_name):
    """Creates standard ICMP triggers for a host."""
    logging.info(f"Attempting to create standard ICMP triggers for host '{host_name}' (ID: {host_id})...")
    create_icmp_trigger(dest_zapi, host_id, host_name)
    create_icmp_loss_trigger(dest_zapi, host_id, host_name)
    create_icmp_response_trigger(dest_zapi, host_id, host_name)

def create_interface_triggers(dest_zapi, host_id, host_name, all_source_items):
    """Creates interface-related triggers (link down, speed change) for a host."""
    logging.info(f"Attempting to create Interface Link Down and Speed Change triggers for host '{host_name}' (ID: {host_id})...")
    link_down_trigger_count = 0
    for item in all_source_items:
        item_key = item.get('key_', '')
        item_name = item.get('name')
        match = re.search(r'net\.if\.status\[ifOperStatus\.("?([^"]+)"?|\d+)\]', item_key)
        if match:
            identifier_part = match.group(1)
            interface_identifier = identifier_part.strip('"')
            interface_name = item_name.split(':')[0].strip() if ':' in item_name else item_name
            interface_name = interface_name.replace("Interface ", "", 1).strip()

            logging.info(f"Found ifOperStatus item for host: Key='{item_key}', Identifier='{interface_name}'")
            create_interface_link_down_trigger(
                dest_zapi,
                host_id,
                host_name,
                interface_name,
                item_key
            )

            speed_item_key = None
            type_item_key = None
            expected_speed_key_ifSpeed = f'net.if.speed[ifSpeed.{identifier_part}]'
            expected_speed_key_ifHighSpeed = f'net.if.speed[ifHighSpeed.{identifier_part}]'
            expected_type_key = f'net.if.type[ifType.{identifier_part}]'

            for source_item in all_source_items:
                if source_item.get('key_') == expected_speed_key_ifSpeed:
                    speed_item_key = source_item.get('key_')
                elif source_item.get('key_') == expected_type_key:
                    type_item_key = source_item.get('key_')
                elif source_item.get('key_') == expected_speed_key_ifHighSpeed:
                    speed_item_key = source_item.get('key_')

            if speed_item_key and type_item_key:
                create_interface_speed_change_trigger(
                    dest_zapi,
                    host_id,
                    host_name,
                    interface_name,
                    speed_item_key,
                    item_key,
                    type_item_key
                )
                link_down_trigger_count += 1
            else:
                logging.warning(f"Could not find corresponding speed ({expected_speed_key_ifSpeed} or {expected_speed_key_ifHighSpeed}) or type ({expected_type_key}) item for interface '{interface_name}'. Skipping speed change trigger creation.")

    logging.info(f"Attempted creation of {link_down_trigger_count} Interface Link Down triggers based on found ifOperStatus items.")

def create_cpu_utilization_trigger_for_host(dest_zapi, host_id, host_name, all_source_items):
    """Creates CPU utilization trigger for a host if a relevant item exists."""
    logging.info(f"Attempting to create CPU utilization trigger for host '{host_name}' (ID: {host_id})...")
    for item in all_source_items:
        item_key = item.get('key_', '')
        match = re.search(r'system\.cpu\.util\[(.*?)\]', item_key)
        if match:
            create_cpu_utilization_trigger(dest_zapi, host_id, host_name)
            return # Assuming one CPU trigger is sufficient

def create_temperature_critical_trigger_for_host(dest_zapi, host_id, host_name, all_source_items):
    """Creates temperature critical trigger for a host if a relevant item exists."""
    logging.info(f"Attempting to create temperature critical trigger for host '{host_name}' (ID: {host_id})...")
    for item in all_source_items:
        item_key = item.get('key_', '')
        match = re.search(r'sensor\.temp\.value\[(.*?)\]', item_key)
        if match:
            create_temperature_critical_trigger(dest_zapi, host_id, host_name, item_key)
            return # Assuming one temperature trigger is sufficient

def create_direct_host_items_as_trappers(dest_zapi, dest_host_id, modified_items, source_items_map_by_key):
    """Creates direct host items as trappers on the destination and builds item mapping."""
    item_mapping = {}
    if not modified_items:
        logging.warning("No modified direct host items to create as trappers.")
        return item_mapping

    logging.info(f"Attempting to create/update {len(modified_items)} direct host items (as trappers) for new host ID {dest_host_id}...")
    successfully_created_keys = set()
    for item_to_create in modified_items:
        item_key = item_to_create.get('key_')
        item_name = item_to_create.get('name')
        try:
            item_to_create['hostid'] = dest_host_id
            create_result = dest_zapi.item.create(item_to_create)
            if create_result and 'itemids' in create_result and create_result['itemids']:
                logging.debug(f"Successfully created item '{item_name}' (Key: {item_key})")
                successfully_created_keys.add(item_key)
            else:
                 logging.error(f"Failed to create item '{item_name}' (Key: {item_key}): API response missing itemids. Response: {create_result}")
        except Exception as e:
            if "already exists" in str(e):
                 logging.warning(f"Item '{item_name}' (Key: {item_key}) already exists on host {dest_host_id}. Assuming it's usable.")
                 successfully_created_keys.add(item_key)
            else:
                 logging.error(f"Failed to create item '{item_name}' (Key: {item_key}): {e}")

    logging.info(f"Building item mapping based on {len(successfully_created_keys)} successfully created/found items.")
    for dest_key in successfully_created_keys:
        source_itemid = source_items_map_by_key.get(dest_key)
        if source_itemid:
            item_mapping[source_itemid] = dest_key
            logging.debug(f"Mapped Source Item ID {source_itemid} -> Dest Key '{dest_key}'")
        else:
            logging.warning(f"Successfully created destination item with key '{dest_key}', but could not find corresponding source item ID.")

    logging.info(f"Generated item mapping for NEW host: {len(item_mapping)} entries.")
    return item_mapping


def create_destination_host(source_host_config, all_source_items, dest_group_ids, dest_template_ids, modified_items, dest_zapi):
    """Creates the host on the destination Zabbix server and builds item mapping."""
    original_host_name = source_host_config['name']
    source_items_map_by_key = {item['key_']: item['itemid'] for item in all_source_items}

    # Generate a fake hostname for privacy
    obfuscated_host_name = generate_fake_hostname()
    logging.info(f"Obfuscating host name from '{original_host_name}' to '{obfuscated_host_name}'")

    # Check if the obfuscated host already exists on destination
    # Note: This check is now against the obfuscated name
    existing_host_id, item_mapping = check_and_map_existing_host(obfuscated_host_name, all_source_items, dest_zapi)

    if existing_host_id:
        # Host exists, replicate macros and create triggers
        replicate_macros(existing_host_id, source_host_config.get('macros', []), dest_zapi)
        create_standard_icmp_triggers(dest_zapi, existing_host_id, obfuscated_host_name)
        create_interface_triggers(dest_zapi, existing_host_id, obfuscated_host_name, all_source_items)
        create_cpu_utilization_trigger_for_host(dest_zapi, existing_host_id, obfuscated_host_name, all_source_items)
        create_temperature_critical_trigger_for_host(dest_zapi, existing_host_id, obfuscated_host_name, all_source_items)
        return existing_host_id, item_mapping

    # Host does NOT exist, proceed with creation
    logging.info(f"Host '{obfuscated_host_name}' not found on destination. Proceeding with creation.")

    # Prepare interfaces - ensure proper types for API and obfuscate IP addresses
    interfaces = source_host_config.get('interfaces', [])
    obfuscated_interfaces = []
    for iface in interfaces:
        mod_iface = iface.copy() # Create a copy to modify
        mod_iface['type'] = int(mod_iface.get('type', '1'))
        mod_iface['main'] = int(mod_iface.get('main', '0'))
        mod_iface['useip'] = int(mod_iface.get('useip', '0'))
        if 'port' in mod_iface:
            mod_iface['port'] = str(mod_iface['port'])

        # Obfuscate IP address if useip is enabled and an IP exists
        if mod_iface['useip'] == 1 and mod_iface.get('ip'):
            original_ip = mod_iface['ip']
            fake_ip = generate_fake_ip_address()
            mod_iface['ip'] = fake_ip
            logging.info(f"Obfuscating IP address from '{original_ip}' to '{fake_ip}' for interface type {mod_iface['type']}")
        elif mod_iface['useip'] == 0 and mod_iface.get('dns'):
            # If using DNS, we might want to obfuscate DNS too, but for now, keep as is or generate fake DNS
            # For simplicity, we'll leave DNS as is, or you could generate a fake domain name
            logging.info(f"Interface uses DNS: {mod_iface['dns']}. IP obfuscation skipped.")


        if mod_iface['type'] == 2: # SNMP interface
            mod_iface['details'] = {
                'version': int(mod_iface.get('details', {}).get('version', '2')),
                'community': mod_iface.get('details', {}).get('community', 'public')
            }
        mod_iface.pop('interfaceid', None)
        obfuscated_interfaces.append(mod_iface)

    params = {
        "host": obfuscated_host_name, # Use the obfuscated hostname
        "name": obfuscated_host_name, # Also set visible name to obfuscated hostname
        "status": 0,
        "interfaces": obfuscated_interfaces, # Use obfuscated interfaces
        "groups": [{"groupid": str(g['groupid'])} for g in dest_group_ids],
        "description": str(source_host_config.get('description', ''))
    }

    for iface in params['interfaces']:
        if not iface['ip'] and not iface['dns']:
            raise ValueError("Interface must have either IP or DNS set")

    if not params["host"]:
        raise ValueError("Host name cannot be empty")
    if not params["interfaces"]:
        raise ValueError("At least one interface is required")
    if not params["groups"]:
        raise ValueError("At least one group is required")

    try:
        logging.info("Final host.create parameters:")
        logging.info(f"Host: {params['host']}")
        logging.info(f"Groups: {params['groups']}")
        logging.info("Interfaces: " +
            ", ".join(f"type={i['type']} ip={i['ip']}" for i in params['interfaces']))

        logging.info("Creating host with params:")
        import json
        logging.info(json.dumps(params, indent=2))

        result = dest_zapi.host.create(params)
        logging.info(f"API response: {result}")

        if not isinstance(result, dict) or 'error' in result or 'hostids' not in result or not isinstance(result['hostids'], list) or len(result['hostids']) == 0:
             raise ValueError(f"API response error during host creation: {result.get('error', 'Unknown error')}")

        dest_host_id = result['hostids'][0]
        logging.info(f"Successfully created host '{obfuscated_host_name}' on destination with ID: {dest_host_id}")

        # Create items and build mapping
        item_mapping = create_direct_host_items_as_trappers(dest_zapi, dest_host_id, modified_items, source_items_map_by_key)

        # Replicate macros and create triggers for the new host
        # Add the {$SOURCE_HOST_ID} macro to the list of macros to be replicated
        macros_to_replicate = source_host_config.get('macros', [])
        macros_to_replicate.append({"macro": "{$SOURCE_HOST_ID}", "value": source_host_config['hostid']})
        replicate_macros(dest_host_id, macros_to_replicate, dest_zapi)
        create_standard_icmp_triggers(dest_zapi, dest_host_id, obfuscated_host_name)
        create_interface_triggers(dest_zapi, dest_host_id, obfuscated_host_name, all_source_items)
        create_cpu_utilization_trigger_for_host(dest_zapi, dest_host_id, obfuscated_host_name, all_source_items)
        create_temperature_critical_trigger_for_host(dest_zapi, dest_host_id, obfuscated_host_name, all_source_items)

        return dest_host_id, item_mapping, obfuscated_host_name

    except Exception as e:
        logging.error(f"Failed to create host '{obfuscated_host_name}' on destination: {e}", exc_info=True)
        if "already exists" in str(e):
            logging.warning(f"Host '{obfuscated_host_name}' reported as existing during creation attempt. Attempting to find its ID and existing items.")
            existing_host_id, item_mapping = check_and_map_existing_host(obfuscated_host_name, all_source_items, dest_zapi)
            if existing_host_id:
                logging.info(f"Found existing host '{obfuscated_host_name}' with ID: {existing_host_id} after creation failure.")
                # Replicate macros and create triggers for the found existing host
                replicate_macros(existing_host_id, source_host_config.get('macros', []), dest_zapi)
                # Also ensure the SOURCE_HOST_ID macro is set for existing hosts
                replicate_macros(existing_host_id, [{"macro": "{$SOURCE_HOST_ID}", "value": source_host_config['hostid']}], dest_zapi)
                create_standard_icmp_triggers(dest_zapi, existing_host_id, obfuscated_host_name)
                create_interface_triggers(dest_zapi, existing_host_id, obfuscated_host_name, all_source_items)
                create_cpu_utilization_trigger_for_host(dest_zapi, existing_host_id, obfuscated_host_name, all_source_items)
                create_temperature_critical_trigger_for_host(dest_zapi, existing_host_id, obfuscated_host_name, all_source_items)
                return existing_host_id, item_mapping
            else:
                logging.error(f"Host '{obfuscated_host_name}' reported as existing, but could not retrieve its ID after creation failure.")
                raise # Re-raise the original exception
        else:
            logging.error(f"API call failed: {e}")
            if hasattr(e, 'response'):
                logging.error(f"Full error response: {e.response}")
            raise # Re-raise other Zabbix API errors


def get_history_value_type(item_value_type):
    """Maps Zabbix item value_type to history table type for history.get."""
    # Zabbix value types: 0 = numeric float, 1 = character, 2 = log, 3 = numeric unsigned, 4 = text
    mapping = {
        '0': 0, # Numeric float -> history
        '3': 3, # Numeric unsigned -> history_uint
        '1': 1, # Character -> history_str
        '4': 4, # Text -> history_text
        '2': 2, # Log -> history_log
    }
    return mapping.get(str(item_value_type)) # Ensure input is string for lookup

def fetch_history(source_items, source_zapi, time_from=None):
    """Fetches history for the given source items within a specified time range."""
    history_data = {} # Store as {source_itemid: [{clock: ts, value: val}, ...]}
    item_ids_by_type = {} # Group item IDs by history value type

    for item in source_items:
        item_id = item['itemid']
        item_name = item.get('name', '')
        item_key = item.get('key_', '')

        # Check if item should be skipped based on predefined rules
        if should_skip_item(item_name, item_key):
            continue

        value_type = item['value_type']
        history_type = get_history_value_type(value_type)

        if history_type is None:
            logging.warning(f"Item '{item_name}' (ID: {item_id}, Key: {item_key}) has value type {value_type} which cannot be fetched via history.get. Skipping history fetch.")
            continue

        if history_type not in item_ids_by_type:
            item_ids_by_type[history_type] = []
        item_ids_by_type[history_type].append(item_id)
        history_data[item_id] = [] # Initialize list for this item's history

    time_till = int(time.time())
    if time_from is None:
        # Default to last 2 hours if no time_from is provided (for initial fetch)
        time_from = time_till - (12 * 60 * 60)

    # Fetch history for each type
    for history_type, item_ids in item_ids_by_type.items():
        try:
            history_result = source_zapi.history.get(
                output="extend",
                history=history_type,
                itemids=item_ids,
                time_from=time_from,
                time_till=time_till,
                sortfield="clock",
                sortorder="ASC"
            )

            # Organize history by itemid
            for record in history_result:
                item_id = record['itemid']
                if item_id in history_data:
                     # Store only clock and value, convert clock to int
                    history_data[item_id].append({
                        "clock": int(record['clock']),
                        "value": record['value']
                    })

        except Exception as e:
            logging.error(f"Error fetching history for item IDs {item_ids}: {e}", exc_info=True)
            # Handle specific Zabbix API errors if needed
            # For now, just log and continue
            continue

    total_records = sum(len(v) for v in history_data.values())
    return history_data


# --- Flask Routes ---

@app.route('/')
def index():
    """Serves the main HTML page."""
    return render_template('index.html')

# API endpoint to receive and store Zabbix configuration
@app.route('/api/config', methods=['POST'])
def configure_zabbix():
    """Receives Zabbix connection details from the frontend."""
    global config
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Check if we have either form values or .env defaults for each required field
        required_keys = ["source_url", "source_token", "dest_url", "dest_token", "dest_trapper_host", "dest_trapper_port"]
        missing_fields = []

        for key in required_keys:
            # Check if field is provided in request or exists in config (from .env)
            if not data.get(key) and not config.get(key):
                missing_fields.append(key)

        if missing_fields:
            # Check if we have all required fields from either source (form or .env)
            has_form_values = all(data.get(key) for key in required_keys)
            has_env_values = all(config.get(key) for key in required_keys)

            if not (has_form_values or has_env_values):
                return jsonify({
                    "error": "Missing required configuration fields",
                    "missing": missing_fields,
                    "message": f"Please provide these fields or set them in .env: {', '.join(missing_fields)}",
                    "has_form_values": has_form_values,
                    "has_env_values": has_env_values
                }), 400

        # Only update config values that aren't already set from .env
        new_config = {
            "source_url": data.get("source_url") or config["source_url"],
            "source_token": data.get("source_token") or config["source_token"],
            "dest_url": data.get("dest_url") or config["dest_url"],
            "dest_token": data.get("dest_token") or config["dest_token"],
            "dest_trapper_host": data.get("dest_trapper_host") or config["dest_trapper_host"],
            "dest_trapper_port": int(data.get("dest_trapper_port", config["dest_trapper_port"])) # Ensure port is int
        }
        config.update(new_config)
        # Optionally, test connection here before confirming success
        return jsonify({"message": "Configuration saved successfully."})

    except Exception as e:
        logging.error(f"Error processing configuration: {e}", exc_info=True)
        return jsonify({"error": f"Internal server error: {e}"}), 500

@app.route('/api/source/hosts', methods=['GET'])
def get_source_hosts():
    """Connects to the source Zabbix and retrieves a list of hosts."""
    # Check for token config
    if not config.get("source_url") or not config.get("source_token"):
        return jsonify({"error": "Source Zabbix is not fully configured (URL/Token)."}), 400

    try:
        #logging.info(f"Connecting to source Zabbix API at {config['source_url']}")
        # Use url keyword and login with token
        zapi = ZabbixAPI(url=config["source_url"], skip_version_check=True)
        zapi.login(token=config["source_token"])

        # Fetch hosts - only need hostid and name for the dropdown
        hosts = zapi.host.get(output=["hostid", "name"], selectInterfaces=["interfaceid", "ip", "dns", "port", "type"]) # Get interfaces too for later use
        # No logout needed for token auth
        # Sort hosts by name for better UI presentation
        hosts_sorted = sorted(hosts, key=lambda h: h['name'])

        return jsonify(hosts_sorted)

    # Catch generic Exception, check message for Zabbix specifics if needed
    except Exception as e:
        # Check if it looks like a Zabbix API error based on common patterns (e.g., response structure)
        # This is less precise than catching a specific exception type.
        logging.error(f"Error while fetching source hosts: {e}", exc_info=True)
        # Return a generic error, or try to parse 'e' if it contains useful info
        return jsonify({"error": f"Failed to fetch source hosts: {e}"}), 500


@app.route('/api/replicate', methods=['POST'])
def replicate_host():
    """Initiates the replication process for a selected host."""
    # Use a database session
    db = SessionLocal()
    try:
        # Check for full configuration including tokens
        if not all(config.get(k) for k in ["source_url", "source_token", "dest_url", "dest_token"]):
            return jsonify({"error": "Zabbix source or destination is not fully configured (URL/Token)."}), 400

        data = request.get_json()
        source_host_id = data.get('hostid')
        if not source_host_id:
            return jsonify({"error": "Missing 'hostid' in request."}), 400

        # Check if a task for this host already exists in the database and is not failed
        existing_task = db.query(ReplicationTask).filter(ReplicationTask.source_host_id == source_host_id).first()
        if existing_task and existing_task.status != 'failed':
             return jsonify({"message": f"Replication for host ID {source_host_id} is already in progress or completed."}), 202 # Accepted

        # Create or update the task in the database
        if existing_task:
            task = existing_task
            task.status = "starting"
            task.message = "Initiating replication..."
            task.start_time = time.time()
            task.cycle_offset = 0 # Reset offset on restart/re-initiation
            task.last_sent_index = {} # Reset index on restart/re-initiation
            task.progress = 0.0 # Reset progress
            # Store current Zabbix API config in the task
            task.source_url = config["source_url"]
            task.source_token = config["source_token"]
            task.dest_url = config["dest_url"]
            task.dest_token = config["dest_token"]
        else:
            task = ReplicationTask(
                source_host_id=source_host_id,
                status="starting",
                message="Initiating replication...",
                start_time=time.time(),
                cycle_offset=0,
                last_sent_index={},
                progress=0.0,
                # Store current Zabbix API config in the task
                source_url=config["source_url"],
                source_token=config["source_token"],
                dest_url=config["dest_url"],
                dest_token=config["dest_token"]
            )
            db.add(task)
        db.commit()
        db.refresh(task) # Refresh to get the latest state from the database

        try:
            # Connect to Source Zabbix using task-specific config
            source_zapi = ZabbixAPI(url=task.source_url, skip_version_check=True)
            source_zapi.login(token=task.source_token)

            # Connect to Destination Zabbix using task-specific config
            dest_zapi = ZabbixAPI(url=task.dest_url, skip_version_check=True)
            dest_zapi.login(token=task.dest_token)

            # --- 1. Get Source Host Configuration ---
            task.status = 'fetching_source_config'
            task.message = 'Fetching source host configuration...'
            db.commit()
            #logging.info(f"Fetching full configuration for source host ID: {source_host_id}")
            source_hosts = source_zapi.host.get(
                hostids=source_host_id,
                selectGroups=['groupid', 'name'],
                selectParentTemplates=['templateid', 'name'],
                selectInterfaces=['type', 'main', 'useip', 'ip', 'dns', 'port'],
                selectItems=['itemid', 'name', 'key_', 'type', 'value_type', 'delay', 'history', 'trends', 'units', 'description', 'status', 'flags'], # Add more fields as needed
                selectMacros='extend', # Fetch host macros
                # selectDiscoveryRule=True, # Maybe later
                # selectHostPrototypes=True # Maybe later
            )

            if not source_hosts:
                raise ValueError(f"Source host with ID {source_host_id} not found.")
            source_host_config = source_hosts[0]
            host_items = source_host_config.get('items', [])
            source_host_macros = source_host_config.get('macros', []) # Get host macros
            logging.info(f"Successfully fetched config for source host: {source_host_config['name']} ({len(host_items)} direct items, {len(source_host_macros)} host macros)")

            # --- Fetch Template Items and Macros ---
            template_items = []
            source_template_macros = [] # To store macros from templates
            template_ids = [t['templateid'] for t in source_host_config.get('parentTemplates', [])]
            if template_ids:
                logging.info(f"Fetching items and macros from {len(template_ids)} source templates...")
                try:
                    # Fetch items belonging to these templates directly
                    template_data = source_zapi.template.get(
                        templateids=template_ids,
                        output=['templateid', 'name'],
                        selectItems=['itemid', 'name', 'key_', 'type', 'value_type', 'delay', 'history', 'trends', 'units', 'description', 'status', 'flags'],
                        selectMacros='extend' # Fetch template macros
                    )
                    for template in template_data:
                        template_items.extend(template.get('items', []))
                        source_template_macros.extend(template.get('macros', []))
                    logging.info(f"Fetched {len(template_items)} items and {len(source_template_macros)} macros from templates.")
                except Exception as e:
                    logging.warning(f"Could not fetch items or macros from source templates: {e}")

            # Combine host and template items, ensuring uniqueness by itemid
            all_source_items_dict = {item['itemid']: item for item in host_items}
            for item in template_items:
                if item['itemid'] not in all_source_items_dict:
                    all_source_items_dict[item['itemid']] = item
            all_source_items = list(all_source_items_dict.values())
            logging.info(f"Total unique source items (host + template): {len(all_source_items)}")

            # Combine host and template macros, prioritizing host macros in case of duplicates
            all_source_macros_dict = {macro['macro']: macro for macro in source_template_macros}
            for macro in source_host_macros:
                 all_source_macros_dict[macro['macro']] = macro # Host macros overwrite template macros
            all_source_macros = list(all_source_macros_dict.values())
            logging.info(f"Total unique source macros (host + template): {len(all_source_macros)}")


            # --- 3. Map Group/Template IDs from Source Names to Destination IDs ---
            task.status = 'mapping_ids'
            task.message = 'Mapping source groups/templates to destination...'
            db.commit()
            #logging.info(f"Mapping groups for host {source_host_config['name']}...")
            dest_group_ids = map_entities_by_name(
                source_entities=source_host_config.get('groups', []),
                dest_entities_func=lambda: dest_zapi.hostgroup.get(output=['groupid', 'name']),
                entity_name_key='name',
                entity_id_key='groupid',
                entity_type='group',
                zapi=dest_zapi # Pass dest_zapi to allow creation in map_entities_by_name
            )

            # Ensure 'clonedfordemo' group exists and add it to the list
            cloned_group_name = 'clonedfordemo'
            cloned_group = dest_zapi.hostgroup.get(filter={"name": cloned_group_name}, output=['groupid'])
            cloned_group_id = None

            if cloned_group:
                cloned_group_id = cloned_group[0]['groupid']
                logging.info(f"Found existing group '{cloned_group_name}' with ID: {cloned_group_id}")
            else:
                try:
                    new_group = dest_zapi.hostgroup.create({"name": cloned_group_name})
                    cloned_group_id = new_group['groupids'][0]
                    logging.info(f"Created missing group '{cloned_group_name}' with ID: {cloned_group_id}")
                except Exception as e:
                    logging.error(f"Failed to create group '{cloned_group_name}': {e}")
                    # Decide how to handle this failure - for now, just log and continue without this group

            # Add the 'clonedfordemo' group ID to the list if it exists or was created
            if cloned_group_id:
                # Check if the group is already in the list (from source mapping)
                if not any(g['groupid'] == cloned_group_id for g in dest_group_ids):
                    dest_group_ids.append({"groupid": cloned_group_id})
                    logging.info(f"Added group '{cloned_group_name}' (ID: {cloned_group_id}) to destination groups.")
                else:
                    logging.info(f"Group '{cloned_group_name}' (ID: {cloned_group_id}) was already in the list from source mapping.")


            if not dest_group_ids:
                # Fallback: Add to default 'Zabbix servers' group if no source groups were mapped
                default_groups = dest_zapi.hostgroup.get(filter={"name": "Zabbix servers"}, output=['groupid'])
                if not default_groups:
                    # Create default group if it doesn't exist
                    default_group = dest_zapi.hostgroup.create({"name": "Zabbix servers"})
                    dest_group_ids = [{"groupid": default_group['groupids'][0]}]
                else:
                    dest_group_ids = [{"groupid": default_groups[0]['groupid']}]
                logging.warning(f"Using fallback group 'Zabbix servers' for host {source_host_config['name']}")

            dest_template_ids = map_entities_by_name(
                source_entities=source_host_config.get('parentTemplates', []),
                dest_entities_func=lambda: dest_zapi.template.get(output=['templateid', 'name']),
                entity_name_key='name',
                entity_id_key='templateid',
                entity_type='template'
            )

            # --- 4. Modify DIRECT HOST Items to Trapper Type ---
            # We only modify direct host items, not template items.
            task.status = 'modifying_items'
            task.message = 'Modifying direct host items to trapper type...'
            db.commit()
            logging.info(f"Modifying {len(host_items)} direct host items for {source_host_config['name']} to trapper type...")
            modified_items = modify_items_to_trapper(host_items) # Use host_items here
            if not modified_items:
                logging.warning(f"No suitable direct host items found or modified for host {source_host_config['name']}. Only template items might exist on destination.")

            # --- 5. Create Host on Destination ---
            task.status = 'creating_dest_host'
            task.message = 'Creating host on destination Zabbix...'
            db.commit()
            #logging.info(f"Creating host on destination Zabbix: Parameters: {source_host_config}, Groups: {dest_group_ids}, Templates: {dest_template_ids,}, Items: {modified_items}")

            dest_host_id = None
            item_mapping = {}

            try:
                # Pass the combined list 'all_source_items' to create_destination_host for mapping purposes
                dest_host_id, item_mapping, actual_dest_host_name = create_destination_host(
                    source_host_config,
                    all_source_items, # Pass the combined list here
                    dest_group_ids,
                    dest_template_ids,
                    modified_items, # Still pass only modified direct items for creation
                    dest_zapi
                )
                if not dest_host_id:
                    logging.error("create_destination_host returned no host ID.")
                    task.status = 'failed'
                    task.message = "Host creation failed: No host ID returned."
                    db.commit()
                    # Do not raise exception here, allow comparison to run
                else:
                    # Store the actual destination host ID and item mapping in the task object
                    task.dest_host_id = dest_host_id
                    task.dest_host_name = actual_dest_host_name # Store the obfuscated name for sender
                    task.item_mapping = item_mapping # Store the initial item mapping
                    db.commit()
                    logging.info(f"Successfully created host with ID: {dest_host_id} and stored initial item mapping ({len(item_mapping)} items).")

                    # --- Automatically Rebuild Mapping ---
                    logging.info(f"Automatically rebuilding item mapping for new host {source_host_id}...")
                    rebuild_success, rebuild_message = perform_mapping_rebuild(source_host_id, dest_host_id, source_zapi, dest_zapi, db)
                    if rebuild_success:
                        logging.info(f"Automatic mapping rebuild successful: {rebuild_message}")
                        # The perform_mapping_rebuild function commits the changes to the DB
                    else:
                        logging.error(f"Automatic mapping rebuild failed: {rebuild_message}")
                        # Decide how to handle failure here - maybe set task status to warning?
                        # For now, just log the error. The task status remains 'creating_dest_host' or moves on.
                        # If the initial creation succeeded, we don't want to mark the whole task as failed just for rebuild failure.


            except Exception as e:
                task.status = 'failed'
                task.message = f"Host creation failed: {e}"
                db.commit()
                logging.error(f"Failed during destination host creation/mapping: {e}", exc_info=True)
                # Set status to failed and do not attempt further mapping here,
                # as create_destination_host already handles existing hosts.
                dest_host_id = None
                item_mapping = {}
                # Ensure the potentially incorrect mapping isn't stored
                task.item_mapping = {}
                db.commit()


            # --- Diagnostic section removed ---

            # --- 6. Fetch Source History (Last 24 hours) ---
            # or if host creation failed.
            task.status = 'comparing_items'
            task.message = 'Comparing source and destination items...'
            db.commit()

            if dest_host_id: # Only compare if destination host ID was obtained
                source_items_full = source_zapi.item.get(hostids=source_host_id, output=['itemid', 'name', 'key_'])
                source_items_map_by_key = {item['key_']: item['itemid'] for item in source_items_full}
                source_items_map_by_id = {item['itemid']: item for item in source_items_full}

                dest_items_full = dest_zapi.item.get(hostids=dest_host_id, output=['itemid', 'name', 'key_'])
                dest_items_map_by_key = {item['key_']: item for item in dest_items_full}

                missing_on_destination = []
                key_mismatches = []

                for source_item in source_items_full:
                    source_item_id = source_item['itemid']
                    source_item_key = source_item['key_']
                    source_item_name = source_item['name']

                    if source_item_key not in dest_items_map_by_key:
                        missing_on_destination.append(f"Source ID: {source_item_id}, Key: {source_item_key}, Name: {source_item_name}")
                    else:
                        # Optional: Check for name mismatch if keys match
                        dest_item = dest_items_map_by_key[source_item_key]
                        if dest_item['name'] != source_item_name:
                             key_mismatches.append(f"Source ID: {source_item_id}, Key: {source_item_key}, Source Name: {source_item_name}, Dest Name: {dest_item['name']}")


                logging.info("--- Item Comparison Results ---")
                if missing_on_destination:
                    logging.warning(f"The following {len(missing_on_destination)} source items were not found on the destination by key:")
                    for item_info in missing_on_destination:
                        logging.warning(f"- {item_info}")
                else:
                    logging.info("All source items found on destination by key.")

                if key_mismatches:
                    logging.warning(f"The following {len(key_mismatches)} items have matching keys but different names:")
                    for item_info in key_mismatches:
                        logging.warning(f"- {item_info}")

            else:
                logging.warning("Skipping item comparison as destination host ID was not obtained.")
            # --- End Diagnostic ---


            # --- 6. Fetch Source History (Last 24 hours) ---
            task.status = 'fetching_history'
            task.message = 'Fetching source item history (last 24h)...'
            db.commit()
            logging.info(f"Fetching history for {len(all_source_items)} total items (host + template) of host {source_host_config['name']}...")
            # Pass the combined list 'all_source_items' to fetch history for all relevant items
            history_data = fetch_history(all_source_items, source_zapi)

            # --- 7. Store History ---
            task.status = 'storing_data'
            task.message = 'Storing history data...'
            db.commit()
            task.history = history_data # Store history in the task object
            db.commit()

            # --- 9. Start Background Replay Task ---
            # Only schedule replay if host creation and mapping were successful
            if dest_host_id and item_mapping:
                task.status = 'scheduling_replay'
                task.message = 'Scheduling data replay task...'
                db.commit()
                # Find the earliest timestamp in the history to set the relative start
                first_ts = None
                for item_id, points in history_data.items():
                    if points:
                        ts = points[0]['clock']
                        if first_ts is None or ts < first_ts:
                            first_ts = ts
                task.first_history_timestamp = first_ts or int(time.time()) # Fallback if no history
                db.commit()

                try:
                    # Schedule job to run e.g., every 60 seconds
                    scheduler.add_job(
                        replay_job,
                        trigger='interval',
                        seconds=60, # Adjust interval as needed
                        id=f"replay_{source_host_id}", # Explicitly set the job ID
                        args=[source_host_id], # Pass source_host_id to job
                        replace_existing=True # Replace if somehow already exists
                    )
                    logging.info(f"Successfully scheduled replay job with ID: replay_{source_host_id}")
                except Exception as sched_e:
                     logging.error(f"Failed to schedule replay job for {source_host_id}: {sched_e}", exc_info=True)
                     # This is a critical failure, update task status
                     task.status = 'failed'
                     task.message = f"Failed to schedule replay job: {sched_e}"
                     db.commit()
                     # Do not raise here, let the function return the current status

                if task.status != 'failed':
                    task.status = 'replicating'
                    task.message = 'Replication setup complete. Replay task scheduled.'
                    db.commit()
                    logging.info(f"Replication setup complete for source host ID: {source_host_id}")
            else:
                # If host creation or mapping failed, the task status is already set to 'failed'
                logging.warning(f"Skipping replay job scheduling for source host {source_host_id} due to previous failure.")


            # No logout needed for token auth

            # Return the current status, which might be 'replicating' or 'failed'
            return jsonify({
                "message": f"Replication process initiated for host ID {source_host_id}.",
                "destination_host_id": task.dest_host_id,
                "status": task.status,
                "details": task.message
            })

        # Catch generic Exception, check message for Zabbix specifics if needed
        except Exception as e:
            logging.error(f"Error during replication for host {source_host_id}: {e}", exc_info=True)
            task.status = 'failed'
            # Try to provide a slightly more specific message if possible
            task.message = f"Replication Error: {e}"
            db.commit()
            # No logout needed for token auth
            return jsonify({"error": f"Replication failed: {e}"}), 500
    finally:
        db.close() # Ensure the session is closed

@app.route('/api/replay/status', methods=['GET'])
def get_replay_status():
    """Returns the status of ongoing replication tasks."""
    db = SessionLocal()
    try:
        # Can optionally filter by hostid if provided as a query parameter
        host_id_filter = request.args.get('hostid')

        if host_id_filter:
            task = db.query(ReplicationTask).filter(ReplicationTask.source_host_id == host_id_filter).first()
            if task:
                # Return task details as a dictionary
                return jsonify({
                    task.source_host_id: {
                        "dest_host_id": task.dest_host_id,
                        "dest_host_name": task.dest_host_name,
                        "status": task.status,
                        "message": task.message,
                        "start_time": task.start_time,
                        "first_history_timestamp": task.first_history_timestamp,
                        "item_mapping": task.item_mapping,
                        "last_sent_index": task.last_sent_index,
                        "cycle_offset": task.cycle_offset,
                        "progress": task.progress
                        # Do NOT return the full history data here, it can be very large
                    }
                })
            else:
                return jsonify({"error": f"No replication task found for host ID {host_id_filter}"}), 404
        else:
            # Return status for all known tasks from the database
            tasks = db.query(ReplicationTask).all()
            status_dict = {}
            for task in tasks:
                 status_dict[task.source_host_id] = {
                    "dest_host_id": task.dest_host_id,
                    "dest_host_name": task.dest_host_name,
                    "status": task.status,
                    "message": task.message,
                    "start_time": task.start_time,
                    "first_history_timestamp": task.first_history_timestamp,
                    "item_mapping": task.item_mapping,
                    "last_sent_index": task.last_sent_index,
                    "cycle_offset": task.cycle_offset,
                    "progress": task.progress
                    # Do NOT return the full history data here
                 }
            return jsonify(status_dict)
    finally:
        db.close() # Ensure the session is closed

# Function to load tasks from the database on startup
def load_replication_tasks_from_db():
    db = SessionLocal()
    try:
        tasks = db.query(ReplicationTask).all()
        for task in tasks:
            # Populate the in-memory dictionary with task objects from the database
            # This allows the replay_job to access task details easily
            replication_tasks[task.source_host_id] = task
            logging.info(f"Loaded task for source host {task.source_host_id} from database.")
    except Exception as e:
        logging.error(f"Error loading replication tasks from database: {e}", exc_info=True)
    finally:
        db.close()

# Load tasks from the database when the application starts
load_replication_tasks_from_db()

# Start the scheduler
scheduler.start()
logging.info("APScheduler started. Current jobs:")
scheduler.print_jobs() # Log the jobs known to the scheduler instance

# Ensure scheduler shuts down gracefully
import atexit
atexit.register(lambda: scheduler.shutdown())




@app.route('/api/rebuild_mapping', methods=['POST'])
def rebuild_mapping_endpoint():
    """Endpoint to manually rebuild the item mapping for a specific replication task."""
    db = SessionLocal()
    try:
        data = request.get_json()
        source_host_id = data.get('source_host_id')
        if not source_host_id:
            return jsonify({"error": "Missing 'source_host_id' in request."}), 400

        task = db.query(ReplicationTask).filter(ReplicationTask.source_host_id == source_host_id).first()
        if not task:
            return jsonify({"error": f"No replication task found for source host ID {source_host_id}"}), 404

        if not task:
            return jsonify({"error": f"No replication task found for source host ID {source_host_id}"}), 404

        # Use task-specific configuration for Zabbix API connections
        if not all([task.source_url, task.source_token, task.dest_url, task.dest_token]):
            return jsonify({"error": f"Replication task {source_host_id} is missing Zabbix API configuration."}), 400

        try:
            # Connect to Source Zabbix using task-specific config
            source_zapi = ZabbixAPI(url=task.source_url, skip_version_check=True)
            source_zapi.login(token=task.source_token)

            # Connect to Destination Zabbix using task-specific config
            dest_zapi = ZabbixAPI(url=task.dest_url, skip_version_check=True)
            dest_zapi.login(token=task.dest_token)

            success, message = perform_mapping_rebuild(source_host_id, task.dest_host_id, source_zapi, dest_zapi, db)

            if success:
                return jsonify({"message": message})
            else:
                return jsonify({"error": message}), 500

        except Exception as e:
            logging.error(f"Error in rebuild_mapping endpoint for host {source_host_id}: {e}", exc_info=True)
            return jsonify({"error": f"Internal server error: {e}"}), 500
    finally:
        db.close()

@app.route('/api/orphaned_hosts', methods=['GET'])
def get_orphaned_hosts():
    """
    Identifies and returns a list of destination hosts in the 'clonedfordemo' group
    that do not have a corresponding ReplicationTask in the database.
    """
    db = SessionLocal()
    try:
        # Use global config for orphaned host identification, as tasks don't exist yet
        if not all(config.get(k) for k in ["dest_url", "dest_token"]):
            return jsonify({"error": "Destination Zabbix is not fully configured (URL/Token) in global config."}), 400

        dest_zapi = ZabbixAPI(url=config["dest_url"], skip_version_check=True)
        dest_zapi.login(token=config["dest_token"])

        # 1. Find 'clonedfordemo' group ID
        cloned_group = dest_zapi.hostgroup.get(filter={"name": CLONED_GROUP_NAME}, output=['groupid'])
        if not cloned_group:
            return jsonify({"message": f"No '{CLONED_GROUP_NAME}' group found on destination Zabbix."}), 200
        cloned_group_id = cloned_group[0]['groupid']

        # 2. List all hosts in 'clonedfordemo' group
        cloned_dest_hosts = dest_zapi.host.get(
            groupids=cloned_group_id,
            output=['hostid', 'name'],
            selectMacros=['macro', 'value'] # Fetch macros to find {$SOURCE_HOST_ID}
        )
        logging.info(f"Found {len(cloned_dest_hosts)} hosts in '{CLONED_GROUP_NAME}' group.")

        orphaned_hosts_info = []
        for dest_host in cloned_dest_hosts:
            dest_host_id = dest_host['hostid']
            dest_host_name = dest_host['name']
            source_host_id_from_macro = None

            # Try to find {$SOURCE_HOST_ID} macro
            for macro in dest_host.get('macros', []):
                if macro['macro'] == '{$SOURCE_HOST_ID}':
                    source_host_id_from_macro = macro['value']
                    break

            # Check if a ReplicationTask exists for this dest_host_id
            # We need to check against both source_host_id and dest_host_id in the task
            # If source_host_id_from_macro is available, we can use it to check for existing tasks
            existing_task = None
            if source_host_id_from_macro:
                existing_task = db.query(ReplicationTask).filter(
                    (ReplicationTask.source_host_id == source_host_id_from_macro) |
                    (ReplicationTask.dest_host_id == dest_host_id)
                ).first()
            else:
                existing_task = db.query(ReplicationTask).filter(ReplicationTask.dest_host_id == dest_host_id).first()


            if not existing_task:
                orphaned_hosts_info.append({
                    "dest_host_id": dest_host_id,
                    "dest_host_name": dest_host_name,
                    "source_host_id_hint": source_host_id_from_macro # Provide hint if macro exists
                })
                logging.info(f"Identified orphaned host: {dest_host_name} (ID: {dest_host_id})")
            else:
                logging.info(f"Host {dest_host_name} (ID: {dest_host_id}) has an existing replication task (Source ID: {existing_task.source_host_id}).")

        return jsonify(orphaned_hosts_info)

    except Exception as e:
        logging.error(f"Error identifying orphaned hosts: {e}", exc_info=True)
        return jsonify({"error": f"Internal server error: {e}"}), 500
    finally:
        db.close()

@app.route('/api/relink_host', methods=['POST'])
def relink_host():
    """
    Re-establishes a replication task for an orphaned destination host
    by linking it to a provided source host ID.
    """
    db = SessionLocal()
    try:
        data = request.get_json()
        dest_host_id = data.get('dest_host_id')
        source_host_id = data.get('source_host_id')

        if not dest_host_id or not source_host_id:
            return jsonify({"error": "Missing 'dest_host_id' or 'source_host_id' in request."}), 400

        # Check if global config is available for initial connection
        if not all(config.get(k) for k in ["source_url", "source_token", "dest_url", "dest_token"]):
            return jsonify({"error": "Zabbix source or destination is not fully configured (URL/Token) in global config."}), 400

        # Connect to Zabbix APIs using global config for initial verification
        source_zapi = ZabbixAPI(url=config["source_url"], skip_version_check=True)
        source_zapi.login(token=config["source_token"])
        dest_zapi = ZabbixAPI(url=config["dest_url"], skip_version_check=True)
        dest_zapi.login(token=config["dest_token"])

        # 1. Verify source host exists
        source_hosts = source_zapi.host.get(hostids=source_host_id, output=['hostid', 'name'])
        if not source_hosts:
            return jsonify({"error": f"Source host with ID {source_host_id} not found."}), 404
        source_host_name = source_hosts[0]['name']

        # 2. Verify destination host exists and get its name
        dest_hosts = dest_zapi.host.get(hostids=dest_host_id, output=['hostid', 'name'])
        if not dest_hosts:
            return jsonify({"error": f"Destination host with ID {dest_host_id} not found."}), 404
        dest_host_name = dest_hosts[0]['name']

        # 3. Check for existing task for this source_host_id (to prevent duplicates)
        existing_task = db.query(ReplicationTask).filter(ReplicationTask.source_host_id == source_host_id).first()
        if existing_task:
            if existing_task.dest_host_id == dest_host_id:
                return jsonify({"message": f"Replication task for source host ID {source_host_id} already exists and is linked to destination host ID {dest_host_id}. Re-scheduling replay if needed."}), 200
            else:
                return jsonify({"error": f"Source host ID {source_host_id} is already linked to a different destination host ID {existing_task.dest_host_id}. Please resolve this conflict manually."}), 409

        # 4. Create a new ReplicationTask
        task = ReplicationTask(
            source_host_id=source_host_id,
            dest_host_id=dest_host_id,
            dest_host_name=dest_host_name,
            status="relinking",
            message=f"Re-linking to source host {source_host_name}...",
            start_time=time.time(),
            cycle_offset=0,
            last_sent_index={},
            progress=0.0,
            # Store current Zabbix API config in the task
            source_url=config["source_url"],
            source_token=config["source_token"],
            dest_url=config["dest_url"],
            dest_token=config["dest_token"]
        )
        db.add(task)
        db.commit()
        db.refresh(task)
        logging.info(f"Created new ReplicationTask for source {source_host_id} -> dest {dest_host_id}.")

        # 5. Fetch all source items (direct and template-inherited)
        source_host_config = source_zapi.host.get(
            hostids=source_host_id,
            selectItems=['itemid', 'name', 'key_', 'type', 'value_type', 'delay', 'history', 'trends', 'units', 'description', 'status', 'flags'],
            selectParentTemplates=['templateid']
        )[0]
        host_items = source_host_config.get('items', [])
        template_items = []
        template_ids = [t['templateid'] for t in source_host_config.get('parentTemplates', [])]
        if template_ids:
            template_data = source_zapi.template.get(
                templateids=template_ids,
                output=['templateid'],
                selectItems=['itemid', 'name', 'key_', 'type', 'value_type', 'delay', 'history', 'trends', 'units', 'description', 'status', 'flags']
            )
            for template in template_data:
                template_items.extend(template.get('items', []))

        all_source_items_dict = {item['itemid']: item for item in host_items}
        for item in template_items:
            if item['itemid'] not in all_source_items_dict:
                all_source_items_dict[item['itemid']] = item
        all_source_items = list(all_source_items_dict.values())
        logging.info(f"Fetched {len(all_source_items)} total source items for re-linking.")

        # 6. Rebuild item mapping
        task.status = 'rebuilding_mapping'
        task.message = 'Rebuilding item mapping...'
        db.commit()
        rebuild_success, rebuild_message = perform_mapping_rebuild(source_host_id, dest_host_id, source_zapi, dest_zapi, db)
        if not rebuild_success:
            task.status = 'failed'
            task.message = f"Re-linking failed during mapping rebuild: {rebuild_message}"
            db.commit()
            return jsonify({"error": f"Failed to rebuild item mapping: {rebuild_message}"}), 500
        logging.info(f"Item mapping rebuilt successfully: {rebuild_message}")

        # 7. Fetch recent history
        task.status = 'fetching_history'
        task.message = 'Fetching recent history...'
        db.commit()
        replay_duration_hours = config.get("replay_duration_hours", 24)
        time_from = int(time.time()) - (replay_duration_hours * 3600)
        history_data = fetch_history(all_source_items, source_zapi, time_from=time_from)
        task.history = history_data
        task.first_history_timestamp = time_from # Set start of history fetch as first timestamp
        db.commit()
        logging.info(f"Fetched {sum(len(v) for v in history_data.values())} history records for re-linking.")

        # 8. Schedule replay job
        task.status = 'scheduling_replay'
        task.message = 'Scheduling data replay task...'
        db.commit()
        scheduler.add_job(
            replay_job,
            trigger='interval',
            seconds=60,
            id=f"replay_{source_host_id}",
            args=[source_host_id],
            replace_existing=True
        )
        logging.info(f"Successfully scheduled replay job for source host ID: {source_host_id}")

        task.status = 'replicating'
        task.message = 'Re-linking complete. Replay task scheduled.'
        db.commit()

        return jsonify({
            "message": f"Replication re-established for destination host ID {dest_host_id} linked to source host ID {source_host_id}.",
            "status": task.status,
            "details": task.message
        })

    except Exception as e:
        logging.error(f"Error during re-linking host {dest_host_id} to {source_host_id}: {e}", exc_info=True)
        if 'task' in locals() and task:
            task.status = 'failed'
            task.message = f"Re-linking failed: {e}"
            db.commit()
        return jsonify({"error": f"Re-linking failed: {e}"}), 500
    finally:
        db.close()


if __name__ == '__main__':
    # Note: Using Flask's development server is not suitable for production.
    # Consider using a proper WSGI server like Gunicorn or uWSGI.
    logging.info("Starting Flask development server.")
    # Use use_reloader=False to prevent scheduler from starting twice in debug mode
    app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
