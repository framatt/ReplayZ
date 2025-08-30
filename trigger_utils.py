import logging
import json
import random
import time
import functools

from common import ReplicationTask

# Dictionary to store active problems and their remaining duration
# Key: (host_id, item_key), Value: remaining_cycles
active_problems = {}

def perform_mapping_rebuild(source_host_id, dest_host_id, source_zapi, dest_zapi, db):
    """Performs the core item mapping rebuild logic."""
    try:
        # Fetch all source items for the host (including template items)
        source_items_full = source_zapi.item.get(hostids=source_host_id, output=['itemid', 'name', 'key_'])
        source_items_map_by_key = {item['key_']: item['itemid'] for item in source_items_full}
        source_items_map_by_id = {item['itemid']: item for item in source_items_full}

        # Fetch all destination items for the destination host
        dest_items_full = dest_zapi.item.get(hostids=dest_host_id, output=['itemid', 'name', 'key_'])
        dest_items_map_by_key = {item['key_']: item['itemid'] for item in dest_items_full}

        # Rebuild the item mapping
        new_item_mapping = {}
        for source_itemid, source_item in source_items_map_by_id.items():
            source_key = source_item['key_']
            if source_key in dest_items_map_by_key:
                new_item_mapping[str(source_itemid)] = source_key # Map source ID (as string) to destination key
                logging.debug(f"Rebuilt mapping: Source ID {source_itemid} -> Dest Key '{source_key}'")
            else:
                logging.warning(f"Source item with key '{source_key}' (ID: {source_itemid}) not found on destination host {dest_host_id} during mapping rebuild. Skipping.")

        # Update the task's item_mapping in the database
        task = db.query(ReplicationTask).filter(ReplicationTask.source_host_id == source_host_id).first()
        if task:
            task.item_mapping = new_item_mapping
            db.commit()
            logging.info(f"Successfully updated item mapping in DB for source host ID {source_host_id}.")
        else:
            logging.error(f"Task not found for source host ID {source_host_id} during mapping rebuild update.")


        logging.info(f"Item mapping rebuild logic completed for source host ID {source_host_id}.")
        return True, "Mapping rebuild successful."

    except Exception as e:
        logging.error(f"Error during item mapping rebuild for host {source_host_id}: {e}", exc_info=True)
        return False, f"Mapping rebuild failed: {e}"

def simulate_random_problem(host_id, item_key, current_value, problem_probability=0.1, min_duration=2, max_duration=5):
    """
    Simulates a random problem for a given item key based on a probability.
    Problems can persist for multiple cycles.
    Returns a problematic value if simulation occurs, otherwise returns the original value.
    """
    logging.info(f"Simulating problem for item key: {item_key}, current value: {current_value}")
    if item_key not in ["cpu.util", "icmpping", "icmppingloss", "icmppingsec", "sensor.temp", "ifOperStatus", "ifSpeed"]:
        # If the item key is not in the list of monitored keys, return the original value
        return current_value
    logging.info(f"Simulating problem for item key: {item_key}")
    problem_key = (host_id, item_key)
    # Check if a problem is already active for this item key
    if problem_key in active_problems and active_problems[problem_key] > 0:
        logging.info(f"Continuing simulated problem for item key: {item_key}, remaining cycles: {active_problems[problem_key]}")
        active_problems[problem_key] -= 1
        # Return a problematic value based on the item key (re-using existing logic)
        if "cpu.util" in item_key:
            logging.info(f"Simulating high CPU utilization for item key: {item_key}")
            return random.uniform(85, 99)
        elif "icmpping" in item_key and "loss" not in item_key and "sec" not in item_key:
            logging.info(f"Simulating ping failure for item key: {item_key}")
            return 0
        elif "icmppingloss" in item_key:
            logging.info(f"Simulating high ping loss for item key: {item_key}")
            return random.uniform(25, 75)
        elif "icmppingsec" in item_key:
            logging.info(f"Simulating high ping response time for item key: {item_key}")
            return random.uniform(0.2, 1.0)
        elif "sensor.temp" in item_key.lower():
            logging.info(f"Simulating high temperature for item key: {item_key}")
            return random.uniform(55, 80)
        elif "ifOperStatus" in item_key:
            logging.info(f"Simulating interface link down for item key: {item_key}")
            return 2
        elif "ifSpeed" in item_key:
            logging.info(f"Simulating interface speed change for item key: {item_key}")
            return random.uniform(0, 1000000000)
        else:
            # If problem type not specifically handled, return a generic problematic value or current_value
            # For now, returning current_value to avoid unexpected behavior
            return current_value
    elif problem_key in active_problems and active_problems[problem_key] == 0:
        # Problem duration ended
        logging.info(f"Simulated problem ended for item key: {item_key}")
        del active_problems[problem_key]
        return current_value
    else:
        # No active problem, check if a new one should be simulated
        r = random.random()
        #logging.info(f"Random number for simulation: {r}, Problem probability: {problem_probability}")
        if r < problem_probability:
            logging.info(f"Simulating NEW problem for item key: {item_key}")
            duration = random.randint(min_duration, max_duration)
            active_problems[problem_key] = duration - 1 # Decrement for the current cycle
            # Simulate different problems based on item key patterns
            if "cpu.util" in item_key:
                logging.info(f"Simulating high CPU utilization for item key: {item_key}")
                # Simulate high CPU utilization (e.g., > 80)
                return random.uniform(85, 99)
            elif "icmpping" in item_key and "loss" not in item_key and "sec" not in item_key:
                logging.info(f"Simulating ping failure for item key: {item_key}")
                # Simulate ping failure (e.g., 0)
                return 0
            elif "icmppingloss" in item_key:
                logging.info(f"Simulating high ping loss for item key: {item_key}")
                # Simulate high ping loss (e.g., > 20)
                return random.uniform(25, 75)
            elif "icmppingsec" in item_key:
                logging.info(f"Simulating high ping response time for item key: {item_key}")
                # Simulate high ping response time (e.g., > 0.1)
                return random.uniform(0.2, 1.0)
            elif "sensor.temp" in item_key.lower():
                logging.info(f"Simulating high temperature for item key: {item_key}")
                # Simulate high temperature (e.g., > 50)
                return random.uniform(55, 80)
            elif "ifOperStatus" in item_key:
                logging.info(f"Simulating interface link down for item key: {item_key}")
                # Simulate interface link down (e.g., 2)
                return 2
            elif "ifSpeed" in item_key:
                logging.info(f"Simulating interface speed change for item key: {item_key}")
                # Simulate interface speed change (e.g., lower speed)
                return random.uniform(0, 1000000000)
            # Add more conditions for other item types as needed
            else:
                #logging.warning(f"Problem simulation not implemented for item key: {item_key}")
                return current_value # Return original value if not implemented
        else:
            return current_value # Return original value if no simulation

def create_or_update_macro(dest_zapi, host_id, macro_name, macro_value):
    """Creates or updates a host macro on the destination."""
    try:
        existing_macros = dest_zapi.usermacro.get(
            hostids=host_id,
            filter={"macro": macro_name},
            selectMacros="extend", # Need hostmacroid for update
            output=["hostmacroid", "value"] # Get ID and value
        )

        if existing_macros:
            if existing_macros[0]['value'] != macro_value:
                logging.info(f"Updating macro '{macro_name}' for host {host_id} with value '{macro_value}'")
                dest_zapi.usermacro.update({
                    "hostmacroid": existing_macros[0]['hostmacroid'],
                    "value": macro_value
                })
            else:
                logging.debug(f"Macro '{macro_name}' already exists with correct value for host {host_id}.")
            return True
        else:
            logging.info(f"Creating macro '{macro_name}' for host {host_id} with value '{macro_value}'")
            dest_zapi.usermacro.create({
                "hostid": host_id,
                "macro": macro_name,
                "value": macro_value
            })
            return True
    except Exception as e:
        logging.error(f"Failed to create or update macro '{macro_name}' for host {host_id}: {e}", exc_info=True)
        return False


def create_icmp_trigger(dest_zapi, dest_host_id, dest_host_name, icmp_item_key="icmpping"):
    """Creates a standard ICMP ping trigger for a host if it doesn't exist."""
    trigger_description = "Unavailable by ICMP ping"
    trigger_expression = f"max(/{dest_host_name}/{icmp_item_key},#3)=0"

    try:
        items = dest_zapi.item.get(hostids=dest_host_id, filter={"key_": icmp_item_key}, output=["itemid"])
        if not items:
            logging.warning(f"Item with key '{icmp_item_key}' not found on host '{dest_host_name}' (ID: {dest_host_id}). Cannot create ICMP trigger.")
            return False

        existing_triggers = dest_zapi.trigger.get(
            hostids=dest_host_id,
            filter={"description": trigger_description},
            output=["triggerid"]
        )
        if existing_triggers:
            logging.debug(f"Trigger '{trigger_description}' already exists for host '{dest_host_name}'. Skipping creation.")
            return True

        logging.info(f"Creating ICMP trigger '{trigger_description}' for host '{dest_host_name}'...")
        params = {
            "description": trigger_description,
            "expression": trigger_expression,
            "priority": "4",
            "comments": "Last three attempts returned timeout. Please check device connectivity.",
            "status": "0"
        }
        result = dest_zapi.trigger.create(params)
        if result and 'triggerids' in result and result['triggerids']:
            logging.info(f"Successfully created trigger '{trigger_description}' with ID: {result['triggerids'][0]}")
            return True
        else:
            logging.error(f"Failed to create trigger '{trigger_description}'. API Response: {result}")
            return False
    except Exception as e:
        logging.error(f"Error creating trigger '{trigger_description}': {e}", exc_info=True)
        if "already exists" in str(e).lower() or "trigger with the same name" in str(e).lower():
             logging.warning(f"Trigger '{trigger_description}' likely already exists (API error: {e}). Skipping.")
             return True
        return False


def create_icmp_loss_trigger(dest_zapi, dest_host_id, dest_host_name, item_key="icmppingloss"):
    """Creates a High ICMP ping loss trigger if it doesn't exist, ensuring macro exists."""
    trigger_description = "High ICMP ping loss"
    macro_name = "{$ICMP_LOSS_WARN}"
    macro_value = "20"

    try:
        items = dest_zapi.item.get(hostids=dest_host_id, filter={"key_": item_key}, output=["itemid"])
        if not items:
            logging.warning(f"Item '{item_key}' not found on host '{dest_host_name}'. Cannot create '{trigger_description}' trigger.")
            return False

        existing_triggers = dest_zapi.trigger.get(
            hostids=dest_host_id,
            filter={"description": trigger_description},
            output=["triggerid"]
        )
        if existing_triggers:
            logging.debug(f"Trigger '{trigger_description}' already exists for host '{dest_host_name}'. Skipping creation.")
            return True

        if not create_or_update_macro(dest_zapi, dest_host_id, macro_name, macro_value):
             logging.error(f"Failed to ensure macro '{macro_name}' exists. Cannot create '{trigger_description}' trigger.")
             return False

        trigger_expression = f"min(/{dest_host_name}/{item_key},5m)>{macro_name} and min(/{dest_host_name}/{item_key},5m)<100"
        logging.info(f"Creating trigger '{trigger_description}' for host '{dest_host_name}'...")
        params = {
            "description": trigger_description,
            "expression": trigger_expression,
            "priority": "2",
            "opdata": "Loss: {ITEM.LASTVALUE1}",
            "comments": f"Ping loss is high. Check network stability. Requires {macro_name} macro.",
            "status": "0"
        }
        result = dest_zapi.trigger.create(params)
        if result and 'triggerids' in result and result['triggerids']:
            logging.info(f"Successfully created trigger '{trigger_description}' with ID: {result['triggerids'][0]}")
            return True
        else:
            logging.error(f"Failed to create trigger '{trigger_description}'. API Response: {result}")
            return False
    except Exception as e:
        logging.error(f"Error creating trigger '{trigger_description}': {e}", exc_info=True)
        if "already exists" in str(e).lower() or "trigger with the same name" in str(e).lower():
             logging.warning(f"Trigger '{trigger_description}' likely already exists (API error: {e}). Skipping.")
             return True
        return False


def create_icmp_response_trigger(dest_zapi, dest_host_id, dest_host_name, item_key="icmppingsec"):
    """Creates a High ICMP ping response time trigger if it doesn't exist, ensuring macro exists."""
    trigger_description = "High ICMP ping response time"
    macro_name = "{$ICMP_RESPONSE_TIME_WARN}"
    macro_value = "0.1"

    try:
        items = dest_zapi.item.get(hostids=dest_host_id, filter={"key_": item_key}, output=["itemid"])
        if not items:
            logging.warning(f"Item '{item_key}' not found on host '{dest_host_name}'. Cannot create '{trigger_description}' trigger.")
            return False

        existing_triggers = dest_zapi.trigger.get(
            hostids=dest_host_id,
            filter={"description": trigger_description},
            output=["triggerid"]
        )
        if existing_triggers:
            logging.debug(f"Trigger '{trigger_description}' already exists for host '{dest_host_name}'. Skipping creation.")
            return True

        if not create_or_update_macro(dest_zapi, dest_host_id, macro_name, macro_value):
            logging.error(f"Failed to ensure macro '{macro_name}' exists. Cannot create '{trigger_description}' trigger.")
            return False

        trigger_expression = f"avg(/{dest_host_name}/{item_key},5m)>{macro_name}"
        logging.info(f"Creating trigger '{trigger_description}' for host '{dest_host_name}'...")
        params = {
            "description": trigger_description,
            "expression": trigger_expression,
            "priority": "2",
            "opdata": "Value: {ITEM.LASTVALUE1}",
            "comments": f"Ping response time is high. Check network latency. Requires {macro_name} macro.",
            "status": "0"
        }
        result = dest_zapi.trigger.create(params)
        if result and 'triggerids' in result and result['triggerids']:
            logging.info(f"Successfully created trigger '{trigger_description}' with ID: {result['triggerids'][0]}")
            return True
        else:
            logging.error(f"Failed to create trigger '{trigger_description}'. API Response: {result}")
            return False
    except Exception as e:
        logging.error(f"Error creating trigger '{trigger_description}': {e}", exc_info=True)
        if "already exists" in str(e).lower() or "trigger with the same name" in str(e).lower():
             logging.warning(f"Trigger '{trigger_description}' likely already exists (API error: {e}). Skipping.")
             return True
        return False


def create_cpu_utilization_trigger(dest_zapi, dest_host_id, dest_host_name, item_key_pattern="system.cpu.util["):
    """Creates High CPU utilization triggers for each relevant CPU item, ensuring macro exists."""
    macro_name = "{$CPU.UTIL.CRIT}"
    macro_value = "80" # Default critical value
    success = True

    try:
        # Find items matching the key pattern
        items = dest_zapi.item.get(
            hostids=dest_host_id,
            search={"key_": item_key_pattern},
            output=["itemid", "key_"]
        )
        if not items:
            logging.warning(f"No items found matching key pattern '{item_key_pattern}' on host '{dest_host_name}'. Cannot create CPU utilization triggers.")
            return False

        # Ensure the critical CPU utilization macro exists
        if not create_or_update_macro(dest_zapi, dest_host_id, macro_name, macro_value):
            logging.error(f"Failed to ensure macro '{macro_name}' exists. Cannot create CPU utilization triggers.")
            return False

        for item in items:
            item_key = item['key_']
            # Extract CPU core number from the item key if possible, or use the full key
            import re
            match = re.search(r'\[.*?(\d+)\]', item_key)
            cpu_core = match.group(1) if match else item_key

            trigger_description = f"High CPU utilization on core {cpu_core} (over {macro_name}% for 5m)"
            trigger_expression = f"min(/{dest_host_name}/{item_key},5m)>{macro_name}"

            # Check if trigger already exists for this specific core
            existing_triggers = dest_zapi.trigger.get(
                hostids=dest_host_id,
                filter={"description": trigger_description},
                output=["triggerid"]
            )
            if existing_triggers:
                logging.debug(f"Trigger '{trigger_description}' already exists for host '{dest_host_name}'. Skipping creation.")
                continue # Skip to the next item

            logging.info(f"Creating trigger '{trigger_description}' for host '{dest_host_name}'...")
            params = {
                "description": trigger_description,
                "expression": trigger_expression,
                "priority": "4", # High priority
                "opdata": "Current utilization: {ITEM.LASTVALUE1}%",
                "comments": f"CPU utilization on core {cpu_core} is high. Check running processes. Requires {macro_name} macro.",
                "status": "0" # Enabled
            }
            result = dest_zapi.trigger.create(params)
            if result and 'triggerids' in result and result['triggerids']:
                logging.info(f"Successfully created trigger '{trigger_description}' with ID: {result['triggerids'][0]}")
            else:
                logging.error(f"Failed to create trigger '{trigger_description}'. API Response: {result}")
                success = False # Mark as failure but continue with other items

    except Exception as e:
        logging.error(f"Error creating CPU utilization triggers: {e}", exc_info=True)
        success = False

    return success


def create_interface_speed_change_trigger(dest_zapi, dest_host_id, dest_host_name, interface_name, speed_item_key, status_item_key, type_item_key):
    """Creates an interface speed change trigger if it doesn't exist."""
    trigger_description = f"Interface {interface_name}: Speed changed to lower"
    # No specific macro needed for this one based on the example, but could add one if desired.

    try:
        # Check if trigger already exists
        existing_triggers = dest_zapi.trigger.get(
            hostids=dest_host_id,
            filter={"description": trigger_description},
            output=["triggerid"]
        )
        if existing_triggers:
            logging.debug(f"Trigger '{trigger_description}' already exists for host '{dest_host_name}'. Skipping creation.")
            return True

        # Construct expression based on user input
        trigger_expression = (
            f"change(/{dest_host_name}/{speed_item_key})<0 and last(/{dest_host_name}/{speed_item_key})>0 "
            f"and (\n"
            f"last(/{dest_host_name}/{type_item_key})=6 or\n"
            f"last(/{dest_host_name}/{type_item_key})=7 or\n"
            f"last(/{dest_host_name}/{type_item_key})=11 or\n"
            f"last(/{dest_host_name}/{type_item_key})=62 or\n"
            f"last(/{dest_host_name}/{type_item_key})=69 or\n"
            f"last(/{dest_host_name}/{type_item_key})=117\n"
            f")\n"
            f"and\n"
            f"(last(/{dest_host_name}/{status_item_key})<>2)"
        )

        # Construct recovery expression based on user input
        recovery_expression = (
            f"(change(/{dest_host_name}/{speed_item_key})>0 and last(/{dest_host_name}/{speed_item_key},#2)>0) or\n"
            f"(last(/{dest_host_name}/{status_item_key})=2)"
        )

        logging.info(f"Creating trigger '{trigger_description}' for host '{dest_host_name}'...")
        params = {
            "description": trigger_description,
            "expression": trigger_expression,
            "priority": "2",  # Warning priority
            "comments": f"Interface {interface_name} speed has changed to a lower value than before.\\nChecks if speed decreased, is not zero, is an Ethernet-like type, and is not operationally down.",
            "status": "0", # Enabled
            "recovery_mode": "1", # Recovery expression
            "recovery_expression": recovery_expression,
            "manual_close": "0", # Allow auto-recovery
            "opdata": "Current speed: {{ITEM.LASTVALUE1}}" # Show speed in opdata (refers to first item in expression)
        }
        result = dest_zapi.trigger.create(params)
        if result and 'triggerids' in result and result['triggerids']:
            logging.info(f"Successfully created trigger '{trigger_description}' with ID: {result['triggerids'][0]}")
            return True
        else:
            logging.error(f"Failed to create trigger '{trigger_description}'. API Response: {result}")
            return False
    except Exception as e:
        logging.error(f"Error creating trigger '{trigger_description}': {e}", exc_info=True)
        if "already exists" in str(e).lower() or "trigger with the same name" in str(e).lower():
             logging.warning(f"Trigger '{trigger_description}' likely already exists (API error: {e}). Skipping.")
             return True
        return False


def create_temperature_critical_trigger(dest_zapi, dest_host_id, dest_host_name, item_key):
    """Creates a trigger for temperature above critical threshold."""
    trigger_description = "Device: Temperature is above critical threshold: >{$TEMP_CRIT:\"Device\"}"
    macro_name = "{$TEMP_CRIT:\"Device\"}"
    # A default value for the macro is needed. Let's assume a reasonable default like 50 degrees Celsius.
    # This should ideally be configurable or fetched from somewhere. Using 50 as a placeholder.
    macro_value = "50"

    try:
        # Check if the required item exists
        items = dest_zapi.item.get(hostids=dest_host_id, filter={"key_": item_key}, output=["itemid"])
        if not items:
            logging.warning(f"Item with key '{item_key}' not found on host '{dest_host_name}' (ID: {dest_host_id}). Cannot create temperature critical trigger.")
            return False

        # Check if trigger already exists
        existing_triggers = dest_zapi.trigger.get(
            hostids=dest_host_id,
            filter={"description": trigger_description},
            output=["triggerid"]
        )
        if existing_triggers:
            logging.debug(f"Trigger '{trigger_description}' already exists for host '{dest_host_name}'. Skipping creation.")
            return True

        # Ensure the critical temperature macro exists
        if not create_or_update_macro(dest_zapi, dest_host_id, macro_name, macro_value):
            logging.error(f"Failed to ensure macro '{macro_name}' exists. Cannot create '{trigger_description}' trigger.")
            return False

        # Construct problem and recovery expressions
        problem_expression = f"avg(/{dest_host_name}/{item_key},5m)>{macro_name}"
        recovery_expression = f"max(/{dest_host_name}/{item_key},5m)<{macro_name}-3"


        logging.info(f"Creating trigger '{trigger_description}' for host '{dest_host_name}'...")
        params = {
            "description": trigger_description,
            "expression": problem_expression,
            "priority": "5",  # Disaster priority for critical temperature
            "comments": f"Temperature is above critical threshold. Check device cooling. Requires {macro_name} macro.",
            "status": "0", # Enabled
            "recovery_mode": "1", # Recovery expression
            "recovery_expression": recovery_expression,
            "manual_close": "0", # Allow auto-recovery
            "opdata": "Current temperature: {{ITEM.LASTVALUE1}}" # Show temperature in opdata
        }
        result = dest_zapi.trigger.create(params)
        if result and 'triggerids' in result and result['triggerids']:
            logging.info(f"Successfully created trigger '{trigger_description}' with ID: {result['triggerids'][0]}")
            return True
        else:
            logging.error(f"Failed to create trigger '{trigger_description}'. API Response: {result}")
            return False
    except Exception as e:
        logging.error(f"Error creating trigger '{trigger_description}': {e}", exc_info=True)
        if "already exists" in str(e).lower() or "trigger with the same name" in str(e).lower():
             logging.warning(f"Trigger '{trigger_description}' likely already exists (API error: {e}). Skipping.")
             return True
        return False


def create_interface_link_down_trigger(dest_zapi, dest_host_id, dest_host_name, interface_name, item_key):
    """Creates an interface link down trigger if it doesn't exist."""
    trigger_description = f"Interface {interface_name}: Link down"
    macro_name = f"{{$IFCONTROL:\"{interface_name}\"}}"
    macro_value = "1" # Default value for {$IFCONTROL}

    try:
        # Check if trigger already exists
        existing_triggers = dest_zapi.trigger.get(
            hostids=dest_host_id,
            filter={"description": trigger_description},
            output=["triggerid"]
        )
        if existing_triggers:
            logging.debug(f"Trigger '{trigger_description}' already exists for host '{dest_host_name}'. Skipping creation.")
            return True

        # Create or update macro on destination host
        if not create_or_update_macro(dest_zapi, dest_host_id, macro_name, macro_value):
            logging.error(f"Failed to ensure macro '{macro_name}' exists. Cannot create '{trigger_description}' trigger.")
            return False

        # Construct expression using the item key, the macro, and the actual host name
        trigger_expression = f'{macro_name}=1 and (last(/{dest_host_name}/{item_key})=2 and (last(/{dest_host_name}/{item_key},#1)<>last(/{dest_host_name}/{item_key},#2))=1)'
        recovery_expression = f'last(/{dest_host_name}/{item_key})<>2 or {macro_name}=0'

        logging.info(f"Creating trigger '{trigger_description}' for host '{dest_host_name}'...")
        params = {
            "description": trigger_description,
            "expression": trigger_expression,
            "priority": "3",  # Average priority
            "comments": f"This trigger expression works as follows:\\r\\n1. Can be triggered if operations status is down.\\r\\n2. {macro_name}=1 - user can redefine Context macro to value - 0. That marks this interface as not important. No new trigger will be fired if this interface is down.\\r\\n3. {{HOST.NAME}}:{item_key}.diff()}}=1) - trigger fires only if operational status was up(1) sometime before. (So, do not fire 'ethernal off' interfaces.)\\r\\n\\r\\nWARNING: if closed manually - won't fire again on next poll, because of .diff.",
            "status": "0",
            "recovery_mode": "1",
            "recovery_expression": recovery_expression,
            "manual_close": "1",
            "opdata": "Current state: {{ITEM.LASTVALUE1}}"
        }
        result = dest_zapi.trigger.create(params)
        if result and 'triggerids' in result and result['triggerids']:
            logging.info(f"Successfully created trigger '{trigger_description}' with ID: {result['triggerids'][0]}")
            return True
        else:
            logging.error(f"Failed to create trigger '{trigger_description}'. API Response: {result}")
            return False
    except Exception as e:
        logging.error(f"Error creating trigger '{trigger_description}': {e}", exc_info=True)
        if "already exists" in str(e).lower() or "trigger with the same name" in str(e).lower():
             logging.warning(f"Trigger '{trigger_description}' likely already exists (API error: {e}). Skipping.")
             return True
        return False
