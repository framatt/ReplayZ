import logging
import time
import random
from datetime import datetime, timedelta
from zabbix_utils import Sender, ItemValue # Correct Sender/ItemValue import
from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm.attributes import flag_modified # Import flag_modified
from sqlalchemy.exc import OperationalError
from trigger_utils import simulate_random_problem

# Import SQLAlchemy components and config from common.py
from common import ReplicationTask, SessionLocal, config # Import necessary components from common.py

# Basic logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def commit_with_retry(db_session, max_retries=5, base_delay=0.1):
    """
    Commit database changes with retry logic to handle SQLite lock errors.

    Args:
        db_session: SQLAlchemy session to commit
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds for exponential backoff

    Returns:
        bool: True if commit succeeded, False otherwise
    """
    for attempt in range(max_retries):
        try:
            db_session.commit()
            if attempt > 0:
                logging.info(f"Database commit succeeded on attempt {attempt + 1}")
            return True
        except OperationalError as e:
            if "database is locked" in str(e).lower():
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)  # Exponential backoff with jitter
                    logging.warning(f"Database locked, retrying commit in {delay:.2f} seconds (attempt {attempt + 1}/{max_retries})")
                    time.sleep(delay)
                    continue
                else:
                    logging.error(f"Database commit failed after {max_retries} attempts: {e}")
                    return False
            else:
                # Re-raise non-lock related operational errors
                raise e
    return False

def replay_job(source_host_id):
    """Scheduled job to send historical data to the destination trapper.

    Args:
        source_host_id: ID of the source host being replicated
    """
    # Use a database session within the job
    db = SessionLocal()
    try:
        # Fetch the task from the database
        task = db.query(ReplicationTask).filter(ReplicationTask.source_host_id == source_host_id).first()

        if not task or task.status == 'completed' or task.status == 'failed':
            logging.warning(f"[Replay Job {source_host_id}] Task not found, completed, or failed. Removing job.")
            # Job removal logic should ideally be handled by the scheduler management in app.py
            # For now, we just return.
            return

        logging.info(f"[Replay Job {source_host_id}] Fetched task from DB. task.last_sent_index: {task.last_sent_index}") # Added Log

        # Access task details from the database object
        dest_host = task.dest_host_name
        history = task.history or {} # Use empty dict if history is None
        item_mapping = task.item_mapping or {} # Use empty dict if item_mapping is None
        start_time = task.start_time
        last_sent_index = task.last_sent_index or {} # Use empty dict if last_sent_index is None
        dest_trapper_host = config.get('dest_trapper_host') # Access config from imported global
        dest_trapper_port = config.get('dest_trapper_port') # Access config from imported global
        cycle_offset = task.cycle_offset or 0 # Use 0 if cycle_offset is None
        replay_duration_hours = config.get('replay_duration_hours', 24) # Get replay duration from config

        if not all([dest_host, history, item_mapping, start_time, dest_trapper_host, dest_trapper_port]):
            logging.error(f"[Replay Job {source_host_id}] Missing necessary data in task details for replay.")
            task.status = 'failed'
            task.message = "Missing necessary data for replay."
            if not commit_with_retry(db):
                logging.error(f"[Replay Job {source_host_id}] Failed to commit failed status")
            return

        current_time = time.time()
        elapsed_seconds = current_time - start_time
        
        # Check if replay duration has been exceeded
        if elapsed_seconds >= replay_duration_hours * 3600:
            logging.info(f"[Replay Job {source_host_id}] Replay duration of {replay_duration_hours} hours exceeded. Restarting replay and resetting start time.")
            task.last_sent_index = {} # Reset index to restart replay from the beginning
            task.start_time = current_time # Reset start time for the next duration calculation
            task.message = f"Replaying... (restarted after {replay_duration_hours} hours, time reset)"
            task.progress = 0.0 # Reset progress for the new cycle
            flag_modified(task, "last_sent_index") # Mark as modified after reset
            # No need to flag_modified for start_time as it's a simple type
            if not commit_with_retry(db):  # Commit the reset
                logging.error(f"[Replay Job {source_host_id}] Failed to commit reset")
            last_sent_index = {} # Update local variable as well

        # Replay time matches elapsed time (1:1 replay speed)
        # Calculate the current timestamp in the replay window
        current_replay_timestamp = (task.first_history_timestamp or 0) + elapsed_seconds

        packet = []
        items_sent_this_run = 0
        total_history_points = 0

        logging.info(f"[Replay Job {source_host_id}] Starting data processing loop. Current local last_sent_index: {last_sent_index}") # Added Log

        try: # Added try block around the loop
            for source_itemid, history_points in history.items():
                dest_key = item_mapping.get(str(source_itemid)) # Ensure source_itemid is string for lookup
                if not dest_key:
                    logging.debug(f"[Replay Job {source_host_id}] Skipping item ID {source_itemid}: No destination key found in mapping.")
                    continue # Skip items without a mapping

                # Get the original item name and key from the source_items_full (if available in task)
                # This requires fetching source_items_full and storing it in the task, or passing it.
                # For now, we'll rely on dest_key and assume it's sufficient for filtering.
                # If dest_key itself contains '<' or starts with 'MTR', it should have been filtered earlier.
                # However, if the mapping somehow includes such a key, we should filter here.
                # This would require access to the original item's name/key, which is not directly in `history` or `item_mapping`.
                # The most robust way would be to store `all_source_items` in the task.history object,
                # or pass it to the replay_job.

                # Given the current structure, the filtering should primarily happen in app.py
                # when fetching history and modifying items. This check is a fallback.
                if '<' in dest_key and '>' in dest_key: # Assuming dynamic interfaces might have '<>' in key too
                    logging.info(f"[Replay Job {source_host_id}] Skipping item with key '{dest_key}' due to dynamic interface pattern '<...>' in key.")
                    continue
                if dest_key.startswith('MTR'):
                    logging.info(f"[Replay Job {source_host_id}] Skipping item with key '{dest_key}' as its key starts with 'MTR'.")
                    continue

                total_history_points += len(history_points)

                # Get the index of the last sent data point for this item
                last_index = last_sent_index.get(str(source_itemid), -1) # Use string key

                # Find the next data point to send
                next_index = last_index + 1
                logging.debug(f"[Replay Job {source_host_id}] Item {source_itemid}: last_index={last_index}, next_index={next_index}, history_points_len={len(history_points)}") # Added Log
                logging.info(f"[Replay Job {source_host_id}] Before update - last_sent_index[{source_itemid}]: {last_sent_index.get(str(source_itemid))}") # Added Log


                if next_index < len(history_points):
                    data_point = history_points[next_index]
                    original_timestamp = data_point['clock']

                    # Calculate the new timestamp for replay - USE CURRENT TIME
                    #time_offset_from_start = original_timestamp - (task.first_history_timestamp or 0) # Original calculation removed
                    #new_timestamp = int(start_time + time_offset_from_start + cycle_offset) # Original calculation removed
                    new_timestamp = int(time.time()) # Use current timestamp
                    # logging.info(f"[Replay Job {source_host_id}] Sending Item: Key='{dest_key}', Value='{data_point['value']}', Timestamp={new_timestamp} ({datetime.utcfromtimestamp(new_timestamp).strftime('%Y-%m-%d %H:%M:%S')} UTC)") # Remove detailed logging

                    # Send the data point
                    # if new_timestamp <= current_time: # <-- REMOVED Check - rely on offset
                    # Simulate problems before sending
                    simulated_value = simulate_random_problem(source_host_id, dest_key, data_point['value'])
                    packet.append(ItemValue(dest_host, dest_key, simulated_value, new_timestamp))
                    last_sent_index[str(source_itemid)] = next_index # Update the last sent index using string key
                    logging.info(f"[Replay Job {source_host_id}] After update - last_sent_index[{source_itemid}]: {last_sent_index.get(str(source_itemid))}") # Added Log
                    items_sent_this_run += 1
                    # else: # <-- REMOVED
                    #    pass # Data point is in the future relative to current replay time # <-- REMOVED
                    # Removed duplicated lines below
                    #else:
                    #    pass # Data point is in the future relative to current replay time
                else:
                    pass # No more history points for this item
        except Exception as loop_error: # Added exception handling for the loop
            logging.error(f"[Replay Job {source_host_id}] Error during data processing loop: {loop_error}", exc_info=True)
            task.status = 'failed_processing'
            task.message = f"Error during data processing: {loop_error}"
            if not commit_with_retry(db):
                logging.error(f"[Replay Job {source_host_id}] Failed to commit loop error status")
            return # Exit job on loop error


        logging.info(f"[Replay Job {source_host_id}] Data processing loop finished. Final local last_sent_index before commit: {last_sent_index}") # Added Log
        # Update the last_sent_index in the task object
        task.last_sent_index = last_sent_index
        flag_modified(task, "last_sent_index") # Mark the JSON field as modified
        db.commit() # Commit the index update
        logging.info(f"[Replay Job {source_host_id}] task.last_sent_index committed.") # Added Log

        if packet:
            logging.info(f"[Replay Job {source_host_id}] Preparing to send {len(packet)} data points...") # Added Log
            try:
                sender_config = {"server": dest_trapper_host, "port": dest_trapper_port}
                sender = Sender(**sender_config)

                #logging.info(f"[Replay Job {source_host_id}] Preparing to send {len(packet)} items:")
                #for i, item in enumerate(packet[:5]): logging.info(f"  Item {i+1}: Host={item.host} Key={item.key} Value={item.value} Clock={item.clock}")

                #logging.info(f"[Replay Job {source_host_id}] Validating packet...")
                for item in packet:
                    if not item.host or not item.key or item.value is None:
                        raise ValueError(f"Invalid ItemValue - host:{item.host} key:{item.key} value:{item.value}")

                # Test connection removed for brevity, assume it works if initial setup passed
                try: # Added try block around sender.send
                    result = sender.send(packet)

                    logging.info(f"[Replay Job {source_host_id}] Sender response: Processed={result.processed}, Failed={result.failed}, Total={result.total}") # Added Log
                    if hasattr(result, 'details'): logging.info(f"  Sender Details: {result.details}") # Added Log

                    if result.failed > 0:
                         logging.error(f"[Replay Job {source_host_id}] Failed to send {result.failed}/{result.total} data points. Details: {result.details}") # Added details to error
                         task.message = f"Replaying... (encountered {result.failed} send failures)"
                    else:
                         task.message = f"Replaying... (sent {len(packet)} points)"

                except Exception as send_error: # Added specific exception handling for sending
                    logging.error(f"[Replay Job {source_host_id}] Error sending data: {send_error}", exc_info=True)
                    task.status = 'failed_sending'
                    task.message = f"Error sending data: {send_error}"
                    # Consider removing job on persistent failure?

            except Exception as general_send_error: # Kept the original broad exception for other potential errors
                logging.error(f"[Replay Job {source_host_id}] General error during sending setup: {general_send_error}", exc_info=True)
                task.status = 'failed_sending_setup'
                task.message = f"Error during sending setup: {general_send_error}"
                # Consider removing job on persistent failure?

        # Update task status and last run time in the task object
        task.last_run_time = current_time # Add last_run_time to the model if needed, or remove this line
        db.commit() # Commit status/message updates

        # Check if all history points have been sent
        # logging.info(f"[Replay Job {source_host_id}] Finished processing items for this run. Final local last_sent_index before check: {last_sent_index}") # Removed Log
        all_history_sent = True
        total_processed_count = 0
        for source_itemid_str, history_points in history.items():
             last_idx = last_sent_index.get(str(source_itemid_str), -1) # Ensure string key here too
             total_processed_count += (last_idx + 1)
             if last_idx < len(history_points) - 1:
                 all_history_sent = False
                 #break # No need to break, calculate total progress

        # Calculate total points for progress
        total_points_to_send = sum(len(points) for points in history.values())
        progress_percent = (total_processed_count / total_points_to_send * 100) if total_points_to_send > 0 else 100

        if all_history_sent:
            logging.info(f"[Replay Job {source_host_id}] Cycle complete. Looping replay with current timestamps.")
            # Reset last_sent_index to replay from the beginning of the history data
            task.last_sent_index = {}
            # DO NOT Increment the cycle offset - it's not used for timestamp calculation anymore
            # task.cycle_offset += 7200 # <-- REMOVED THIS LINE
            task.status = 'replaying' # Keep status as replaying
            task.message = f"Replaying... (looping with current timestamps)" # Updated message
            task.progress = 0.0 # Reset progress for the new cycle
            flag_modified(task, "last_sent_index") # Mark as modified after reset
            # flag_modified(task, "cycle_offset") # No longer needed to flag offset
            db.commit() # Commit cycle completion updates
        else:
             task.status = 'replaying'
             task.message = f"Replaying... Sent {items_sent_this_run} points this run. ({total_processed_count}/{total_points_to_send} total)"
             task.progress = round(progress_percent, 2)
             db.commit() # Commit progress updates
             # logging.info(f"[Replay Job {source_host_id}] History not fully sent. Progress: {progress_percent:.2f}% ({total_processed_count}/{total_points_to_send})") # Removed Log


    except Exception as e:
        logging.error(f"[Replay Job {source_host_id}] Unexpected error: {e}", exc_info=True)
        db.rollback() # Rollback the transaction on error
        if task:
            task.status = 'failed'
            task.message = f"Job Error: {e}"
            try:
                db.commit() # Attempt to commit the status update after rollback
            except Exception as commit_e:
                logging.error(f"[Replay Job {source_host_id}] Error committing failed status after rollback: {commit_e}", exc_info=True)
        # Job removal logic should ideally be handled by the scheduler management in app.py
        # For now, we just log.
        # try:
        #     scheduler.remove_job(f"replay_{source_host_id}")
        # except Exception as rem_e:
        #     logging.warning(f"[Replay Job {source_host_id}] Could not remove job after error: {rem_e}")
    finally:
        db.close() # Ensure the session is closed
