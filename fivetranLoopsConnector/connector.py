import time
import json
import csv
import io
import urllib.request
import requests

# Fivetran Connector SDK import
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

def wait(seconds):
    """Simple wait utility (sleep)."""
    time.sleep(seconds)

def fetch_loops_export(session_cookie):
    """
    Initiates and polls for a Loops audience export.
    Returns the presigned download URL for the CSV once ready.
    """
    log.info("Requesting Loops export creation...")

    export_resp = requests.post(
        "https://app.loops.so/api/trpc/lists.exportContacts",
        headers={
            "content-type": "application/json",
            "cookie": session_cookie
        },
        json={
            "json": {
                "filter": None,
                "mailingListId": ""
            }
        }
    )
    export_resp.raise_for_status()

    export_data = export_resp.json().get("result", {}).get("data", {})
    export_id = export_data.get("json", {}).get("id")
    if not export_id:
        log.severe("Could not initiate Loops export – missing export ID.")
        raise ValueError("Could not initiate Loops export – missing export ID.")

    log.info(f"Loops export job created with ID={export_id}. Polling until ready...")

    # 2. Poll until status is 'Complete'
    status = None
    check_params = {
        "json": {
            "id": export_id
        }
    }
    encoded_params = urllib.request.quote(json.dumps(check_params))
    check_url = f"https://app.loops.so/api/trpc/audienceDownload.getAudienceDownload?input={encoded_params}"

    while status != "Complete":
        wait(5)
        log.info("Checking Loops export status...")

        poll_resp = requests.get(
            check_url,
            headers={
                "content-type": "application/json",
                "cookie": session_cookie
            }
        )
        poll_resp.raise_for_status()

        poll_data = poll_resp.json().get("result", {}).get("data", {})
        status = poll_data.get("json", {}).get("status", None)
        log.info(f"Current export status: {status}")

    # 3. Request signed S3 download URL
    log.info("Export is complete! Retrieving the presigned download URL...")
    sign_resp = requests.post(
        "https://app.loops.so/api/trpc/audienceDownload.signs3Url",
        headers={
            "content-type": "application/json",
            "cookie": session_cookie
        },
        json={"json": {"id": export_id}}
    )
    sign_resp.raise_for_status()

    s3_data = sign_resp.json().get("result", {}).get("data", {})
    download_url = s3_data.get("json", {}).get("presignedUrl")
    if not download_url:
        log.severe("Could not retrieve presigned download URL for Loops export.")
        raise ValueError("Could not retrieve presigned download URL for Loops export.")

    return download_url

def download_loops_csv(download_url):
    """
    Downloads the CSV from the Loops presigned URL, returns it as in-memory data.
    """
    log.info("Downloading Loops CSV file...")
    resp = requests.get(download_url)
    resp.raise_for_status()
    return resp.content  # raw bytes

def schema(configuration):
    """
    Define the schema for our tables.
    """
    return [
        {
            "table": "audience",
            "primary_key": ["email"],
            "columns": {
                "email": "STRING",
                "firstName": "STRING",
                "lastName": "STRING",
                "createdAt": "UTC_DATETIME",
                "updatedAt": "UTC_DATETIME",
                "unsubscribed": "BOOLEAN"
            }
        }
    ]

def update(configuration, state):
    """
    Main data sync function.
    """
    log.info("Starting Loops data sync...")
    log.info(f"Configuration: {configuration}")
    
    if not configuration or "SESSION_COOKIE" not in configuration:
        log.severe("Missing SESSION_COOKIE in configuration")
        raise ValueError("Missing SESSION_COOKIE in configuration")
    
    session_cookie = configuration["SESSION_COOKIE"]

    # Get download URL
    download_url = fetch_loops_export(session_cookie)

    # 2. Download CSV in-memory
    csv_bytes = download_loops_csv(download_url)

    # 3. Parse CSV
    log.info("Parsing the CSV from Loops export...")
    
    # Process in chunks using a file-like object
    csv_file = io.StringIO(csv_bytes.decode("utf-8", errors="ignore"))
    reader = csv.DictReader(csv_file)
    
    # Process in batches of 1000 records
    batch_size = 1000
    current_batch = []
    total_records = 0
    
    for row in reader:
        # Convert data types
        record = {
            "email": row.get("email", ""),
            "firstName": row.get("firstName", ""),
            "lastName": row.get("lastName", ""),
            "createdAt": row.get("createdAt", "").replace('Z', '+00:00') if row.get("createdAt") else None,
            "updatedAt": row.get("updatedAt", "").replace('Z', '+00:00') if row.get("updatedAt") else None,
            "unsubscribed": row.get("unsubscribed", "false").lower() == "true"
        }
        
        current_batch.append(record)
        
        # When we reach batch_size, yield the batch and checkpoint
        if len(current_batch) >= batch_size:
            total_records += len(current_batch)
            log.info(f"Processing batch of {len(current_batch)} records (total processed: {total_records})")
            
            # Yield each record in the batch
            for r in current_batch:
                yield op.upsert("audience", r)
            
            # Checkpoint after each batch
            yield op.checkpoint(state={"last_sync": time.time(), "records_processed": total_records})
            
            # Clear the batch
            current_batch = []
    
    # Process any remaining records
    if current_batch:
        total_records += len(current_batch)
        log.info(f"Processing final batch of {len(current_batch)} records (total processed: {total_records})")
        
        for r in current_batch:
            yield op.upsert("audience", r)
    
    # Final checkpoint
    yield op.checkpoint(state={"last_sync": time.time(), "records_processed": total_records})
    
    log.info(f"Completed processing {total_records} total records")

# Initialize the Connector object
connector = Connector(update=update, schema=schema)
