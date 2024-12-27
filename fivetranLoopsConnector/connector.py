import time
import json
import csv
import io
import urllib.request
import requests
import re

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

def sanitize_field_name(field_name):
    """
    Sanitize field names to be SQL-safe.
    - Replace spaces and special characters with underscores
    - Ensure it doesn't start with a number
    - Convert to lowercase
    """
    # Replace any non-alphanumeric character with underscore
    sanitized = re.sub(r'[^a-zA-Z0-9]', '_', field_name)
    # Ensure it doesn't start with a number
    if sanitized[0].isdigit():
        sanitized = 'f_' + sanitized
    # Convert to lowercase
    return sanitized.lower()

def fetch_custom_fields(api_key):
    """
    Fetches custom fields from Loops API.
    Returns a dictionary of field names and their types.
    """
    log.info("Fetching custom fields from Loops API...")

    resp = requests.get(
        "https://app.loops.so/api/v1/contacts/customFields",
        headers={
            "Authorization": f"Bearer {api_key}"
        }
    )
    resp.raise_for_status()

    custom_fields = resp.json()
    field_types = {}

    # Map Loops types to Fivetran types
    type_mapping = {
        "string": "STRING",
        "number": "DOUBLE",
        "boolean": "BOOLEAN",
        "date": "UTC_DATETIME"
    }

    # Keep track of original to sanitized field name mapping
    field_mapping = {}

    for field in custom_fields:
        original_key = field["key"]
        sanitized_key = sanitize_field_name(original_key)
        loops_type = field["type"]
        
        # Store both the type and original name
        field_types[sanitized_key] = {
            "type": type_mapping.get(loops_type, "STRING"),
            "original_name": original_key
        }
        field_mapping[original_key] = sanitized_key

    return field_types, field_mapping

def schema(configuration):
    """
    Define the schema for our tables.
    """
    if not configuration or "LOOPS_API_KEY" not in configuration:
        log.severe("Missing LOOPS_API_KEY in configuration")
        raise ValueError("Missing LOOPS_API_KEY in configuration")

    api_key = configuration["LOOPS_API_KEY"]
    custom_fields, _ = fetch_custom_fields(api_key)

    # Base schema with standard fields
    columns = {
        "email": "STRING",
        "firstname": "STRING",
        "lastname": "STRING",
        "createdat": "UTC_DATETIME",
        "updatedat": "UTC_DATETIME",
        "unsubscribed": "BOOLEAN"
    }

    # Add custom fields to schema
    for field, info in custom_fields.items():
        columns[field] = info["type"]

    log.info(f"Schema includes {len(custom_fields)} custom fields")

    return [
        {
            "table": "audience",
            "primary_key": ["email"],
            "columns": columns
        }
    ]

def update(configuration, state):
    """
    Main data sync function.
    """
    log.info("Starting Loops data sync...")
    log.info(f"Configuration: {configuration}")
    
    if not configuration or "SESSION_COOKIE" not in configuration or "LOOPS_API_KEY" not in configuration:
        log.severe("Missing required configuration (SESSION_COOKIE or LOOPS_API_KEY)")
        raise ValueError("Missing required configuration (SESSION_COOKIE or LOOPS_API_KEY)")
    
    session_cookie = configuration["SESSION_COOKIE"]
    api_key = configuration["LOOPS_API_KEY"]

    # Get custom fields first to know what to expect in the CSV
    custom_fields, field_mapping = fetch_custom_fields(api_key)
    log.info(f"Found {len(custom_fields)} custom fields")

    # Get download URL
    download_url = fetch_loops_export(session_cookie)

    # 2. Download CSV in-memory
    csv_bytes = download_loops_csv(download_url)

    # 3. Parse CSV
    log.info("Parsing the CSV from Loops export...")
    
    # Process in chunks using a file-like object
    csv_file = io.StringIO(csv_bytes.decode("utf-8", errors="ignore"))
    reader = csv.reader(csv_file)
    
    # Get headers and map to indices
    headers = next(reader)
    field_indices = {}
    
    # Map standard fields (case-insensitive)
    standard_mapping = {
        "email": "email",
        "firstName": "firstname",
        "lastName": "lastname",
        "createdAt": "createdat",
        "updatedAt": "updatedat",
        "unsubscribed": "unsubscribed"
    }
    
    for original, sanitized in standard_mapping.items():
        if original in headers:
            field_indices[sanitized] = headers.index(original)
    
    # Map custom fields using the field mapping
    for original_name, sanitized_name in field_mapping.items():
        if original_name in headers:
            field_indices[sanitized_name] = headers.index(original_name)
    
    log.info(f"Found {len(field_indices)} fields in CSV")
    
    # Process in batches of 1000 records
    batch_size = 1000
    current_batch = []
    total_records = 0
    
    for row in reader:
        # Start with standard fields
        record = {
            "email": row[field_indices["email"]] if "email" in field_indices else "",
            "firstname": row[field_indices["firstname"]] if "firstname" in field_indices else "",
            "lastname": row[field_indices["lastname"]] if "lastname" in field_indices else "",
            "createdat": row[field_indices["createdat"]].replace('Z', '+00:00') if "createdat" in field_indices and row[field_indices["createdat"]] else None,
            "updatedat": row[field_indices["updatedat"]].replace('Z', '+00:00') if "updatedat" in field_indices and row[field_indices["updatedat"]] else None,
            "unsubscribed": row[field_indices["unsubscribed"]].lower() == "true" if "unsubscribed" in field_indices else False
        }
        
        # Add custom fields
        for sanitized_name, info in custom_fields.items():
            if sanitized_name in field_indices:
                value = row[field_indices[sanitized_name]]
                field_type = info["type"]
                if field_type == "BOOLEAN":
                    record[sanitized_name] = value.lower() == "true" if value else False
                elif field_type == "DOUBLE":
                    record[sanitized_name] = float(value) if value else None
                elif field_type == "UTC_DATETIME":
                    record[sanitized_name] = value.replace('Z', '+00:00') if value else None
                else:  # STRING
                    record[sanitized_name] = value
        
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
