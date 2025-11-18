# Azure Event Grid System
# Creating Event Grid System Topic 
# Create Event Subscription as a webhook with the function url 

import azure.functions as func
import logging
import os
import gzip
import zlib
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import traceback
import json
from urllib.parse import urlparse

# --- CONFIGURATION ---
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Environment variables (set these in Azure App Settings)
SRC_PREFIX = os.getenv("SRC_PREFIX", "")
DEST_PREFIX = os.getenv("DEST_PREFIX", "")
DEST_CONTAINER = os.getenv("DEST_CONTAINER", "")
STORAGE_CONN_STR = os.getenv("mainblobstorage")

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# --- MAIN FUNCTION ---
@app.route(route="http_trigger", methods=["POST"])
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Parse the Event Grid message
        events = req.get_json()
        if not isinstance(events, list):
            events = [events]

        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        processed_files = []

        for event in events:
            # Handle Event Grid validation handshake
            if event.get("eventType") == "Microsoft.EventGrid.SubscriptionValidationEvent":
                validation_code = event["data"]["validationCode"]
                logging.info(f"Responding to Event Grid validation with code: {validation_code}")
                return func.HttpResponse(
                    json.dumps({"validationResponse": validation_code}),
                    mimetype="application/json"
                )

            # Handle blob created event
            elif event.get("eventType") == "Microsoft.Storage.BlobCreated":
                blob_url = event["data"]["url"]
                parsed_url = urlparse(blob_url)
                path_parts = parsed_url.path.lstrip("/").split("/", 1)
                container_name = path_parts[0]
                blob_name = path_parts[1]

                logging.info(f"[START] Processing blob: {blob_name} in container: {container_name}")

                # Prepare destination blob
                dest_blob_name = f"{DEST_PREFIX}/{os.path.basename(blob_name)}"
                dest_blob_client = blob_service_client.get_blob_client(
                    container=DEST_CONTAINER,
                    blob=dest_blob_name
                )

                # Skip if already exists
                if dest_blob_client.exists():
                    logging.info(f"[SKIP] Destination blob already exists: {dest_blob_name}")
                    continue

                # Download source blob
                src_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                raw_data = src_blob_client.download_blob().readall()
                logging.info(f"Downloaded {len(raw_data)} bytes from {blob_name}")

                # Attempt decompression for .Z files
                if blob_name.endswith(".Z"):
                    try:
                        raw_data = zlib.decompress(raw_data, 15 + 32)
                        logging.info("Decompression successful")
                    except Exception as dec_err:
                        logging.warning(f"Decompression failed, might already be uncompressed: {dec_err}")

                # Detect and normalize encoding
                decoded_text = None
                used_decoding = "binary"
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        decoded_text = raw_data.decode(encoding)
                        used_decoding = encoding
                        break
                    except (UnicodeDecodeError, AttributeError):
                        continue

                # Re-encode and compress
                try:
                    iso_encoded = decoded_text.encode('iso-8859-1', errors='strict') if decoded_text else raw_data
                    used_encoding = 'iso-8859-1'
                except UnicodeEncodeError:
                    iso_encoded = decoded_text.encode('utf-8') if decoded_text else raw_data
                    used_encoding = 'utf-8'

                compressed_buffer = BytesIO()
                with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
                    gz.write(iso_encoded)
                compressed_data = compressed_buffer.getvalue()

                # Upload compressed file
                dest_blob_client.upload_blob(
                    compressed_data,
                    overwrite=False,
                    metadata={
                        'original_size': str(len(raw_data)),
                        'compressed_size': str(len(compressed_data)),
                        'original_encoding': used_decoding,
                        'compressed_encoding': used_encoding,
                        'source_file': blob_name
                    }
                )

                logging.info(f"[SUCCESS] Uploaded processed blob: {dest_blob_name}")
                processed_files.append(blob_name)

        return func.HttpResponse(f"Processed {len(processed_files)} files", status_code=200)

    except Exception:
        logging.error("[FATAL] HTTP function failed.")
        logging.error(traceback.format_exc())
        return func.HttpResponse("Failed", status_code=500)
