# b_Prod_iso_to_UTF
import azure.functions as func
import logging
import os
import gzip
import zlib
from azure.storage.blob import BlobServiceClient
import io
from io import BytesIO
import codecs
import traceback
import json
from urllib.parse import urlparse

# --- CONFIGURATION ---
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

SRC_PREFIX = os.getenv("SRC_PREFIX")
DEST_PREFIX = os.getenv("DEST_PREFIX")
DEST_CONTAINER = os.getenv("DEST_CONTAINER")
STORAGE_CONN_STR = os.getenv("mainblobstorage")

def load_file_destination_mapping():
    try:
        blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)

        container_name = "config-data"
        blob_name = "FileDelivery/file_delivery_locations.json"

        blob_client = blob_service.get_blob_client(container_name, blob_name)

        file_bytes = blob_client.download_blob().readall()
        mapping = json.loads(file_bytes.decode("utf-8"))

        logging.info("Successfully loaded File_DEST_MAPPING from blob.")
        return mapping

    except Exception as e:
        logging.error(f"Failed to load file_delivery_locations.json: {e}")
        raise

# Load mapping at startup
File_DEST_MAPPING = load_file_destination_mapping()

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

CHUNK_SIZE = 1024 * 1024 * 8   # 8MB chunks for large .ABC files

@app.route(route="http_trigger", methods=["POST"])
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Starting function...")

        events = req.get_json()
        if not isinstance(events, list):
            events = [events]

        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
        processed_files = []
        logging.info("Container connection is ok")

        for event in events:

            # Event Grid validation handshake
            if event.get("eventType") == "Microsoft.EventGrid.SubscriptionValidationEvent":
                return func.HttpResponse(
                    json.dumps({"validationResponse": event["data"]["validationCode"]}),
                    mimetype="application/json"
                )

            # Blob created event handling
            elif event.get("eventType") == "Microsoft.Storage.BlobCreated":

                blob_url = event["data"]["url"]
                parsed = urlparse(blob_url)
                container_name, blob_name = parsed.path.lstrip("/").split("/", 1)

                logging.info(f"[START] Processing: {blob_name}")

                src_blob_client = blob_service_client.get_blob_client(container_name, blob_name)

                # CASE 1: PROCESSING ".ABC"

                if ".ABC" in blob_name.upper():
                    logging.info("Detected .ABC file → using streaming large-file pipeline")
                    src_chunks = src_blob_client.download_blob().chunks()
                    logging.info("Downloading Blob")
                    # --- streaming gz decompressor ---
                    def stream_gz_decompress(chunck):
                        d = zlib.decompressobj(16 + zlib.MAX_WBITS)   # gzip format
                        for chunk in chunck:
                            data = d.decompress(chunk)
                            if data:
                                yield data
                        tail = d.flush()
                        if tail:
                            yield tail

                    input_stream = stream_gz_decompress(src_chunks)
                    logging.info("Streaming")

                    # Streaming encoder/decoder
                    decoder = codecs.getincrementaldecoder("iso-8859-1")()
                    encoder = codecs.getincrementalencoder("utf-8")()

                    # Streaming output (compressed)
                    out_buffer = io.BytesIO()
                    gzip_writer = gzip.GzipFile(fileobj=out_buffer, mode="wb")

                    for chunk in input_stream:
                        text = decoder.decode(chunk)
                        utf8_bytes = encoder.encode(text)
                        gzip_writer.write(utf8_bytes)
                   
                    final_text = decoder.decode(b"", final=True)
                    if final_text:
                        gzip_writer.write(encoder.encode(final_text))

                    gzip_writer.close()
                    data_to_upload = out_buffer.getvalue()
                    
                # CASE 2: NORMAL WORKLOAD FOR NON-ABC FILES
                else: 
                    logging.info("Normal file detected → using standard workload")

                    raw_data = src_blob_client.download_blob().readall()

                    if blob_name.endswith(".gz"):
                        try:
                            with gzip.GzipFile(fileobj=BytesIO(raw_data), mode="rb") as gz:
                                raw_data = gz.read()
                            logging.info("Successfully decompressed .gz file")
                        except Exception as e:
                            logging.error(f"Failed to decompress .gz: {e}")
                            raise

                    # Detect and normalize encoding
                    try:
                        decoded_text = raw_data.decode("iso-8859-1")
                        logging.info("Decoded as ISO-8859-1")
                    except Exception as e:
                        logging.error("Failed to decode from ISO-8859-1")
                        raise

                    utf8_data = decoded_text.encode("utf-8")
                    buffer_out = BytesIO()
                    with gzip.GzipFile(fileobj=buffer_out, mode="wb") as gz:
                        gz.write(utf8_data)
                    data_to_upload  = buffer_out.getvalue()


                # Determine directories to upload
                upload_dirs = []
                for key, dirs in File_DEST_MAPPING.items():
                    if key in blob_name.upper():
                        upload_dirs.extend(dirs)
                if not upload_dirs:
                    upload_dirs = [""]

                #  Upload to all destinations 
                for directory in upload_dirs:
                    dest_blob_name = f"{DEST_PREFIX}/{directory}{os.path.basename(blob_name)}"
                    dest_blob_client = blob_service_client.get_blob_client(DEST_CONTAINER, dest_blob_name)
                    dest_blob_client.upload_blob(
                        data_to_upload,
                        overwrite=True,
                        metadata={
                            "converted_from": "iso-8859-1",
                            "converted_to": "utf-8",
                            "compressed": "gz",
                            "streaming": "true" if ".ABC" in blob_name.upper() else "false",
                            "source_file": blob_name
                        },
                    )
                    logging.info(f"[SUCCESS] Uploaded to {dest_blob_name}")

                # delete source blob
                src_blob_client.delete_blob()
                processed_files.append(blob_name)

        return func.HttpResponse(f"Processed {len(processed_files)} files.", status_code=200)

    except Exception:
        logging.error("Fatal error")
        logging.error(traceback.format_exc())
        return func.HttpResponse("Failed", status_code=500)



