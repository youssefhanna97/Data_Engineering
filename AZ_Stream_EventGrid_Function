# ISO TO UTF
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
File_DEST_MAPPING = {
    "ABA": ["CH4/ClientFiles/", "ClientFiles/Dataset 2/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/"],
    "ABC": ["CH4/ClientFiles/", "ClientFiles/Dataset 2/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/"],
    "BVE": ["CH4/ClientFiles/", "ClientFiles/Dataset 1/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/"],
    "CEA": ["CH4/ClientFiles/", "ClientFiles/CET/", "Sky/ClientFiles/"],
    "CEA_FULL": ["CH4/ClientFiles/", "ClientFiles/CET/", "Sky/ClientFiles/"],
    "CEO": ["CH4/ClientFiles/", "ClientFiles/Overnights/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/"],
    "CET": ["CH4/ClientFiles/", "ClientFiles/CET/", "Sky/ClientFiles/"],
    "LIT": ["CH4/ClientFiles/", "ClientFiles/Dataset 2/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/"],
    "MAS": ["CH4/ClientFiles/", "ClientFiles/MAS and UNI/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/", "Sky/ClientFiles/"],
    "NLE": ["CH4/ClientFiles/", "ClientFiles/Dataset 2/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/", "Sky/ClientFiles/"],
    "OIT": ["CH4/ClientFiles/", "ClientFiles/Dataset 2/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/", "Sky/ClientFiles/"],
    "ONE": ["CH4/ClientFiles/", "ClientFiles/Dataset 2/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/"],
    "PEA": ["CH4/ClientFiles/", "ClientFiles/PET/", "ITV/ClientFiles/", "Sky/ClientFiles/"],
    "PEA_FULL": ["CH4/ClientFiles/", "ClientFiles/PET/", "ITV/ClientFiles/", "Sky/ClientFiles/"],
    "PET": ["CH4/ClientFiles/", "ClientFiles/PET/", "ITV/ClientFiles/", "Sky/ClientFiles/"],
    "PV2": ["CH4/ClientFiles/", "ClientFiles/Dataset 1/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/", "Sky/ClientFiles/"],
    "PVE": ["ClientFiles/Dataset 1/Enhanced/", "ITV/ClientFiles/", "Sky/ClientFiles/"],
    "PVX": ["CH4/ClientFiles/", "ClientFiles/Dataset 1/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/", "Sky/ClientFiles/"],
    "PXE": ["ClientFiles/Dataset 1/Enhanced/", "ITV/ClientFiles/", "Sky/ClientFiles/"],
    "SVE": ["CH4/ClientFiles/", "ClientFiles/Dataset 1/", "ITV/ClientFiles/", "Sky/ClientFiles/"],
    "TOD": ["CH4/ClientFiles/", "ClientFiles/Dataset 2/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/"],
    "UNI": ["CH4/ClientFiles/", "ClientFiles/MAS and UNI/", "ITV/ClientFiles/", "Mediaocean/ClientFiles/", "Sky/ClientFiles/"]
}

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












##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################




# ISO TO UTF ALL





# import azure.functions as func
# import logging
# import os
# import gzip
# import zlib
# from azure.storage.blob import BlobServiceClient
# import io
# from io import BytesIO
# import codecs
# import traceback
# import json
# from urllib.parse import urlparse

# # --- CONFIGURATION ---
# app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# SRC_PREFIX = os.getenv("SRC_PREFIX")
# DEST_PREFIX = os.getenv("DEST_PREFIX")
# DEST_CONTAINER = os.getenv("DEST_CONTAINER")
# STORAGE_CONN_STR = os.getenv("mainblobstorage")

# logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# CHUNK_SIZE = 1024 * 1024 * 8   # 8MB chunks for large .ABC files

# @app.route(route="http_trigger", methods=["POST"])
# def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         logging.info("Starting function...")

#         events = req.get_json()
#         if not isinstance(events, list):
#             events = [events]

#         blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
#         processed_files = []
#         logging.info("Container connection is ok")

#         for event in events:

#             # Event Grid validation handshake
#             if event.get("eventType") == "Microsoft.EventGrid.SubscriptionValidationEvent":
#                 return func.HttpResponse(
#                     json.dumps({"validationResponse": event["data"]["validationCode"]}),
#                     mimetype="application/json"
#                 )

#             # Blob created event handling
#             elif event.get("eventType") == "Microsoft.Storage.BlobCreated":

#                 blob_url = event["data"]["url"]
#                 parsed = urlparse(blob_url)
#                 container_name, blob_name = parsed.path.lstrip("/").split("/", 1)

#                 logging.info(f"[START] Processing: {blob_name}")

#                 src_blob_client = blob_service_client.get_blob_client(container_name, blob_name)
#                 dest_blob_name = f"{DEST_PREFIX}/{os.path.basename(blob_name)}"
#                 dest_blob_client = blob_service_client.get_blob_client(DEST_CONTAINER, dest_blob_name)

#                 # CASE 1: PROCESSING ".ABC"

#                 if ".ABC" in blob_name.upper():
#                     logging.info("Detected .ABC file → using streaming large-file pipeline")

#                     src_chunks = src_blob_client.download_blob().chunks()
#                     logging.info("Downloading Blob")
#                     # --- streaming gz decompressor ---
#                     def stream_gz_decompress(chunck):
#                         d = zlib.decompressobj(16 + zlib.MAX_WBITS)   # gzip format
#                         for chunk in chunck:
#                             data = d.decompress(chunk)
#                             if data:
#                                 yield data
#                         tail = d.flush()
#                         if tail:
#                             yield tail

#                     input_stream = stream_gz_decompress(src_chunks)
#                     logging.info("streaming")


#                     # Streaming encoder/decoder
#                     decoder = codecs.getincrementaldecoder("iso-8859-1")()
#                     encoder = codecs.getincrementalencoder("utf-8")()

#                     # Streaming output (compressed)
#                     out_buffer = io.BytesIO()
#                     gzip_writer = gzip.GzipFile(fileobj=out_buffer, mode="wb")

#                     for chunk in input_stream:
#                         text = decoder.decode(chunk)
#                         utf8_bytes = encoder.encode(text)
#                         gzip_writer.write(utf8_bytes)
                   
#                     gzip_writer.close()
#                     logging.info("decode and encode done")

#                     dest_blob_client.upload_blob(
#                         out_buffer.getvalue(),
#                         overwrite=True,
#                         metadata={
#                             "converted_from": "iso-8859-1",
#                             "converted_to": "utf-8",
#                             "compressed": "gz",
#                             "streaming": "true",
#                             "source_file": blob_name,
#                         },
#                     )

#                     logging.info(f"[SUCCESS] Uploaded processed .ABC blob: {dest_blob_name}")
#                     src_blob_client.delete_blob()
#                     processed_files.append(blob_name)
#                     continue  

#                 # CASE 2: NORMAL WORKLOAD FOR NON-ABC FILES
#                 logging.info("Normal file detected → using standard workload")

#                 raw_data = src_blob_client.download_blob().readall()

#                 if blob_name.endswith(".gz"):
#                     try:
#                         with gzip.GzipFile(fileobj=BytesIO(raw_data), mode="rb") as gz:
#                             raw_data = gz.read()
#                         logging.info("Successfully decompressed .gz file")
#                     except Exception as e:
#                         logging.error(f"Failed to decompress .gz: {e}")
#                         raise

#                 # Detect and normalize encoding
#                 try:
#                     decoded_text = raw_data.decode("iso-8859-1")
#                     logging.info("Decoded as ISO-8859-1")
#                 except Exception as e:
#                     logging.error("Failed to decode from ISO-8859-1")
#                     raise

#                 utf8_data = decoded_text.encode("utf-8")
#                 buffer_out = BytesIO()
#                 with gzip.GzipFile(fileobj=buffer_out, mode="wb") as gz:
#                     gz.write(utf8_data)
#                 compressed_data = buffer_out.getvalue()


#                 # Compress and upload
#                 compressed_buffer = io.BytesIO()
#                 with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as gz:
#                     gz.write(utf8_data)

#                 # Upload compressed file
#                 dest_blob_client.upload_blob(
#                     compressed_data,
#                     overwrite=True,
#                     metadata={
#                         "converted_from": "iso-8859-1",
#                         "converted_to": "utf-8",
#                         "compressed": "gz",
#                         "streaming": "False",
#                         'source_file': blob_name
#                         }
#                     )

#                 logging.info(f"[SUCCESS] Uploaded processed blob: {dest_blob_name}")

#                 src_blob_client.delete_blob()
#                 processed_files.append(blob_name)

#         return func.HttpResponse(f"Processed {len(processed_files)} files.", status_code=200)

#     except Exception:
#         logging.error("Fatal error")
#         logging.error(traceback.format_exc())
#         return func.HttpResponse("Failed", status_code=500)










##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################



# iso to utf, no abc





# import azure.functions as func
# import logging
# import os
# import gzip
# import zlib
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO
# import traceback
# import json
# from urllib.parse import urlparse

# # --- CONFIGURATION ---
# app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# # Environment variables (set these in Azure App Settings)
# SRC_PREFIX = os.getenv("SRC_PREFIX")
# DEST_PREFIX = os.getenv("DEST_PREFIX")
# DEST_CONTAINER = os.getenv("DEST_CONTAINER")
# STORAGE_CONN_STR = os.getenv("mainblobstorage")

# logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# # --- MAIN FUNCTION ---
# @app.route(route="http_trigger", methods=["POST"])
# def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         logging.info('starting the function')

#         # Parse the Event Grid message
#         events = req.get_json()
#         if not isinstance(events, list):
#             events = [events]

#         logging.info('Parsed the event grid')

#         blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
#         processed_files = []

#         logging.info('storage connection is ok')

#         for event in events:
#             # Handle Event Grid validation handshake
#             if event.get("eventType") == "Microsoft.EventGrid.SubscriptionValidationEvent":
#                 validation_code = event["data"]["validationCode"]
#                 logging.info(f"Responding to Event Grid validation with code: {validation_code}")
#                 return func.HttpResponse(
#                     json.dumps({"validationResponse": validation_code}),
#                     mimetype="application/json"
#                 )

#             # Handle blob created event
#             elif event.get("eventType") == "Microsoft.Storage.BlobCreated":
#                 blob_url = event["data"]["url"]
#                 parsed_url = urlparse(blob_url)
#                 path_parts = parsed_url.path.lstrip("/").split("/", 1)
#                 container_name = path_parts[0]
#                 blob_name = path_parts[1]

#                 logging.info(f"[START] Processing blob: {blob_name} in container: {container_name}")

#                 # Download source blob
#                 src_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
#                 raw_data = src_blob_client.download_blob().readall()
#                 logging.info(f"Downloaded {len(raw_data)} bytes from {blob_name}")

#                 if blob_name.endswith(".gz"):
#                     try:
#                         with gzip.GzipFile(fileobj=BytesIO(raw_data), mode="rb") as gz:
#                             raw_data = gz.read()
#                         logging.info("Successfully decompressed .gz file")
#                     except Exception as e:
#                         logging.error(f"Failed to decompress .gz: {e}")
#                         raise
                    
#                 # Detect and normalize encoding
#                 try:
#                     decoded_text = raw_data.decode("iso-8859-1")
#                     logging.info("Decoded as ISO-8859-1")
#                 except Exception as e:
#                     logging.error("Failed to decode from ISO-8859-1")
#                     raise

#                 utf8_data = decoded_text.encode("utf-8")
#                 buffer_out = BytesIO()
#                 with gzip.GzipFile(fileobj=buffer_out, mode="wb") as gz:
#                     gz.write(utf8_data)
#                 compressed_data = buffer_out.getvalue()

#                 dest_blob_name = f"{DEST_PREFIX}/{os.path.basename(blob_name)}"
#                 dest_blob_client = blob_service_client.get_blob_client(
#                     container=DEST_CONTAINER, blob=dest_blob_name
#                 )
#                 # Upload compressed file
#                 dest_blob_client.upload_blob(
#                     compressed_data,
#                     overwrite=True,
#                     metadata={
#                         "converted_from": "iso-8859-1",
#                         "converted_to": "utf-8",
#                         "compressed": "gz",
#                         'source_file': blob_name
#                         }
#                     )

#                 logging.info(f"[SUCCESS] Uploaded processed blob: {dest_blob_name}")
#                 processed_files.append(blob_name)

#                 src_blob_client.delete_blob()
#                 logging.info(f"[DELETE] Deleted source blob: {blob_name}")

#         return func.HttpResponse(f"Processed {len(processed_files)} files", status_code=200)

#     except Exception:
#         logging.error("[FATAL] HTTP function failed.")
#         logging.error(traceback.format_exc())
#         return func.HttpResponse("Failed", status_code=500)









##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################
##############################################################################################



# utf to iso





# import azure.functions as func
# import logging
# import os
# import gzip
# import zlib
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO
# import traceback
# import json
# from urllib.parse import urlparse

# # --- CONFIGURATION ---
# app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# # Environment variables (set these in Azure App Settings)
# SRC_PREFIX = os.getenv("SRC_PREFIX")
# DEST_PREFIX = os.getenv("DEST_PREFIX")
# DEST_CONTAINER = os.getenv("DEST_CONTAINER")
# STORAGE_CONN_STR = os.getenv("mainblobstorage")

# logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# # --- MAIN FUNCTION ---
# @app.route(route="http_trigger", methods=["POST"])
# def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
#     try:
#         logging.info('starting the function')

#         # Parse the Event Grid message
#         events = req.get_json()
#         if not isinstance(events, list):
#             events = [events]

#         logging.info('Parsed the event grid')

#         blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
#         processed_files = []

#         logging.info('storage connection is ok')

#         for event in events:
#             # Handle Event Grid validation handshake
#             if event.get("eventType") == "Microsoft.EventGrid.SubscriptionValidationEvent":
#                 validation_code = event["data"]["validationCode"]
#                 logging.info(f"Responding to Event Grid validation with code: {validation_code}")
#                 return func.HttpResponse(
#                     json.dumps({"validationResponse": validation_code}),
#                     mimetype="application/json"
#                 )

#             # Handle blob created event
#             elif event.get("eventType") == "Microsoft.Storage.BlobCreated":
#                 blob_url = event["data"]["url"]
#                 parsed_url = urlparse(blob_url)
#                 path_parts = parsed_url.path.lstrip("/").split("/", 1)
#                 container_name = path_parts[0]
#                 blob_name = path_parts[1]

#                 logging.info(f"[START] Processing blob: {blob_name} in container: {container_name}")

#                 # Prepare destination blob
#                 dest_blob_name = f"{DEST_PREFIX}/{os.path.basename(blob_name)}"
#                 dest_blob_client = blob_service_client.get_blob_client(
#                     container=DEST_CONTAINER,
#                     blob=dest_blob_name
#                 )

#                 # Download source blob
#                 src_blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
#                 raw_data = src_blob_client.download_blob().readall()
#                 logging.info(f"Downloaded {len(raw_data)} bytes from {blob_name}")

#                 # Attempt decompression for .Z files
#                 if blob_name.endswith(".Z"):
#                     try:
#                         raw_data = zlib.decompress(raw_data, 15 + 32)
#                         logging.info("Decompression successful")
#                     except Exception as dec_err:
#                         logging.warning(f"Decompression failed, might already be uncompressed: {dec_err}")

#                 # Detect and normalize encoding
#                 decoded_text = None
#                 used_decoding = "binary"
#                 for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
#                     try:
#                         decoded_text = raw_data.decode(encoding)
#                         used_decoding = encoding
#                         break
#                     except (UnicodeDecodeError, AttributeError):
#                         continue

#                 # Re-encode and compress
#                 try:
#                     iso_encoded = decoded_text.encode('iso-8859-1', errors='strict') if decoded_text else raw_data
#                     used_encoding = 'iso-8859-1'
#                 except UnicodeEncodeError:
#                     iso_encoded = decoded_text.encode('utf-8') if decoded_text else raw_data
#                     used_encoding = 'utf-8'

#                 compressed_buffer = BytesIO()
#                 with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
#                     gz.write(iso_encoded)
#                 compressed_data = compressed_buffer.getvalue()

#                 # Upload compressed file
#                 dest_blob_client.upload_blob(
#                     compressed_data,
#                     overwrite=True,
#                     metadata={
#                         'original_encoding': used_decoding,
#                         'compressed_encoding': used_encoding,
#                         'source_file': blob_name
#                         }
#                     )

#                 logging.info(f"[SUCCESS] Uploaded processed blob: {dest_blob_name}")
#                 processed_files.append(blob_name)

#                 src_blob_client.delete_blob()
#                 logging.info(f"[DELETE] Deleted source blob: {blob_name}")

#         return func.HttpResponse(f"Processed {len(processed_files)} files", status_code=200)

#     except Exception:
#         logging.error("[FATAL] HTTP function failed.")
#         logging.error(traceback.format_exc())
#         return func.HttpResponse("Failed", status_code=500)



