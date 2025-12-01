import azure.functions as func
import logging
import os
import gzip
import zlib
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import traceback
import datetime
import logging
import azure.core.pipeline.policies

logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

app = func.FunctionApp()

CRON1 = "0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58 10-23 * * *"
CRON2 = "15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59 9 * * *" 
TIMEZONE = "Europe/London"
SRC_PREFIX = os.getenv("SRC_PREFIX")
DEST_PREFIX = os.getenv("DEST_PREFIX")
CONTAINER = os.getenv("container_name")

def process_blobs():
    processed_files = []
    try:
        logging.info('Starting blob processing.')
        conn_str = os.getenv("mainblobstorage")
        logging.info(f"conn_str: {'Connected' if conn_str else 'MISSING'}")

        cont_cli = BlobServiceClient.from_connection_string(conn_str).get_container_client(CONTAINER)
        blobs = list(cont_cli.list_blobs(name_starts_with=SRC_PREFIX))

        for blob in blobs:
            try:
                if not blob.name.endswith(".Z"):
                    logging.info(f"Skipping non-Z file: {blob.name}")
                    continue
                dest_blob_name = f"{DEST_PREFIX}/{os.path.basename(blob.name)}"
                dest_blob_client = cont_cli.get_blob_client(dest_blob_name)

                # Download blob
                blob_client = cont_cli.get_blob_client(blob.name)
                raw_data = blob_client.download_blob().readall()
                logging.info(f"Downloaded {len(raw_data)} bytes from {blob.name}")

                # Decompress .Z
                if blob.name.endswith(".Z"):
                    try:
                        raw_data = zlib.decompress(raw_data, 15 + 32)
                        logging.info("Decompression successful")
                    except Exception as dec_err:
                        logging.warning(f"Decompression failed, might already be uncompressed: {dec_err}")

                decoded_text = None
                for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
                    try:
                        decoded_text = raw_data.decode(encoding)
                        used_decoding = encoding
                        break
                    except (UnicodeDecodeError, AttributeError):
                        continue

                try:
                    iso_encoded = decoded_text.encode('iso-8859-1', errors='strict')
                    used_encoding = 'iso-8859-1'
                except UnicodeEncodeError:
                    # Fallback if the data couldn't be represented in ISO-8859-1
                    iso_encoded = decoded_text.encode('utf-8')
                    used_encoding = 'utf-8'

                # Gzip compress
                compressed_buffer = BytesIO()
                with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
                    gz.write(iso_encoded)
                compressed_data = compressed_buffer.getvalue()

                # Upload
                dest_blob_client.upload_blob(
                    compressed_data,
                    overwrite=False,
                    metadata={
                        'original_size': str(len(raw_data)),
                        'compressed_size': str(len(compressed_data)),
                        'encoding':  used_decoding,
                        'compressed_encoding': used_encoding,
                        'source_file': blob.name
                    }
                )
                logging.info(f"[SUCCESS] Uploaded: {dest_blob_name}")
                blob_client.delete_blob() # DELETE SOURCE
                logging.info(f"Deleted source blob: {blob.name}")
                processed_files.append(blob.name)

            except Exception:
                logging.error(f"[ERROR] Failed processing blob: {blob.name}")
                logging.error(traceback.format_exc())

    except Exception:
        logging.error("Blob processing failed")
        logging.error(traceback.format_exc())

    return processed_files


@app.function_name(name="encoder_timer_1")
@app.schedule(schedule=CRON1, arg_name="timer", run_on_startup=False, use_monitor=True, timezone=TIMEZONE)
def encoder_timer_1(timer: func.TimerRequest):
    try:
        logging.info("Timer 1 triggered")
        processed = process_blobs()
        logging.info(f"[{datetime.datetime.now()}] Schedule 1 ran — processed: {len(processed)} files")
    except Exception as e:
        logging.error("Timer 1 failed immediately!")
        logging.error(str(e))
        logging.error(traceback.format_exc())


@app.function_name(name="encoder_timer_2")
@app.schedule(schedule=CRON2, arg_name="timer", run_on_startup=False, use_monitor=True, timezone=TIMEZONE)
def encoder_timer_2(timer: func.TimerRequest):
    try:
        logging.info("Timer 2 triggered")
        processed = process_blobs()
        logging.info(f"[{datetime.datetime.now()}] Schedule 2 ran — processed: {len(processed)} files")
    except Exception as e:
        logging.error("Timer 2 failed immediately!")
        logging.error(str(e))
        logging.error(traceback.format_exc())










# import azure.functions as func
# import logging
# import os
# import gzip
# import zlib
# from azure.storage.blob import BlobServiceClient
# from io import BytesIO
# import traceback
# import datetime
# import logging
# import azure.core.pipeline.policies

# logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# app = func.FunctionApp()

# CRON1 = "0,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,54,56,58 10-23 * * *"
# CRON2 = "15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59 9 * * *" 
# TIMEZONE = "Europe/London"
# SRC_PREFIX = os.getenv("SRC_PREFIX") #test dep
# DEST_PREFIX = os.getenv("DEST_PREFIX")
# CONTAINER = os.getenv("container_name")

# def process_blobs():
#     processed_files = []
#     try:
#         logging.info('Starting blob processing.')
#         conn_str = os.getenv("mainblobstorage")
#         logging.info(f"conn_str: {conn_str[:10] if conn_str else 'MISSING'}")

#         cont_cli = BlobServiceClient.from_connection_string(conn_str).get_container_client(CONTAINER)
#         blobs = list(cont_cli.list_blobs(name_starts_with=SRC_PREFIX))

#         for blob in blobs:
#             try:
#                 dest_blob_name = f"{DEST_PREFIX}/{os.path.basename(blob.name)}"
#                 dest_blob_client = cont_cli.get_blob_client(dest_blob_name)

#                 # Skip if destination exists
#                 if dest_blob_client.exists():
#                     logging.info(f"[SKIP] Destination blob already exists: {dest_blob_name}")
#                     continue

#                 # Download blob
#                 blob_client = cont_cli.get_blob_client(blob.name)
#                 raw_data = blob_client.download_blob().readall()
#                 logging.info(f"Downloaded {len(raw_data)} bytes from {blob.name}")

#                 # Decompress .Z
#                 if blob.name.endswith(".Z"):
#                     try:
#                         raw_data = zlib.decompress(raw_data, 15 + 32)
#                         logging.info("Decompression successful")
#                     except Exception as dec_err:
#                         logging.warning(f"Decompression failed, might already be uncompressed: {dec_err}")

#                 decoded_text = None
#                 for encoding in ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']:
#                     try:
#                         decoded_text = raw_data.decode(encoding)
#                         used_decoding = encoding
#                         break
#                     except (UnicodeDecodeError, AttributeError):
#                         continue

#                 try:
#                     iso_encoded = decoded_text.encode('iso-8859-1', errors='strict')
#                     used_encoding = 'iso-8859-1'
#                 except UnicodeEncodeError:
#                     # Fallback if the data couldn't be represented in ISO-8859-1
#                     iso_encoded = decoded_text.encode('utf-8')
#                     used_encoding = 'utf-8'

#                 # Gzip compress
#                 compressed_buffer = BytesIO()
#                 with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
#                     gz.write(iso_encoded)
#                 compressed_data = compressed_buffer.getvalue()

#                 # Upload
#                 dest_blob_client.upload_blob(
#                     compressed_data,
#                     overwrite=False,
#                     metadata={
#                         'original_size': str(len(raw_data)),
#                         'compressed_size': str(len(compressed_data)),
#                         'encoding':  used_decoding,
#                         'compressed_encoding': used_encoding,
#                         'source_file': blob.name
#                     }
#                 )
#                 logging.info(f"[SUCCESS] Uploaded: {dest_blob_name}")
#                 processed_files.append(blob.name)

#             except Exception:
#                 logging.error(f"[ERROR] Failed processing blob: {blob.name}")
#                 logging.error(traceback.format_exc())

#     except Exception:
#         logging.error("[FATAL] Blob processing failed.")
#         logging.error(traceback.format_exc())

#     return processed_files


# @app.function_name(name="encoder_timer_1")
# @app.schedule(schedule=CRON1, arg_name="timer", run_on_startup=False, use_monitor=True, timezone=TIMEZONE)
# def encoder_timer_1(timer: func.TimerRequest):
#     try:
#         logging.info("Timer 1 triggered")
#         processed = process_blobs()
#         logging.info(f"[{datetime.datetime.now()}] Schedule 1 ran — processed: {len(processed)} files")
#     except Exception as e:
#         logging.error("Timer 1 failed immediately!")
#         logging.error(str(e))
#         logging.error(traceback.format_exc())


# @app.function_name(name="encoder_timer_2")
# @app.schedule(schedule=CRON2, arg_name="timer", run_on_startup=False, use_monitor=True, timezone=TIMEZONE)
# def encoder_timer_2(timer: func.TimerRequest):
#     try:
#         logging.info("Timer 2 triggered")
#         processed = process_blobs()
#         logging.info(f"[{datetime.datetime.now()}] Schedule 2 ran — processed: {len(processed)} files")
#     except Exception as e:
#         logging.error("Timer 2 failed immediately!")
#         logging.error(str(e))
#         logging.error(traceback.format_exc())

