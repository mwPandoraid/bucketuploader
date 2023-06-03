from google.cloud import storage
import argparse
import os
import threading
import math

parser = argparse.ArgumentParser(description="Example: uploader.py /home/usr/files",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument("path", help="Path to directory you want to upload from")
parser.add_argument("bucket", help="GCP Bucket you want to upload to")
parser.add_argument("-t", "--threads", default=2, type=int, help="Amount of threads you want to run.")

args = parser.parse_args()

config = vars(args)

print(config)

def upload_blob(bucket_name, source_file_name, thread):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob("bucket/" + source_file_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    generation_match_precondition = 0

    blob.upload_from_filename(config["path"] + "\\" + source_file_name)

    print(
        f"THREAD {thread}: File {source_file_name} uploaded to {bucket_name}."
    )

def upload_loop(start, end, thread):
    end = end
    print(f"THREAD {thread}: {os.listdir(config['path'])[start:end + 1]}")
    for filename in os.listdir(config["path"])[start:end]:
        upload_blob(config["bucket"], filename, thread)

threads = config["threads"]
filelist = os.listdir(config["path"])
workload = math.floor((len(filelist) / (threads)))

for i in range(threads):
    start = workload * i
    end = start + workload
    thread = threading.Thread(target=upload_loop, args=(start, end, i))
    print(f"Spawned thread number {i} with workload: {start}-{end}")
    thread.start()

#cleanup thread
finishedfiles = workload * threads
thread = threading.Thread(target=upload_loop, args=(finishedfiles, len(filelist), threads))
print(f"Spawned thread number {threads} with workload: {finishedfiles}-{len(filelist)}")
thread.start()

