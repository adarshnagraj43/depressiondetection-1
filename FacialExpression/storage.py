from pathlib import Path
import traceback
from google.cloud.exceptions import GoogleCloudError, NotFound
import os
from sqlalchemy import *
import json
from google.cloud import bigquery
from google.cloud import storage
import time
import sys
import copy
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import jsonpickle
from datetime import datetime
import argparse




class Storage:
    def __init__(self, google_key_path='/home/adarsh/PycharmProjects/project/emotion_and_depression_detection/cloud.json'):
        if google_key_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path
            self.bigquery_client = bigquery.Client()
        else:
            self.bigquery_client = None

    def read_files(self, filename, preprocessor=False):
        data = []
        with open(filename) as f:
            for line in f:
                if preprocessor:
                    data.extend(jsonpickle.decode(line)['messages'])
                else:
                    data.extend(jsonpickle.decode(line))
        return filename, data

    def read_data_bucket(self, bucket_name, prefix, preprocessor=False):
        # downloads all file names in a bucket and returns list of filenames.
        try:
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            file_list = list(bucket.list_blobs(prefix=prefix))
            file_list = [str(blob.id).split("/")[-2] for blob in file_list]
            file_list = [file_name for file_name in file_list if file_name]
            for file_name in file_list:
                blob = bucket.blob(prefix + '/' + file_name)
                with open(file_name, "wb") as f:
                    blob.download_to_file(f)
                yield self.read_files(file_name, preprocessor)
            else:
                return []
        except Exception as e:
            print(e)
            raise e

    def upload_to_cloud_storage(self, google_key_path, bucket_name, source, destination):
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = google_key_path

        try:
            client = storage.Client()
            bucket = client.get_bucket(bucket_name)
            blob = bucket.blob(destination)
            blob.upload_from_filename(source)

            return True
        except GoogleCloudError as e:
            print(str(e))
            return None
        except NotFound as e:
            print(str(e))
            return None

    def write_bucket(self, args, destination_bucket_name, prefix, data, folder_name=None):
        # creates and writes a file in a bucket with the json data passed
        filename = '{}_{}.json'.format(prefix, datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S"))
        with open(filename, 'w') as fp:
            json.dump(data, fp)
        try:
            if folder_name:
                status = self.upload_to_cloud_storage(args.google_key_path, destination_bucket_name, source=filename,
                                                      destination=folder_name + "/" + filename)
            else:
                status = self.upload_to_cloud_storage(args.google_key_path, destination_bucket_name, source=filename,
                                                      destination=prefix + '/' + filename)
            os.remove(filename)
            if not status:
                raise
            return True
        except Exception as e:
            if os.path.exists(filename):
                os.remove(filename)
            print(e)
            raise e

if __name__ == "__main__":
    pass