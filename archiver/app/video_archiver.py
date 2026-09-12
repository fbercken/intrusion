import os, time, pathlib
import json
import boto3
import threading
from typing import Dict, List
from botocore.config import Config


# S3 Data Fabric Configurations
S3_ARCHIVER = os.getenv("ARCHIVER","myArchiver")
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL","http://datafabric01:9000")
ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID","")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY","")
S3_BUCKET = os.getenv("S3_BUCKET_NAME","video_archive")
S3_PREFIX = os.getenv("S3_PRREFIX","test")
SRC_FOLDER = os.getenv("SRC_FOLDER","/tmp")


class S3Archiver(threading.Thread):
    thread_store: Dict[str, List[threading.Thread]] = {}

    def __init__(self, archiver: str, folder: str, bucket: str, prefix: str):
        super().__init__()
        self.archiver = archiver
        self.bucket = bucket
        self.prefix = prefix
        self.sourceDirectory = pathlib.Path(folder)
        self.sourceDirectory.mkdir(exist_ok=True)
        print(f"{self.archiver} is initialized")


    def connect(self, endPoint: str | None, accessKey: str, secretKey: str):

        self._s3_client = boto3.client(
            's3',
            endpoint_url=endPoint,
            aws_access_key_id=accessKey,
            aws_secret_access_key=secretKey,
            config=Config(signature_version='s3v4', s3={'addressing_style': 'path'})
        )

    def run(self):
        while True:
            for f in self.sourceDirectory.glob("*.mp4"):
                if time.time() - f.stat().st_mtime > 60:
                    key = f"{self.prefix}/{f.name.split('_')[0]}/{f.name}"
                    self._s3_client.upload_file(str(f), self.bucket, key)
                    print(key)
                    f.unlink()
            time.sleep(15)


def main():
    myS3Archiver = S3Archiver(S3_ARCHIVER, SRC_FOLDER,  S3_BUCKET, S3_PREFIX)
    myS3Archiver.connect(S3_ENDPOINT, ACCESS_KEY_ID , SECRET_KEY)
    myS3Archiver.start()
    myS3Archiver.join()


if __name__ == "__main__":
    main()
