from minio import Minio
import os
from dotenv import load_dotenv
load_dotenv()

def setup_minio_client():
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    return minio_client
    