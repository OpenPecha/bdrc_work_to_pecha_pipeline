import configparser
import os

import boto3

BDRC_ARCHIVE_BUCKET = "archive.tbrc.org"
OCR_OUTPUT_BUCKET = "ocr.bdrc.io"

aws_credentials_file = os.path.expanduser("~/.aws/credentials")
config = configparser.ConfigParser()
config.read(aws_credentials_file)

aws_access_key_id = config.get(
    "archive_tbrc_org", "aws_access_key_id", fallback="DUMMY_ACCESS_KEY"
)
aws_secret_access_key = config.get(
    "archive_tbrc_org", "aws_secret_access_key", fallback="DUMMY_SECRET_KEY"
)

bdrc_archive_session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

s3_client = bdrc_archive_session.client("s3")
s3_resource = bdrc_archive_session.resource("s3")
bdrc_archive_bucket = s3_resource.Bucket(BDRC_ARCHIVE_BUCKET)
ocr_output_bucket = s3_resource.Bucket(OCR_OUTPUT_BUCKET)
