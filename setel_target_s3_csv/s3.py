from target_s3_csv.s3 import *


def tagset_to_string(tagset):
    result = ""
    for tag in tagset:
        result += f'''{tag["Key"]}={tag["Value"]}&'''
    return result[:-1]


@retry_pattern()
def upload_file_with_tagset(filename, s3_client, bucket, s3_key, tagset,
                            encryption_type=None, encryption_key=None):
    if encryption_type is None or encryption_type.lower() == "none":
        # No encryption config (defaults to settings on the bucket):
        encryption_desc = ""
        encryption_args = None
    else:
        if encryption_type.lower() == "kms":
            encryption_args = {"ServerSideEncryption": "aws:kms"}
            if encryption_key:
                encryption_desc = (
                    " using KMS encryption key ID '{}'"
                        .format(encryption_key)
                )
                encryption_args["SSEKMSKeyId"] = encryption_key
            else:
                encryption_desc = " using default KMS encryption"
        else:
            raise NotImplementedError(
                "Encryption type '{}' is not supported. "
                "Expected: 'none' or 'KMS'"
                    .format(encryption_type)
            )
    LOGGER.info(
        "Uploading {} to bucket {} at {}{}"
            .format(filename, bucket, s3_key, encryption_desc)
    )
    if encryption_args is None:
        encryption_args = {}
    with open(filename, "rb") as f:
        s3_client.put_object(Body=f, Bucket=bucket, Key=s3_key, **encryption_args, Tagging=tagset_to_string(tagset))
