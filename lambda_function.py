import boto3
import gnupg
import os

excluded_file_exts = [] # file extensions to exclude from processing
param_name = "SSM_PARAM_NAME"
dest_bucket = "DEST_BUCKET_NAME"
sns_topic_arn = 'SNS_TOPIC_ARN'

# path to archive successfully decrypted files in source bucket
archived_path = "archived/"

file_base_dir = '/tmp/'
decrypt_suffix = '.decrypt'

ssm_client = boto3.client('ssm')
s3_client = boto3.client('s3')

gpg = gnupg.GPG(gnupghome="/tmp", gpgbinary='/opt/bin/gpg')
gpg.encoding = 'utf-8'

def lambda_handler(event, context):
    
    # Access the value of the private key from parameter store
    private_key_string = ssm_client.get_parameter(Name=param_name, WithDecryption=True)['Parameter']['Value']
    gpg.import_keys(private_key_string)
    
    # get file info from trigger
    source_bucket = event["Records"][0]['s3']['bucket']['name']
    key = event["Records"][0]['s3']['object']['key']
    etag = event["Records"][0]['s3']['object']['eTag']
    file_name = key.split('/')[-1]
    encrypted_file_ext = file_name.split('.')[-1]
    
    # doesn't allow files with extensions in excluded_file_exts to be processed
    if encrypted_file_ext in excluded_file_exts:
        return {
            'message': f"File extension for {key} is not allowed"
        }

    
    # check if encrypted file is already processed
    response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=archived_path+file_name)
    if 'Contents' in response:
        for obj in response['Contents']:
           if obj['ETag'].strip('"') == etag:
               return {
                    'message': f"File has already been decrypted successfully: {file_name}"
                }
        

    encrypted_file_path = file_base_dir+file_name
    decrypted_file_name = file_name + decrypt_suffix
    decrypted_file_path = file_base_dir+decrypted_file_name
    
    # download file from source_bucket
    s3_client.download_file(source_bucket, key, encrypted_file_path)
    
    # decrypt the downloaded file
    decrypted_data = gpg.decrypt_file(encrypted_file_path, output=decrypted_file_path)
    
    if decrypted_data.ok:
        # upload the decrypted file to dest_bucket
        s3_client.upload_file(decrypted_file_path, dest_bucket, decrypted_file_name)
        
        # move encrypted file from incoming to archived
        s3_client.copy_object(Bucket=source_bucket, CopySource={'Bucket': source_bucket, 'Key': key}, Key=archived_path+file_name)
        s3_client.delete_object(Bucket=source_bucket, Key=key)
        res_message = f"File decrypted successfully: {file_name}"
    else:
        # send sns notification when decrypt fails
        sns_client = boto3.client('sns')

        # Define the message and subject
        subject = 'Automated Decrypt Failed'
        res_message = f"{key} file in bucket {source_bucket} FAILED to decrypt!"

        # Publish a message to the specified topic
        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=res_message,
            Subject=subject
        )

    # clean up
    if os.path.isfile(encrypted_file_path): os.remove(encrypted_file_path)
    if os.path.isfile(decrypted_file_path): os.remove(decrypted_file_path)

    return {
        'message': res_message
    } 