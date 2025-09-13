import urllib.parse
import json
import boto3
from botocore.config import Config
import os
import traceback

# CORRECTED: Improved prompt to ensure only JSON is returned, making parsing more reliable.
prompt_template = """
Transform the source file info into a JSON object with "Target File Name" and "Target File Path" keys.

Rules:

For files starting with SAM_:

Target File Name: [blinding status]_[dataset]_[date].[ext]

Target File Path: rtft/[study name]/[vendor]/

Date Format: Convert dates like 2023APR18 to 20230418.

Placeholders: Use unknowndataset or unknownvendor if info isn't in the filename.

For all other files:

Target File Name: Keep the original filename.

Target File Path: rtft/[study name]/[vendor]/. Use unknownstudy or unknownvendor as placeholders if they cannot be found in the source path or filename.

if pattent didnt match try finding corrosponding values,sample Values to help identify if its not in same ordered as shown:

study_name: B15-845, ABT-199, P23-380

dataset: DA, RNKIT, MAGEINV, TV

vendor: UC lab, LBC, EPC, ABBV

blinding status: BLINDED, UNBLINDED

Your entire response must be ONLY the JSON object.

Examples:

Example 1:

Input: SAM_P23-380_TEST_TV_BLINDED_UC lab_20231030.csv, samprod-fileingestion/P23-380/

Output: {"Target File Name": "BLINDED_TV_20231030.csv", "Target File Path": "rtft/P23-380/UC lab/"}

Example 2:

Input: SAM_Mock Study 34_TEST_RNKIT_UNBLINDED_EPC_2023APR18.txt, samprod-fileingestion/Mock Study 34/

Output: {"Target File Name": "UNBLINDED_RNKIT_20230418.txt", "Target File Path": "rtft/Mock Study 34/EPC/"}

Example 3:

Input: my_report.pdf, samprod-fileingestion/Mock Study 33/

Output: {"Target File Name": "my_report.pdf", "Target File Path": "rtft/Mock Study 33/unknownvendor/"}
"""

sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

# Configure Bedrock client with region
config = Config(
    region_name=os.environ.get('AWS_REGION', 'us-east-1')
)

bedrock_client = boto3.client(
    service_name='bedrock-runtime',
    config=config
)

def lambda_handler(event, context):
    # Validate required environment variables
    required_env_vars = ['SQS_QUEUE_NAME', 'DESTINATION_BUCKET_NAME']
    missing_vars = [var for var in required_env_vars if var not in os.environ]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        print(error_msg)
        return {'statusCode': 500, 'body': json.dumps(error_msg)}
    
    try:
        sqs_queue_url = sqs_client.get_queue_url(QueueName=os.environ['SQS_QUEUE_NAME'])['QueueUrl']
    except Exception as e:
        print(f"Error getting SQS Queue URL: {e}")
        return {'statusCode': 500, 'body': json.dumps('SQS queue URL not found.')}

    for record in event['Records']:
        try:
            # Handle both SQS-wrapped S3 events and direct S3 events
            if 's3' in record:
                s3_event = record
            else:
                # SQS event: extract S3 event from message body
                message_body = record.get('body')
                if message_body:
                    body_json = json.loads(message_body)
                    # S3 event notification structure
                    s3_event = body_json['Records'][0]['s3'] if 'Records' in body_json and 's3' in body_json['Records'][0] else None
                else:
                    s3_event = None

            if not s3_event:
                raise KeyError("No S3 event found in record")

            source_bucket = s3_event['bucket']['name']
            source_key = urllib.parse.unquote_plus(s3_event['object']['key'])

            source_file_name = os.path.basename(source_key)
            source_file_path = os.path.dirname(source_key) + '/'

            full_prompt = f"{prompt_template}\n\nInput:\nSource File Name: {source_file_name}\nSource File Path: {source_file_path}\nOutput:"

            body_dict = {
                "anthropic_version": "bedrock-2023-05-31",
                "messages": [
                    {"role": "user", "content": full_prompt}
                ],
                "max_tokens": 512,
                "temperature": 0.0,
                "top_p": 0.9
            }
            body = json.dumps(body_dict)

            print("DEBUG: Invoking Bedrock model...")
            response = bedrock_client.invoke_model(
                body=body,
                modelId="arn:aws:bedrock:us-east-1:419835568062:application-inference-profile/out5xci4bakz",
                accept="application/json",
                contentType="application/json"
            )

            response_body = json.loads(response['body'].read().decode('utf-8'))
            if 'content' in response_body and response_body['content']:
                llm_output_text = response_body['content'][0]['text']
            else:
                raise ValueError(f"Unexpected response structure from Bedrock: {json.dumps(response_body)}")

            print(f"DEBUG: Raw LLM output text: {llm_output_text}")
            llm_output = json.loads(llm_output_text)

            target_file_path = llm_output['Target File Path'].replace("rtft/", "")
            target_file_name = llm_output['Target File Name']

            new_key = os.path.join(target_file_path, target_file_name)
            # Strip whitespace from bucket name to avoid invalid bucket errors
            destination_bucket = os.environ['DESTINATION_BUCKET_NAME'].strip()

            print(f"DEBUG: Copying from s3://{source_bucket}/{source_key} to s3://{destination_bucket}/{new_key}")
            s3_client.copy_object(
                Bucket=destination_bucket,
                CopySource={'Bucket': source_bucket, 'Key': source_key},
                Key=new_key
            )

            print(f"File {source_key} copied to {new_key} in {destination_bucket}")

            sqs_client.delete_message(
                QueueUrl=sqs_queue_url,
                ReceiptHandle=record['receiptHandle']
            )

        except Exception as e:
            print(f"Error processing message: {e}")
            traceback.print_exc()
            
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete!')
    }
