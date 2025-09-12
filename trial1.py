import urllib.parse
import json
import boto3
from botocore.config import Config
import os

prompt_template = """
You are a file path transformation system. Given a source file name and folder path, extract key info and return a JSON with the new file name and path.

Rules:
- For files starting with SAM_, output file name as [blinding status]_[dataset]_[date].[ext] and path as rtft/[study name]/[vendor]/
- If not SAM_, keep original file name, path as rtft/[study name]/[vendor]/ (or unknown if missing)
- Convert dates like 2023APR18 to 20230418 (APR->04)
- Use placeholders unknowndataset or unknownvendor if not found

Example:
Input:
Source File Name: SAM_P23-380_TEST_TV_BLINDED_UC lab_20231030.csv
Source File Path: samprod-fileingestion/P23-380/
Output: {"Target File Name": "BLINDED_TV_20231030.csv", "Target File Path": "rtft/P23-380/UC lab/"}

Return only the JSON object as output.
"""

sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

# Configure Bedrock client with hardcoded Inference Profile
config = Config(
    region_name='us-east-1',
    inference_profile={
        'inference_profile_arn': "arn:aws:bedrock:us-east-1:419835568062:application-inference-profile/out5xci4bakz",
        'inference_profile_id': "out5xci4bakz"
    }
)

bedrock_client = boto3.client(
    service_name='bedrock-runtime',
    config=config
)

def lambda_handler(event, context):
    # Validate required environment variables
    required_env_vars = [
        'SQS_QUEUE_NAME',
        'DESTINATION_BUCKET_NAME'
    ]
    
    missing_vars = [var for var in required_env_vars if var not in os.environ]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        print(error_msg)
        return {
            'statusCode': 500,
            'body': json.dumps(error_msg)
        }
    
    try:
        sqs_queue_url = sqs_client.get_queue_url(QueueName=os.environ['SQS_QUEUE_NAME'])['QueueUrl']
    except Exception as e:
        print(f"Error getting SQS Queue URL: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('SQS queue URL not found.')
        }

    for record in event['Records']:
        try:
            message_body = json.loads(record['body'])
            s3_event = message_body['Records'][0]
            source_bucket = s3_event['s3']['bucket']['name']
            source_key = urllib.parse.unquote_plus(s3_event['s3']['object']['key'])
            
            source_file_name = os.path.basename(source_key)
            
            # This logic extracts the path part of the S3 key
            source_file_path = os.path.dirname(source_key) + '/'

            # Construct the final prompt for the LLM
            full_prompt = f"{prompt_template}\n\nInput:\nSource File Name: {source_file_name}\nSource File Path: {source_file_path}\nOutput:"

            # The issue is likely in the JSON body below

            # Payload for Sonnet model
            body_dict = {
                "inputText": full_prompt,
                "textGenerationConfig": {
                    "maxTokenCount": 512,
                    "temperature": 0.0,
                    "topP": 0.9
                }
            }
            print("DEBUG: Bedrock payload:", json.dumps(body_dict, indent=2))
            body = json.dumps(body_dict)

            print("DEBUG: Invoking Bedrock model...")
            response = bedrock_client.invoke_model(
                body=body,
                modelId="anthropic.claude-sonnet-4-20250514-v1:0",
                accept="application/json",
                contentType="application/json"
            )

            # Read and decode the response body
            response_text = response['body'].read().decode('utf-8')
            print(f"DEBUG: Raw response: {response_text[:200]}...")
            
            response_body = json.loads(response_text)
            if 'completion' in response_body:
                llm_output_text = response_body['completion']
            else:
                print(f"DEBUG: Unexpected response keys: {list(response_body.keys())}")
                raise ValueError(f"Unexpected response structure: {json.dumps(response_body)}")
            
            # Isolate and parse the JSON string from the LLM output
            json_start = llm_output_text.find('{')
            json_end = llm_output_text.rfind('}') + 1
            json_string = llm_output_text[json_start:json_end]

            llm_output = json.loads(json_string)
            
            target_file_path = llm_output['Target File Path'].replace("rtft/", "")
            target_file_name = llm_output['Target File Name']
            
            new_key = target_file_path + target_file_name
            destination_bucket = os.environ['DESTINATION_BUCKET_NAME']

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
            print(f"Error processing S3 object {source_key}: {e}")
            import traceback
            traceback.print_exc()
            
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete!')
    }