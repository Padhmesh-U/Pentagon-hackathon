import json
import boto3
import os

# The full prompt, which acts as the core instruction set for the LLM.
# It is defined globally to be initialized only once per Lambda execution environment.
prompt_template = """
Instruction:
You are a highly reliable and expert file path transformation system. Your sole task is to analyze a source file name and folder path, extract key information, and then format it into a new target file path and name. You must provide your output in a strict JSON format.
Input:
Source File Name: SAM_<<study_name>>_<<env>>_<<dataset>>_<<blinded/unblinded>>_<<vendor>>_yyyymmdd.csv
Source File Path: samprod-fileingestion/<<study_name>>/
Output:
Expected file format: <<blinded>>_<<dataset>>_yyyymmdd.csv
Expected file location: rtft/<<study_name>>/<<vendor>>/
Core Extraction Logic
Follow these steps to extract the necessary components from the input:
Date and Month Conversion: If the date in the file name is in the format YYYYMMMDD (e.g., 2023APR18), convert the three-letter month abbreviation to its two-digit numeric format (e.g., APR -> 04). The final date format should always be YYYYMMDD (e.g., 20230418). The date is typically located before the file extension.
Identify Study Name: Find the study code, which is a string that appears in both the file name and the source folder path (e.g., P23-380). This is typically found after SAM_ and before the environment (e.g., TEST, PROD).
Identify Blinding Status: Look for the keywords BLINDED or UNBLINDED.
Identify Dataset Name: This is the key piece of information that describes the file's content.
For SAM files, the dataset is typically found after the env segment and before the blinding status. It can be a single word (e.g., TV) or a code (e.g., RNKIT).
If you cannot confidently identify the dataset, use unknowndataset as the placeholder.
Identify Vendor: Look for a vendor name (vendorname), which is often a company or lab name like UC lab or EPC, typically found before the date. If a vendor name is not present in the file name, use unknownvendor as the placeholder.
Extract Extension: Extract the file extension (e.g., .csv, .txt, .pdf).

If place are interchanged try identifying it, this is example of each component:
study_name:B15-845,ABT-199,M24-064
env: TEST or PROD
Dataset: DA,RNKIT,MAGEINV
blinded/unblinded:blinded,unblinded
Vendor: UC lab,LBC,EPC,ABBV
Transformation Rules
Use the extracted components to build the target path and file name.
Rule A (Default Case): For files starting with SAM_, the Target File Name should be [blinding status]_[dataset]_[converted date].[extension]. The Target File Path should be rtft/[study name]/[vendor name]/.
Rule B (Special Case/General Fallback): For all other files, the Target File Name is the original Source File Name. The Target File Path should be rtft/[study name]/[vendor name]/ if both can be identified. If only the study name is identified, the path should be rtft/[study name]/unknownvendor/. If neither can be identified, the path should be rtft/unknownstudy/unknownvendor/.
Example Walkthroughs
Example 1 (Default Case):
Input:
Source File Name: SAM_P23-380_TEST_TV_BLINDED_UC lab_20231030.csv
Source File Path: samprod-fileingestion/P23-380/
Output: {"Target File Name": "BLINDED_TV_20231030.csv", "Target File Path": "rtft/P23-380/UC lab/"}
Example 2 (Date Conversion Case):
Input:
Source File Name: SAM_Mock Study 34_TEST_RNKIT_UNBLINDED_EPC_2023APR18.txt
Source File Path: samprod-fileingestion/Mock Study 34/
Output: {"Target File Name": "UNBLINDED_RNKIT_20230418.txt", "Target File Path": "rtft/Mock Study 34/EPC/"}
Example 3 (Fallback Case):
Input:
Source File Name: my_report.pdf
Source File Path: samprod-fileingestion/Mock Study 33/
Output: {"Target File Name": "my_report.pdf", "Target File Path": "rtft/Mock Study 33/unknownvendor/"}
Example 4 (Unknown Case):
Input:
Source File Name: another_report.docx
Source File Path: miscellaneous/
Output: {"Target File Name": "another_report.docx", "Target File Path": "rtft/unknownstudy/unknownvendor/"}

Final Output Format
Provide only a single JSON object with the following structure. Do not include any additional text, explanations, or code outside of the JSON.
JSON

{
  "Target File Name": "[constructed_file_name]",
  "Target File Path": "[constructed_file_path]"
}


"""

# Initialize clients outside the handler to reuse them across invocations
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')
bedrock_client = boto3.client('bedrock-runtime', region_name='ap-south-1')

def lambda_handler(event, context):
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
            source_key = s3_event['s3']['object']['key']
            
            source_file_name = os.path.basename(source_key)
            # This logic extracts the path part of the S3 key
            source_file_path = os.path.dirname(source_key) + '/'

            # Construct the final prompt for the LLM
            full_prompt = f"{prompt_template}\n\nInput:\nSource File Name: {source_file_name}\nSource File Path: {source_file_path}\nOutput:"

            body = json.dumps({
                "inputText": full_prompt,
                "textGenerationConfig": {
                    "maxTokenCount": 512,
                    "stopSequences": ["```json", "```"],
                    "temperature": 0,
                    "topP": 0.9,
                }
            })

            response = bedrock_client.invoke_model(
                body=body,
                modelId="amazon.titan-text-express-v1",
                accept="application/json",
                contentType="application/json"
            )

            response_body = json.loads(response.get('body').read())
            llm_output_text = response_body.get('results')[0].get('outputText')
            
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
            
    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete!')
    }