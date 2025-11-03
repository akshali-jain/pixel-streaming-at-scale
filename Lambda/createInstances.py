import json
import boto3
import os
import random
import base64
import time
from boto3.dynamodb.conditions import Attr

def lambda_handler(event, context):
    print("=== createInstances Lambda started ===")
    print(f"Incoming event: {json.dumps(event)}")

    try:
        # Step 1: Get configuration
        print("Initializing SSM client")
        ssm = boto3.client('ssm')

        print("Fetching concurrency limit from SSM")
        concurrency_limit = ssm.get_parameter(Name='HealthCoach-ConcurrencyLimit')['Parameter']['Value']
        print(f"Concurrency limit: {concurrency_limit}")

        print("Fetching Matchmaker IP from SSM")
        matchmaker_ip = ssm.get_parameter(Name='HealthCoach-MatchmakerIP')['Parameter']['Value']
        print(f"Matchmaker IP: {matchmaker_ip}")

        # Step 2: DynamoDB
        print("Initializing DynamoDB resource")
        dynamodb = boto3.resource('dynamodb')
        table_name = os.environ.get('DynamoDBName')
        print(f"Using DynamoDB table: {table_name}")

        table = dynamodb.Table(table_name)
        print("Scanning DynamoDB for empty InstanceIDs")
        response = table.scan(FilterExpression=Attr('InstanceID').eq(''))
        print(f"DynamoDB scan response: {json.dumps(response)}")

        if len(response['Items']) == 0:
            print("No available slots found in table — at capacity.")
            return {
                'statusCode': 400,
                'body': json.dumps('Instance pool at capacity! Could not create new instance')
            }

        # Step 3: Build EC2 user data
        print("Building EC2 user_data script")
        user_data_script = f"""<powershell>
Write-Host "Starting HealthCoach auto-scale instance..."
# Matchmaker IP: {matchmaker_ip}
</powershell>"""
        user_data_encoded = base64.b64encode(user_data_script.encode('utf-8')).decode('utf-8')
        print("User data encoded successfully")

        # Step 4: Launch EC2
        print("Initializing EC2 client")
        ec2 = boto3.client('ec2')

        launch_template_value = os.environ.get('LaunchTemplateName', 'HealthCoach-Production-UESignaling-LT')
        print(f"Launch template from env: {launch_template_value}")
        launch_template_key = 'LaunchTemplateId' if launch_template_value.startswith('lt-') else 'LaunchTemplateName'
        print(f"Launch template key: {launch_template_key}")

        launch_params = {
            'LaunchTemplate': {
                launch_template_key: launch_template_value,
                'Version': '$Latest'
            },
            'MinCount': 1,
            'MaxCount': 1,
            'UserData': user_data_encoded,
            'TagSpecifications': [{
                'ResourceType': 'instance',
                'Tags': [
                    {'Key': 'Name', 'Value': 'HealthCoach-UESignaling-Auto'},
                    {'Key': 'Type', 'Value': 'signalling'},
                    {'Key': 'Application', 'Value': 'HealthCoach'},
                    {'Key': 'CreatedBy', 'Value': 'Lambda-AutoScale'}
                ]
            }]
        }
        print(f"Launch parameters: {json.dumps(launch_params)}")

        response = ec2.run_instances(**launch_params)
        print(f"EC2 run_instances response: {json.dumps(response)}")

        instance_id = response['Instances'][0]['InstanceId']
        print(f"Created new HealthCoach instance: {instance_id}")

        # Step 5: Wait for IP
        def get_instance_details(instance_id, max_wait=20):
            waited = 0
            while waited < max_wait:
                print(f"Checking instance details... waited={waited}s")
                desc = ec2.describe_instances(InstanceIds=[instance_id])
                instance = desc['Reservations'][0]['Instances'][0]
                public_ip = instance.get('PublicIpAddress')
                private_ip = instance.get('PrivateIpAddress')
                state = instance['State']['Name']
                print(f"State: {state}, PublicIP: {public_ip}, PrivateIP: {private_ip}")
                if public_ip:
                    return public_ip, private_ip, state
                time.sleep(2)
                waited += 2
            return instance.get('PublicIpAddress', 'Pending'), instance.get('PrivateIpAddress', 'N/A'), instance['State']['Name']

        public_ip, private_ip, state = get_instance_details(instance_id)
        print(f"Instance ready: ID={instance_id}, PrivateIP={private_ip}, PublicIP={public_ip}, State={state}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'New HealthCoach instance created successfully',
                'InstanceId': instance_id,
                'PrivateIp': private_ip,
                'PublicIp': public_ip,
                'State': state
            })
        }

    except Exception as e:
        print(f"❌ Error creating instance: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error creating instance: {str(e)}')
        }
