import json

import boto3

import os

import random

import base64

import time

from boto3.dynamodb.conditions import Attr
 
def lambda_handler(event, context):

    """

    Creates new HealthCoach UE+Signaling instances on demand

    Triggered by: poller Lambda when no instances available

    """

    try:

        # Get configuration from SSM

        ssm = boto3.client('ssm')

        concurrency_limit = int(ssm.get_parameter(Name='HealthCoach-ConcurrencyLimit')['Parameter']['Value'])

        matchmaker_ip = ssm.get_parameter(Name='HealthCoach-MatchmakerIP')['Parameter']['Value']

        # Check capacity in DynamoDB

        dynamodb = boto3.resource('dynamodb')

        table = dynamodb.Table(os.environ['DynamoDBName'])

        response = table.scan(FilterExpression=Attr('InstanceID').eq(''))

        if len(response['Items']) == 0:

            return {

                'statusCode': 400,

                'body': json.dumps('Instance pool at capacity! Could not create new instance')

            }

        # UserData script for HealthCoach

        user_data_script = f'''<powershell>

# Set execution policy

Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Force
 
# Create log file

$logFile = "C:\\healthcoach-startup.log"

Start-Transcript -Path $logFile
 
Write-Host "Starting HealthCoach auto-scale instance..."
 
# Get instance metadata

$instanceId = Invoke-RestMethod -Uri "http://169.254.169.254/latest/meta-data/instance-id"

$region = Invoke-RestMethod -Uri "http://169.254.169.254/latest/meta-data/placement/region"
 
# Download HealthCoach deployment package from S3

Write-Host "Downloading HealthCoach deployment package..."

aws s3 sync s3://{os.environ.get('S3BucketName', 'healthcoach-deployment')}/HealthCoach-Deployment/ C:\\ --delete --region $region
 
# Start HealthCoach services

if (Test-Path "C:\\start-healthcoach.bat") {{

    Write-Host "Starting HealthCoach services..."

    (Get-Content "C:\\start-healthcoach.bat") -replace '%MATCHMAKER_IP%', '{matchmaker_ip}' | Set-Content "C:\\start-healthcoach.bat"

    Start-Process -FilePath "C:\\start-healthcoach.bat" -NoNewWindow -Wait

}} else {{

    Write-Host "ERROR: start-healthcoach.bat not found!"

}}
 
# Tag instance as ready

aws ec2 create-tags --resources $instanceId --tags Key=Status,Value=Ready --region $region
 
Write-Host "HealthCoach instance deployment completed!"

Stop-Transcript
</powershell>'''

        # Encode UserData

        user_data_encoded = base64.b64encode(user_data_script.encode('utf-8')).decode('utf-8')

        # Create new instance

        ec2 = boto3.client('ec2')

        launch_template_value = os.environ.get('LaunchTemplateName', 'HealthCoach-Production-UESignaling-LT')

        if launch_template_value.startswith('lt-'):

            launch_template_key = 'LaunchTemplateId'

        else:

            launch_template_key = 'LaunchTemplateName'

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

        response = ec2.run_instances(**launch_params)

        instance_id = response['Instances'][0]['InstanceId']

        print(f"Created new HealthCoach instance: {instance_id}")

        # ✅ NEW: Wait for Public/Private IPs (retry loop up to 20 seconds)

        def get_instance_details(instance_id, max_wait=20):

            waited = 0

            while waited < max_wait:

                desc = ec2.describe_instances(InstanceIds=[instance_id])

                instance = desc['Reservations'][0]['Instances'][0]

                public_ip = instance.get('PublicIpAddress')

                private_ip = instance.get('PrivateIpAddress')

                state = instance['State']['Name']

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

        print(f"Error creating instance: {str(e)}")

        return {

            'statusCode': 500,

            'body': json.dumps(f'Error creating instance: {str(e)}')

        }

 
