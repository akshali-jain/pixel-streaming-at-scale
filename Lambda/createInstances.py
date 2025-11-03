import json
import boto3
import os
import random
import base64
import time
from boto3.dynamodb.conditions import Attr

def lambda_handler(event, context):
    print("=== HealthCoach CreateInstances Lambda STARTED ===")
    print(f"Incoming event: {json.dumps(event)}")

    try:
        # === Step 1: Get configuration ===
        ssm = boto3.client('ssm')
        print("Fetching concurrency limit and matchmaker IP from SSM...")

        concurrency_limit = int(ssm.get_parameter(Name='HealthCoach-ConcurrencyLimit')['Parameter']['Value'])
        matchmaker_ip = ssm.get_parameter(Name='HealthCoach-MatchmakerIP')['Parameter']['Value']

        print(f"Concurrency Limit: {concurrency_limit}")
        print(f"Matchmaker IP: {matchmaker_ip}")

        # === Step 2: Setup EC2 and DynamoDB clients ===
        ec2 = boto3.client('ec2')
        dynamodb = boto3.resource('dynamodb')
        table_name = os.environ.get('DynamoDBName', 'HealthCoach-Production-SessionMapping')
        table = dynamodb.Table(table_name)
        print(f"Using DynamoDB table: {table_name}")

        # === Step 3: Random subnet logic ===
        all_subnets = [os.environ.get('SubnetIdPublicA'), os.environ.get('SubnetIdPublicB')]
        all_subnets = [s for s in all_subnets if s]
        print(f"Available subnets: {all_subnets}")

        # === Step 4: Build User Data Script ===
        user_data_script = f"""<powershell>
Write-Host "Starting HealthCoach signalling instance..."
Write-Host "Matchmaker IP: {matchmaker_ip}"
aws s3 sync s3://{os.environ.get('S3BucketName', 'healthcoach-deployment')}/HealthCoach-Deployment/ C:\\ --delete
if (Test-Path "C:\\start-healthcoach.bat") {{
    Write-Host "Starting HealthCoach service..."
    (Get-Content "C:\\start-healthcoach.bat") -replace '%MATCHMAKER_IP%', '{matchmaker_ip}' | Set-Content "C:\\start-healthcoach.bat"
    Start-Process -FilePath "C:\\start-healthcoach.bat" -NoNewWindow -Wait
}} else {{
    Write-Host "ERROR: start-healthcoach.bat not found!"
}}
Write-Host "Setup complete. Tagging instance as Ready."
Stop-Transcript
</powershell>"""

        user_data_encoded = base64.b64encode(user_data_script.encode('utf-8')).decode('utf-8')
        print("UserData encoded successfully")

        # === Step 5: Launch Template setup ===
        launch_template_value = os.environ.get('LaunchTemplateName', 'HealthCoach-Production-UESignaling-LT')
        launch_template_key = 'LaunchTemplateId' if launch_template_value.startswith('lt-') else 'LaunchTemplateName'
        print(f"LaunchTemplate key={launch_template_key}, value={launch_template_value}")

        # === Step 6: Scheduled Mode (startAllServers) ===
        if "startAllServers" in event and event["startAllServers"]:
            print("Scheduled mode: creating multiple instances...")
            response = ec2.run_instances(
                LaunchTemplate={launch_template_key: launch_template_value, 'Version': '$Latest'},
                MinCount=int(concurrency_limit),
                MaxCount=int(concurrency_limit),
                UserData=user_data_encoded,
                SubnetId=random.choice(all_subnets)
            )
            print(f"Created {concurrency_limit} instances.")
            return {'statusCode': 200, 'body': json.dumps('All instances created successfully')}

        # === Step 7: On-demand Mode ===
        print("On-demand mode: checking DynamoDB for available slots...")
        response = table.scan(FilterExpression=Attr('InstanceID').eq(''))
        print(f"DynamoDB scan found {len(response['Items'])} available slots.")

        if len(response['Items']) == 0:
            print("⚠️ No available slots found — creating new instance anyway.")
        else:
            print("✅ Slot available — proceeding to instance creation.")

        # === Step 8: Launch new EC2 instance ===
        subnet_to_use = random.choice(all_subnets) if all_subnets else None
        print(f"Launching new instance in subnet: {subnet_to_use}")

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

        if subnet_to_use:
            launch_params['SubnetId'] = subnet_to_use

        print(f"Launching instance with params: {json.dumps(launch_params)}")
        ec2_response = ec2.run_instances(**launch_params)
        instance_id = ec2_response['Instances'][0]['InstanceId']
        print(f"✅ Created new EC2 instance: {instance_id}")

        # === Step 9: Fetch instance IPs ===
        def get_instance_details(instance_id, max_wait=30):
            waited = 0
            while waited < max_wait:
                print(f"Waiting for instance details... {waited}s elapsed")
                desc = ec2.describe_instances(InstanceIds=[instance_id])
                instance = desc['Reservations'][0]['Instances'][0]
                public_ip = instance.get('PublicIpAddress')
                private_ip = instance.get('PrivateIpAddress')
                state = instance['State']['Name']
                if public_ip:
                    return public_ip, private_ip, state
                time.sleep(3)
                waited += 3
            return 'Pending', 'N/A', 'Unknown'

        public_ip, private_ip, state = get_instance_details(instance_id)
        print(f"Instance ready — ID={instance_id}, PrivateIP={private_ip}, PublicIP={public_ip}, State={state}")

        # === Step 10: Update DynamoDB (optional) ===
        if len(response['Items']) > 0:
            item_key = response['Items'][0]['id'] if 'id' in response['Items'][0] else response['Items'][0]['TargetGroup']
            print(f"Updating DynamoDB slot with InstanceID={instance_id}")
            table.update_item(
                Key={'TargetGroup': item_key},
                UpdateExpression="SET InstanceID = :iid",
                ExpressionAttributeValues={':iid': instance_id}
            )

        # === Step 11: Return result ===
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
        print(f"❌ ERROR: {str(e)}")
        return {'statusCode': 500, 'body': json.dumps(f'Error creating instance: {str(e)}')}
