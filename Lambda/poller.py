import boto3
import json
import os
import urllib.request
import urllib.error
 
def lambda_handler(event, context):
    print("=== Poller Lambda started ===")
    print(f"Incoming event: {json.dumps(event)}")
 
    try:
        lambdaFunc = boto3.client('lambda')
        print("Initialized boto3 Lambda client")
 
        # Retrieve Lambda ARNs dynamically
        print("Fetching ARN for sendSessionDetails")
        response = lambdaFunc.get_function(FunctionName='HealthCoach-sendSessionDetails')
        lambdaArnSendSesionDetails = response['Configuration']['FunctionArn']
        print(f"sendSessionDetails ARN: {lambdaArnSendSesionDetails}")
 
        print("Fetching ARN for createInstances")
        response = lambdaFunc.get_function(FunctionName='HealthCoach-createInstances')
        lambdaArnCreateInstances = response['Configuration']['FunctionArn']
        print(f"createInstances ARN: {lambdaArnCreateInstances}")
 
        print("Fetching ARN for keepConnectionAlive")
        response = lambdaFunc.get_function(FunctionName='HealthCoach-keepConnectionAlive')
        lambdaArnKeepAlive = response['Configuration']['FunctionArn']
        print(f"keepConnectionAlive ARN: {lambdaArnKeepAlive}")
 
        # Get matchmaker secret from SSM
        print("Fetching matchmaker client secret from SSM")
        ssm = boto3.client('ssm')
        parameter = ssm.get_parameter(Name='HealthCoach-ClientSecret')
        matchmakersecret = parameter['Parameter']['Value']
        print("Successfully retrieved matchmaker secret")
 
        # Connect to SQS
        sqs_name = os.environ.get("SQSName")
        print(f"Connecting to SQS with name: {sqs_name}")
 
        sqs = boto3.resource('sqs')
        print("Initialized SQS resource")
 
        # Extra debug — list visible queues
        client = boto3.client('sqs')
        queues = client.list_queues()
        print(f"Queues visible to Lambda: {queues.get('QueueUrls', [])}")
 
        queue = sqs.get_queue_by_name(QueueName=sqs_name)
        print(f"Connected to SQS queue successfully: {queue.url}")
 
        messages = queue.receive_messages()
        print(f"Received {len(messages)} messages from queue")
 
        for message in messages:
            print(f"Processing message: {message.body}")
 
            payload = json.loads(message.body)
            print(f"Parsed payload: {json.dumps(payload)}")
 
            # Send keep-alive message
            print("Invoking keepConnectionAlive Lambda")
            lambdaFunc.invoke(
                FunctionName=lambdaArnKeepAlive,
                InvocationType='RequestResponse',
                Payload=json.dumps(payload)
            )
            print("keepConnectionAlive Lambda invoked successfully")
 
            print(f"Connection id: {payload.get('connectionId')}")
            print(f"MatchMaker URL: {os.environ.get('MatchMakerURL')}")
            print("Contacting MatchMaker to check server availability")
 
            try:
                request = urllib.request.Request(
                    url=os.environ["MatchMakerURL"],
                    headers={"clientsecret": matchmakersecret},
                    method='GET'
                )
                response = urllib.request.urlopen(request, timeout=5)
                print(f"MatchMaker response status: {response.status}")
 
                if response.status == 200:
                    responsePayload = response.read()
                    JSON_object = json.loads(responsePayload.decode("utf-8"))
                    payload.update(JSON_object)
                    print(f"MatchMaker response: {json.dumps(JSON_object)}")
 
                    print("Invoking sendSessionDetails Lambda")
                    response = lambdaFunc.invoke(
                        FunctionName=lambdaArnSendSesionDetails,
                        InvocationType='RequestResponse',
                        Payload=json.dumps(payload)
                    )
                    print("sendSessionDetails Lambda invoked successfully")
 
                    message.delete()
                    print("Deleted message from SQS after processing")
 
                    print(f"Found server to service request: {responsePayload.decode('utf-8')}")
                else:
                    print(f"Unexpected MatchMaker status code: {response.status}")
 
            except urllib.error.HTTPError as err:
                print(f"HTTPError from MatchMaker: {err.code} - {err.reason}")
                if err.code == 400:
                    print("No server available; invoking createInstances Lambda")
                    inputParams = {"Key": "value"}
                    lambdaFunc.invoke(
                        FunctionName=lambdaArnCreateInstances,
                        InvocationType='Event',
                        Payload=json.dumps(inputParams)
                    )
                    print("createInstances Lambda invoked successfully")
                else:
                    raise err
 
        print("=== Poller Lambda completed successfully ===")
 
        return {
            'statusCode': 200,
            'body': json.dumps('Completed scanning for incoming requests')
        }
 
    except Exception as e:
        print(f"Poller error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Poller error: {str(e)}")
        }
