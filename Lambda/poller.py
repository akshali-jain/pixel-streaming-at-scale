import boto3
import json
import os
import urllib.request
import urllib.error

def lambda_handler(event, context):
    print("=== 🚀 Poller Lambda started ===")
    print(f"📥 Incoming event: {json.dumps(event)}")

    try:
        # --- Initialize Lambda client ---
        lambdaFunc = boto3.client('lambda')
        print("✅ Initialized boto3 Lambda client")

        # --- Retrieve ARNs dynamically ---
        print("🔍 Fetching ARNs for other Lambdas...")
        sendSession = lambdaFunc.get_function(FunctionName='HealthCoach-sendSessionDetails')
        lambdaArnSendSesionDetails = sendSession['Configuration']['FunctionArn']
        print(f"📦 sendSessionDetails ARN: {lambdaArnSendSesionDetails}")

        createInstances = lambdaFunc.get_function(FunctionName='HealthCoach-createInstances')
        lambdaArnCreateInstances = createInstances['Configuration']['FunctionArn']
        print(f"⚙️ createInstances ARN: {lambdaArnCreateInstances}")

        keepAlive = lambdaFunc.get_function(FunctionName='HealthCoach-keepConnectionAlive')
        lambdaArnKeepAlive = keepAlive['Configuration']['FunctionArn']
        print(f"🔌 keepConnectionAlive ARN: {lambdaArnKeepAlive}")

        # --- Get matchmaker secret from SSM ---
        print("🔐 Fetching matchmaker client secret from SSM Parameter Store...")
        ssm = boto3.client('ssm')
        parameter = ssm.get_parameter(Name='HealthCoach-ClientSecret')
        matchmakersecret = parameter['Parameter']['Value']
        print("✅ Successfully retrieved matchmaker secret")

        # --- Connect to SQS ---
        sqs_name = os.environ.get("SQSName")
        print(f"📦 Connecting to SQS queue: {sqs_name}")

        sqs = boto3.resource('sqs')
        client = boto3.client('sqs')
        print("✅ Initialized SQS resource")

        queues = client.list_queues()
        print(f"🧾 Queues visible to Lambda: {queues.get('QueueUrls', [])}")

        queue = sqs.get_queue_by_name(QueueName=sqs_name)
        print(f"✅ Connected to SQS queue successfully: {queue.url}")

        messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=2)
        print(f"📨 Received {len(messages)} messages from queue")

        if len(messages) == 0:
            print("⚠️ No new messages found in SQS.")
        else:
            for message in messages:
                print(f"🧩 Processing message body: {message.body}")
                payload = json.loads(message.body)
                print(f"📤 Parsed payload: {json.dumps(payload)}")

                # --- Invoke keepConnectionAlive Lambda ---
                print("🔁 Invoking keepConnectionAlive Lambda...")
                lambdaFunc.invoke(
                    FunctionName=lambdaArnKeepAlive,
                    InvocationType='Event',
                    Payload=json.dumps(payload)
                )
                print("✅ keepConnectionAlive Lambda invoked successfully")

                connection_id = payload.get('connectionId')
                print(f"🔗 Connection ID: {connection_id}")

                # --- Contact MatchMaker ---
                matchmaker_url = os.environ.get("MatchMakerURL")
                print(f"🌐 Contacting MatchMaker: {matchmaker_url}")
                print("🧾 Sending request with clientsecret from SSM")

                try:
                    request = urllib.request.Request(
                        url=matchmaker_url,
                        headers={"clientsecret": matchmakersecret},
                        method='GET'
                    )

                    response = urllib.request.urlopen(request, timeout=10)
                    print(f"✅ MatchMaker response status: {response.status}")

                    if response.status == 200:
                        response_payload = response.read()
                        json_data = json.loads(response_payload.decode("utf-8"))
                        print(f"📦 MatchMaker JSON response: {json.dumps(json_data)}")

                        # Merge with payload and invoke sendSessionDetails
                        payload.update(json_data)

                        print("🚀 Invoking sendSessionDetails Lambda...")
                        lambdaFunc.invoke(
                            FunctionName=lambdaArnSendSesionDetails,
                            InvocationType='Event',
                            Payload=json.dumps(payload)
                        )
                        print("✅ sendSessionDetails Lambda invoked successfully")

                        # Delete processed message
                        message.delete()
                        print("🗑️ Deleted message from SQS after successful processing")

                    else:
                        print(f"⚠️ Unexpected MatchMaker status code: {response.status}")

                except urllib.error.HTTPError as err:
                    print(f"❌ HTTPError from MatchMaker: {err.code} - {err.reason}")
                    if err.code == 400:
                        print("⚙️ No signalling servers available — invoking createInstances Lambda")
                        inputParams = {"startAllServers": False}
                        lambdaFunc.invoke(
                            FunctionName=lambdaArnCreateInstances,
                            InvocationType='Event',
                            Payload=json.dumps(inputParams)
                        )
                        print("✅ createInstances Lambda invoked successfully to spawn new servers")
                    else:
                        raise err

        print("=== ✅ Poller Lambda completed successfully ===")

        return {
            'statusCode': 200,
            'body': json.dumps('Poller completed scanning and processing messages')
        }

    except Exception as e:
        print(f"❌ Poller error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Poller error: {str(e)}")
        }
