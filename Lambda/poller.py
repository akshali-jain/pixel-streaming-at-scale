# This function polls the SQS queue for new session requests and checks
# if a Signalling instance is available to service the request or needs to be created.
# It should be triggered on a schedule (e.g., every minute via CloudWatch Event).

import boto3
import json
import os
import logging
import urllib.request
import urllib.error

# Configure logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("=== START: Poller Lambda ===")
    logger.info(f"Incoming event: {json.dumps(event)}")

    # Initialize boto3 Lambda client
    lambdaFunc = boto3.client('lambda')
    logger.info("Initialized boto3 Lambda client")

    # Retrieve ARNs for dependent Lambdas
    logger.info("Fetching ARNs for dependent Lambdas...")

    response = lambdaFunc.get_function(FunctionName='HealthCoach-sendSessionDetails')
    lambdaArnSendSessionDetails = response['Configuration']['FunctionArn']
    logger.info(f"sendSessionDetails ARN: {lambdaArnSendSessionDetails}")

    response = lambdaFunc.get_function(FunctionName='HealthCoach-createInstances')
    lambdaArnCreateInstances = response['Configuration']['FunctionArn']
    logger.info(f"createInstances ARN: {lambdaArnCreateInstances}")

    response = lambdaFunc.get_function(FunctionName='HealthCoach-keepConnectionAlive')
    lambdaArnKeepAlive = response['Configuration']['FunctionArn']
    logger.info(f"keepConnectionAlive ARN: {lambdaArnKeepAlive}")

    # Retrieve MatchMaker client secret from SSM
    ssm = boto3.client('ssm')
    logger.info("Fetching MatchMaker client secret from SSM...")
    parameter = ssm.get_parameter(Name='HealthCoach-ClientSecret')
    matchmakersecret = parameter['Parameter']['Value']
    logger.info("Successfully retrieved MatchMaker client secret")

    # Connect to SQS
    sqs = boto3.resource('sqs')
    queue_name = os.environ["SQSName"]
    logger.info(f"Connecting to SQS queue: {queue_name}")
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    logger.info(f"Connected to SQS queue URL: {queue.url}")

    # Receive messages
    messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=2)
    logger.info(f"Received {len(messages)} messages from SQS")

    for message in messages:
        logger.info(f"Processing message body: {message.body}")

        try:
            payload = json.loads(message.body)
        except Exception as e:
            logger.error(f"Failed to parse message body: {str(e)}")
            continue

        # Step 1: Send a keep-alive signal to the frontend
        logger.info("Invoking keepConnectionAlive Lambda...")
        lambdaFunc.invoke(
            FunctionName=lambdaArnKeepAlive,
            InvocationType='Event',
            Payload=json.dumps(payload)
        )
        logger.info("keepConnectionAlive invoked successfully")

        # Step 2: Log connection details
        connection_id = payload.get("connectionId", "Unknown")
        matchmaker_url = os.environ["MatchMakerURL"]
        logger.info(f"Connection ID: {connection_id}")
        logger.info(f"MatchMaker URL: {matchmaker_url}")
        logger.info(f"Client Secret: {matchmakersecret}")

        # Step 3: Check with MatchMaker for available Signalling servers
        try:
            logger.info("Sending GET request to MatchMaker...")

            request = urllib.request.Request(
                url=matchmaker_url,
                headers={"clientsecret": matchmakersecret},
                method='GET'
            )

            logger.info(f"Request prepared: URL={request.full_url}, Headers={request.header_items()}")

            response = urllib.request.urlopen(request, timeout=10)
            logger.info(f"MatchMaker response status: {response.status}")

            if response.status == 200:
                responsePayload = response.read()
                JSON_object = json.loads(responsePayload.decode("utf-8"))
                logger.info(f"MatchMaker response JSON: {json.dumps(JSON_object)}")

                # Merge MatchMaker data into the original payload
                payload.update(JSON_object)

                # Step 4: Invoke sendSessionDetails Lambda
                logger.info("Invoking sendSessionDetails Lambda...")
                lambdaFunc.invoke(
                    FunctionName=lambdaArnSendSessionDetails,
                    InvocationType='Event',
                    Payload=json.dumps(payload)
                )
                logger.info("sendSessionDetails Lambda invoked successfully")

                # Delete the processed message
                delete_response = message.delete()
                logger.info(f"Deleted message from SQS after successful processing. Response: {delete_response}")

            else:
                logger.warning(f"Unexpected MatchMaker status code: {response.status}")
                delete_response = message.delete()
                logger.info(f"Deleted message from SQS after unexpected status. Response: {delete_response}")

        except urllib.error.HTTPError as err:
            logger.error(f"HTTPError while contacting MatchMaker: {err.code} - {err.reason}")
            if err.code == 400:
                logger.info("No signalling servers available — invoking createInstances Lambda")

                inputParams = {"startAllServers": False}
                lambdaFunc.invoke(
                    FunctionName=lambdaArnCreateInstances,
                    InvocationType='Event',
                    Payload=json.dumps(inputParams)
                )
                logger.info("createInstances Lambda invoked to start new server instance")
                # Message not deleted here, will be retried next poll

            else:
                logger.error(f"Unhandled HTTP error: {err}")
                raise err

        except Exception as e:
            logger.error(f"General exception during MatchMaker communication: {str(e)}")
            raise e

    logger.info("=== END: Poller Lambda completed successfully ===")

    return {
        'statusCode': 200,
        'body': json.dumps('Completed scanning for incoming requests')
    }
