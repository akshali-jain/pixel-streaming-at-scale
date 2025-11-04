import boto3
import json
import os
import traceback

def lambda_handler(event, context):
    print("===== 🚀 START: requestSession Lambda =====")

    try:
        # Step 1: Log the raw event (trimmed for safety)
        print(f"📩 Incoming event: {json.dumps(event)[:1000]}")

        # Step 2: Setup SQS client
        sqs = boto3.resource("sqs")
        sqs_name = os.environ.get("SQSName", "")
        client_secret = os.environ.get("clientSecret", "")

        print(f"🔹 SQS Queue Name: {sqs_name}")
        print(f"🔹 Client Secret present: {'Yes' if client_secret else 'No'}")

        # Step 3: Validate environment variables
        if not sqs_name or not client_secret:
            print("❌ ERROR: Missing environment variables SQSName or clientSecret!")
            return {
                "statusCode": 500,
                "body": json.dumps("Server configuration error: missing environment variables.")
            }

        # Step 4: Connect to SQS
        print("🔗 Connecting to SQS queue...")
        queue = sqs.get_queue_by_name(QueueName=sqs_name)
        print("✅ Connected to SQS successfully!")

        # Step 5: Extract event details
        messageReqId = event["requestContext"]["requestId"]
        messageConnId = event["requestContext"]["connectionId"]
        messageReqBody = event["body"]
        uniqueId = str(event["requestContext"]["requestTimeEpoch"])

        print(f"🆔 Request ID: {messageReqId}")
        print(f"🔌 Connection ID: {messageConnId}")
        print(f"⏱️ Unique ID (epoch): {uniqueId}")

        # Step 6: Parse and validate request body
        try:
            parsed_body = json.loads(messageReqBody)
            secretParam = parsed_body.get("bearer", "")
        except Exception as e:
            print(f"❌ ERROR: Failed to parse request body: {e}")
            return {
                "statusCode": 400,
                "body": json.dumps("Invalid request body format")
            }

        print(f"🔑 Bearer received: {secretParam}")

        # Step 7: Validate bearer
        if secretParam == client_secret:
            print("✅ Bearer validated successfully!")
            
            payload = {
                "requestId": messageReqId,
                "connectionId": messageConnId,
                "body": parsed_body
            }
            payload_str = json.dumps(payload)

            # Step 8: Send message to SQS
            print(f"📤 Sending message to SQS queue: {sqs_name}")
            queue.send_message(
                MessageBody=payload_str,
                MessageGroupId=messageReqId,
                MessageDeduplicationId=uniqueId
            )
            print("✅ Message successfully sent to SQS!")
            print(f"📦 Payload sent: {payload_str}")

            return {
                "statusCode": 200,
                "body": json.dumps("Message posted to queue successfully!")
            }

        else:
            print("⚠️ Invalid client! Bearer token did not match.")
            return {
                "statusCode": 403,
                "body": json.dumps("Invalid client authentication")
            }

    except Exception as e:
        print("❌ ERROR: Exception occurred while processing requestSession")
        print(traceback.format_exc())
        return {
            "statusCode": 500,
            "body": json.dumps(f"Internal Server Error: {str(e)}")
        }

    finally:
        print("===== 🏁 END: requestSession Lambda =====")
