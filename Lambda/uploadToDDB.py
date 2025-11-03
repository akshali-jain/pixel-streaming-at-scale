import json
import boto3
import os
import traceback

def lambda_handler(event, context):
    print("=== HealthCoach PopulateDynamoDB Lambda STARTED ===")

    try:
        # === Step 1: Initialize AWS clients ===
        client = boto3.client('elbv2')
        dynamodb = boto3.resource('dynamodb')

        # === Step 2: Load environment variables ===
        alb_name = os.environ.get("ALBName")
        table_name = os.environ.get("DynamoDBName")
        clear_table = os.environ.get("CLEAR_TABLE_ON_START", "False").lower() == "true"

        if not alb_name or not table_name:
            raise ValueError("Missing required environment variables: ALBName or DynamoDBName")

        table = dynamodb.Table(table_name)
        print(f"Using ALB: {alb_name}")
        print(f"Using DynamoDB table: {table_name}")
        print(f"Auto-clear table on start: {clear_table}")

        # === Step 3: Optionally clear DynamoDB table ===
        if clear_table:
            print("Clearing all items from table before repopulating...")
            scan = table.scan(ProjectionExpression='TargetGroup')
            with table.batch_writer() as batch:
                for item in scan.get('Items', []):
                    batch.delete_item(Key={'TargetGroup': item['TargetGroup']})
            print("✅ Table cleared successfully.")

        # === Step 4: Fetch Load Balancer ARN ===
        print("Fetching LoadBalancer ARN...")
        response = client.describe_load_balancers(Names=[alb_name])
        loadbalancerarn = response['LoadBalancers'][0]['LoadBalancerArn']
        print(f"Found LoadBalancer ARN: {loadbalancerarn}")

        # === Step 5: Fetch Listener ARN ===
        print("Fetching Listener ARN...")
        response = client.describe_listeners(LoadBalancerArn=loadbalancerarn)
        listenerarn = response['Listeners'][0]['ListenerArn']
        print(f"Found Listener ARN: {listenerarn}")

        # === Step 6: Fetch Listener Rules ===
        print("Fetching listener rules...")
        response = client.describe_rules(ListenerArn=listenerarn)
        rules = response.get('Rules', [])
        print(f"Found {len(rules)} listener rules")

        added_count = 0
        skipped_count = 0

        # === Step 7: Iterate and populate DynamoDB ===
        for rule in rules:
            if rule['Priority'] == 'default':
                print("Skipping default rule...")
                continue

            try:
                qs_key = rule['Conditions'][0]['QueryStringConfig']['Values'][0]['Key']
                qs_value = rule['Conditions'][0]['QueryStringConfig']['Values'][0]['Value']
                qs = f"{qs_key}={qs_value}"
                target_group = f"TG{qs_value}"
                arn = rule['Actions'][0]['TargetGroupArn']

                print(f"Processing rule: TargetGroup={target_group}, ARN={arn}, QueryString={qs}")

                # Check for existing item (avoid duplicates)
                existing = table.get_item(Key={'TargetGroup': target_group})
                if 'Item' in existing:
                    print(f"⚠️ Skipping duplicate TargetGroup: {target_group}")
                    skipped_count += 1
                    continue

                # Insert new record
                table.put_item(
                    Item={
                        'TargetGroup': target_group,
                        'ARN': arn,
                        'InstanceID': '',
                        'QueryString': qs
                    }
                )
                print(f"✅ Added TargetGroup {target_group} to DynamoDB.")
                added_count += 1

            except Exception as inner_e:
                print(f"❌ Error processing rule: {str(inner_e)}")
                traceback.print_exc()

        print(f"=== Population Complete ===")
        print(f"Records added: {added_count}")
        print(f"Records skipped (duplicates): {skipped_count}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'DynamoDB population complete',
                'added': added_count,
                'skipped': skipped_count
            })
        }

    except Exception as e:
        print("❌ ERROR in Lambda execution:")
        print(str(e))
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
