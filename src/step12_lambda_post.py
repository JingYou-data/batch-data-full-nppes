"""
Step 12: Lambda function that returns data from POST body back to caller
"""
import json

def lambda_handler(event, context):
    """
    Lambda function: POST request echoes back the body
    """
    
    # Parse the body from the event
    try:
        if 'body' in event:
            # If body is a string, parse it as JSON
            if isinstance(event['body'], str):
                body = json.loads(event['body'])
            else:
                body = event['body']
        else:
            body = {}
        
        # Echo back the received data
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps({
                'message': 'Data received and echoed back',
                'received_data': body,
                'data_type': str(type(body).__name__),
                'request_id': context.request_id if context else 'local-test'
            })
        }
    
    except json.JSONDecodeError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'error': 'Invalid JSON in request body',
                'details': str(e)
            })
        }

# Local testing
if __name__ == "__main__":
    print("=" * 70)
    print("Step 12: POST Lambda - Echo Request Body")
    print("=" * 70)
    
    # Mock context
    class MockContext:
        request_id = 'test-67890'
    
    # Test cases
    test_cases = [
        {
            'name': 'Simple JSON',
            'body': json.dumps({'name': 'Jing', 'role': 'Data Engineer'})
        },
        {
            'name': 'Nested JSON',
            'body': json.dumps({
                'user': {'name': 'Jing', 'location': 'Nashville'},
                'data': {'specialists': 1000, 'states': 50}
            })
        },
        {
            'name': 'Array',
            'body': json.dumps({'items': [1, 2, 3, 4, 5]})
        }
    ]
    
    print("\n### Testing Lambda Function ###")
    for i, test in enumerate(test_cases, 1):
        print(f"\nTest {i}: {test['name']}")
        event = {'body': test['body']}
        result = lambda_handler(event, MockContext())
        
        body = json.loads(result['body'])
        print(f"  Status: {result['statusCode']}")
        print(f"  Received: {json.dumps(body['received_data'], indent=2)}")
    
    print("\n" + "=" * 70)
    print("Step 12 Complete!")
    print("=" * 70)
    print("✓ Lambda function ready for deployment")
    print("✓ Echoes POST body back to caller")