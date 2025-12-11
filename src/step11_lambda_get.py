"""
Step 11: Lambda function that returns a random number (GET request)
"""
import json
import random

def lambda_handler(event, context):
    """
    Lambda function: GET request returns random number
    """
    
    # Generate random number
    random_number = random.randint(1, 1000)
    
    # Return response
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'message': 'Random number generated',
            'random_number': random_number,
            'request_id': context.request_id if context else 'local-test'
        })
    }

# Local testing
if __name__ == "__main__":
    print("=" * 70)
    print("Step 11: GET Lambda - Random Number Generator")
    print("=" * 70)
    
    # Mock context
    class MockContext:
        request_id = 'test-12345'
    
    # Test the lambda
    print("\n### Testing Lambda Function ###")
    for i in range(5):
        result = lambda_handler({}, MockContext())
        body = json.loads(result['body'])
        print(f"Test {i+1}: Random number = {body['random_number']}")
    
    print("\n" + "=" * 70)
    print("Step 11 Complete!")
    print("=" * 70)
    print("✓ Lambda function ready for deployment")
    print("✓ Returns random number on GET request")