import boto3
from boto3.dynamodb.conditions import Key, Attr
from chalice import Chalice

TABLE_NAME = 'Violations'
REGION = 'us-east-1'

dynamodb = boto3.resource('dynamodb', region_name=REGION)
table = dynamodb.Table(TABLE_NAME)
app = Chalice(app_name='chalice-app')

@app.route('/')
def index():
    # get most recent 20 violations (by violation ID, descending)
    resp = table.query(
        IndexName='idx_violation_id',
        KeyConditionExpression=Key('type').eq('violation'),
        Limit=20,
        ScanIndexForward=False,
    )
    return resp['Items']

# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users/', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.json_body
#     # Suppose we had some 'db' object that we used to
#     # read/write from our database.
#     # user_id = db.create_user(user_as_json)
#     return {'user_id': user_id}
#
# See the README documentation for more examples.
#
