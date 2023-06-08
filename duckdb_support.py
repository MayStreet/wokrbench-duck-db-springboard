import os
import http.client
import json
 
aws_magic_host = '169.254.170.2'
 
def get_data_from_url(url):
    conn = http.client.HTTPConnection(aws_magic_host, 80)
    payload = ''
    headers = {}
    conn.request("GET", url, payload, headers)
    res = conn.getresponse()
    data = res.read()
    return data.decode("utf-8")
 
 
def set_duckdb_aws_credentials(duckdb_connection):
    aws_url = os.getenv('AWS_CONTAINER_CREDENTIALS_RELATIVE_URI')
    if not aws_url:
        raise Exception('could not find AWS URL')
 
    returned_data = get_data_from_url(aws_url)
    formatted_data = json.loads(returned_data)
 
    created_sql = [
        "INSTALL 'httpfs';",
        "LOAD 'httpfs';",
        "SET s3_region='us-east-1';",
        f"SET s3_access_key_id='{formatted_data['AccessKeyId']}';",
        f"SET s3_secret_access_key='{formatted_data['SecretAccessKey']}';"
        f"SET s3_session_token='{formatted_data['Token']}';"
    ]
    duckdb_connection.execute('\n'.join(created_sql))