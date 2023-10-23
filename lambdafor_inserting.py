import json
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
import csv

host = "db_instance_endpoint"
username = "username_of_db_instance"
password = "password_of_db_instance"
database = "database_name"

conn = psycopg2.connect(
    host =host,
    database = database,
    user = username,
    password = password
)

def lambda_handler(event, context):
    # TODO implement
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Create the table if it doesn't exist
    sql = """
    CREATE TABLE IF NOT EXISTS table_name (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        hex VARCHAR(7) NOT NULL,
        rgb VARCHAR(15) NOT NULL
    );
    """
    cur.execute(sql)

    # Read the CSV file from S3
    s3 = boto3.client('s3')

    bucket = 'bucket_name'
    key = 'csv_file_key'

    obj = s3.get_object(Bucket=bucket, Key=key)
    csv_data = obj['Body'].read().decode('utf-8')

    # Insert the data from the CSV file into the table
    reader = csv.reader(csv_data.splitlines())

    for row in reader:
        sql = """
        INSERT INTO table_name (name, hex, rgb)
        VALUES (%s, %s, %s)
        """
        cur.execute(sql, (row[0], row[1], row[2]))

    conn.commit()

    cur.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted successfully!')
    }