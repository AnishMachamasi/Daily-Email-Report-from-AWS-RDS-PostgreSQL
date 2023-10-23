import json
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3

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
    cur = conn.cursor(cursor_factory=RealDictCursor)

    sql = """
    SELECT * FROM table_name
    """
    cur.execute(sql)

    table_data = cur.fetchall()

    conn.commit()

    cur.close()

    # Send the table data to email
    ses = boto3.client('ses')

    sender = 'email_identity'
    receiver = 'email_identity'
    subject = {
        'Data': 'Table data from PostgreSQL table'
    }

    # Create the HTML table
    html_table = """
<table>
  <tr>
    <th>ID</th>
    <th>Name</th>
    <th>Hex</th>
    <th>RGB</th>
  </tr>
  """

    for row in table_data:
        html_table += """<tr>"""
        for column in row:
            html_table += """<td>{}</td>""".format(row[column])
        html_table += """</tr>"""

    html_table += """</table>"""

    # Construct the email message
    message = {
        'Subject': subject,
        'Body': {
            'Html': {
                'Data': html_table
            }
        }
    }

    # Send the email
    ses.send_email(
        Source=sender,
        Destination={
            'ToAddresses': [receiver]
        },
        Message=message
    )

    return {
        'statusCode': 200,
        'body': json.dumps('Table data sent to email successfully!')
    }