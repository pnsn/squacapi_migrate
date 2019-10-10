import os
import psycopg2
import psycopg2.extras
from squacapipy.squacapi import Metric

# for psycopg2
DB_NAME = os.environ.get('DB_NAME')
DB_USER = os.environ.get('DB_USER')
DB_PASSWD = os.environ.get('DB_PASSWD')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')

# for squacapipy
API_TOKEN = os.getenv('SQUAC_API_TOKEN')
API_BASE_URL = os.getenv('SQUAC_API_BASE')

'''
Migrate all metrics from stations_metrics to squac_production
To run:
source .env && python migrate_metrics.py
* assumes all env vars are in file called .env
                                       Table "public.metrics"
   Column    |            Type             |            Modifiers
-------------+-----------------------------+---------------------------------
 id          | integer                     | not null default nextval
 metric      | character varying           | not null
 unit        | character varying           | not null
 description | character varying           | not null
 created_at  | timestamp without time zone | not null
 updated_at  | timestamp without time zone | not null

 Dest:
 Class Metric , table measurement_metric
                                        Table "public.measurement_metric"
   Column    |           Type           | Collation | Nullable |   Default
-------------+--------------------------+-----------+----------+-------------
 id          | integer                  |           | not null | nextval
 created_at  | timestamp with time zone |           | not null |
 updated_at  | timestamp with time zone |           | not null |
 name        | character varying(255)   |           | not null |
 description | character varying(255)   |           | not null |
 unit        | character varying(255)   |           | not null |
 url         | character varying(255)   |           | not null |
 user_id     | integer                  |           | not null |

    attr:

'''


def make_metric_payload(row):
    '''
        index column
        0 id
        1 metric
        2 unit
        3 description
        4 created_at
        5 updated_at
    '''
    payload = {
        'name': row[1],
        'unit': row[2],
        'description': row[3],
    }
    return payload


try:
    connection = psycopg2.connect(dbname=DB_NAME,
                                  user=DB_USER,
                                  password=DB_PASSWD,
                                  host=DB_HOST)

    # use DictCursor to access by column name
    cursor = connection.cursor()
    sql_query = """ SELECT * FROM metrics"""
    cursor.execute(sql_query)
    metrics = cursor.fetchall()
    payloads = []
    for m in metrics:
        payloads.append(make_metric_payload(m))
    # print(payloads)
    for p in payloads:
        m = Metric().post(p)
except psycopg2.Error as error:
    print("Error connecting to station_mentrics DB. Error: {}".format(error))
finally:
    if(cursor):
        cursor.close()
        connection.close()
