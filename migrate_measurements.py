import os
import psycopg2
import psycopg2.extras
from squacapipy.squacapi import Metric, Measurement, Channel, Network

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
To run:
source .env && python migrate_measurements.py
* assumes all env vars are in file called .env

Desired metrics
metric_name             metric_id    threshold (per hour)
snr20_0p34cmHP          153             1
RMSduration_0p07cm      110             60
pctavailable            77              98%
ngaps                   78              1

Source tables in station_metric dbs
   Column   |            Type             |           Modifiers
------------+-----------------------------+---------------------------------
 id         | bigint                      | not null default nextval
 sncl_id    | integer                     |
 metric_id  | integer                     |
 value      | double precision            | not null
 starttime  | timestamp without time zone | not null
 endtime    | timestamp without time zone | not null
 datasrc_id | integer                     | not null
 created_at | timestamp without time zone | not null

                                       Table "public.metrics"
   Column    |            Type             |            Modifiers
-------------+-----------------------------+---------------------------------
 id          | integer                     | not null default nextval
 metric      | character varying           | not null
 unit        | character varying           | not null
 description | character varying           | not null
 created_at  | timestamp without time zone | not null
 updated_at  | timestamp without time zone | not null

                                      Table "public.sncls"
    Column    |            Type             |           Modifiers
--------------+-------------------------------------------------------------
 id           | integer                     | not null default nextval
 created_at   | timestamp without time zone | not null
 updated_at   | timestamp without time zone | not null
 net          | character varying           | not null
 sta          | character varying           | not null
 loc          | character varying           | not null
 chan         | character varying           | not null
 sncl         | character varying           | not null
 lat          | double precision            | 
 lon          | double precision            | 
 elev         | double precision            | 
 depth        | double precision            | 
 samp_rate    | double precision            | 
 inshakealert | integer                     | 


 Dest:
 Class Measurement:
    attr:

'''


def make_measurement_payload(row, lookup):
    '''Convert of row data  into payload dict for POST
        index
        0       id
        1       sncl_id
        2       metric_id
        3       value
        4       starttime
        5       endtime
        6       datasrc_id
        7       created_at
        8       net
        9       sta
        10      loc
        11      chan
        12      metric
    '''
    # write keys for hash
    net, sta, loc, chan = row[8].lower(), row[9].lower(),\
        row[10].lower(), row[11].lower()
    channel_key = net + "_" + sta + "_" + loc + "_" + chan
    if channel_key not in lookup:
        chan = Channel().get(network=net, station=sta,
                           loc=loc, channel=chan)
        try:
            lookup[channel_key] = chan.body[0]['id']
        except IndexError:
            print("Channel {} not found".format(channel_key))
            return None
            

    channel_id = lookup[channel_key]
    metric_id = lookup[row[12]]
    value, starttime, endtime = row[3], row[4], row[5]

    payload = {
        'channel_id': channel_id,
        'metric_id': metric_id,
        'value': value,
        'starttime': starttime,
        'endtime': endtime

    }
    return payload


# no spaces in in param
networks = Network().get(network='uw,uo,cc')
if networks.status_code != 200:
    print('Error {}'.format(networks.body))

metrics = Metric().get(
    name='pctavailable,ngaps,snr20_0p34cmHP,RMSduration_0p07cm')
# create hash of unique keys for quick lookup
lookup = {}
for m in metrics.body:
    key = m['name']
    lookup[key] = m['id']
print(lookup)
cursor = None
try:
    connection = psycopg2.connect(dbname=DB_NAME,
                                  user=DB_USER,
                                  password=DB_PASSWD,
                                  host=DB_HOST)

    # use DictCursor to access by column name
    cursor = connection.cursor()
    sql_query = """ SELECT
                        m.*,
                        s.net as net,
                        s.sta as sta,
                        s.loc as loc,
                        s.chan as chan,
                        metrics.metric as metric
                    FROM measurements m
                    JOIN sncls s ON s.id = m.sncl_id
                    JOIN metrics  ON metrics.id = m.metric_id
                    WHERE s.net IN ('UW', 'UO', 'CC')
                    AND metrics.metric IN(
                        'snr20_0p34cmHP', 'snr20_0p34cmHP',
                        'pctavailable', 'ngaps'
                    )
                    AND m.starttime = '2019-10-03';
                """
    cursor.execute(sql_query)
    measurements = cursor.fetchall()
    payloads = []
    for m in measurements:
        payload = make_measurement_payload(m, lookup)
        if payload:
            payloads.append(payload)
    print(payloads)
except psycopg2.Error as error:
    print("Error connecting to station_mentrics DB. Error: {}".format(error))
finally:
    if(cursor):
        cursor.close()
        connection.close()
