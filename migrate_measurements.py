import os
import psycopg2
import psycopg2.extras
from squacapipy.squacapi import Metric, Channel, Network, Measurement
from datetime import datetime
import pytz
import argparse

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
Scipt to read data from prototype station_metrics db into squac using
squacapipy

To run:
 See usage

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
        'channel': channel_id,
        'metric': metric_id,
        'value': value,
        'starttime': starttime,
        'endtime': endtime

    }
    return payload


parser = argparse.ArgumentParser(
    description="""Migrate station_metrics db measurements into squac db""",
    usage="""source .env && python migrate_measurements.py
            --networks=CC,UW,UO
            --metrics=snr20_0p34cmHP,snr20_0p34cmHP,pctavailable,ngaps
            --starttime=YYYY-mm-dd
            --endtime=YYYY-mm-dd""")

parser.add_argument('--networks', required=True,
                    help="Comma seperated list of networks")
parser.add_argument('--metrics', required=True,
                    help="Comma seperated list of metrics")
parser.add_argument('--starttime', required=True,
                    help="metrics created at and after, inclusive")
parser.add_argument('--endtime', required=True,
                    help="metrics created beforem exclusive")                    
args = parser.parse_args()


networks_tup = tuple(n.upper() for n in args.networks.split(","))
metrics_tup = tuple(m for m in args.metrics.split(","))
print(networks_tup)
print(metrics_tup)

year, month, day = args.starttime.split("-")
starttime = datetime(int(year), int(month), int(day), tzinfo=pytz.UTC)
year, month, day = args.endtime.split("-")
endtime = datetime(int(year), int(month), int(day), tzinfo=pytz.UTC)
nets_squac = Network().get(network=args.networks.lower())
if nets_squac.status_code != 200:
    print('Error {}'.format(nets_squac.body))

metrics = Metric().get(
    name=args.metrics)
# create hash of unique keys for quick lookup
lookup = {}
for m in metrics.body:
    key = m['name']
    lookup[key] = m['id']
cursor = None
try:
    connection = psycopg2.connect(dbname=DB_NAME,
                                  user=DB_USER,
                                  password=DB_PASSWD,
                                  host=DB_HOST)

    # use DictCursor to access by column name
    cursor = connection.cursor()
    sql_query = '''SELECT
                        m.*,
                        s.net as net,
                        s.sta as sta,
                        s.loc as loc,
                        s.chan as chan,
                        metrics.metric as metric
                    FROM measurements m
                    JOIN sncls s ON s.id = m.sncl_id
                    JOIN metrics  ON metrics.id = m.metric_id
                    WHERE s.net IN %s
                    AND metrics.metric IN %s
                    AND m.starttime >= %s
                    AND m.starttime < %s;
                '''

    cursor.execute(sql_query, (networks_tup, metrics_tup, starttime, endtime))
    # print(cursor.query)
    measurements = cursor.fetchall()
    payloads = []
    for m in measurements:
        payload = make_measurement_payload(m, lookup)
        if payload:
            payloads.append(payload)
    if len(payload) > 0:
        # slice payloads into 100's
        slicey = 100
        end = slicey
        start = 0
        while start < len(payloads):
            collection = payloads[start:end]
            m = Measurement().post(collection)
            if m.status_code != 201:
                print("Error {} on bulk post".format(m.status_code))
                print(m.body[0])
                print("Trying single posts...")
                for p in collection:
                    m = Measurement().post(p)
                    if m.status_code != 201:
                        print("Error {} on single post".format(m.status_code))
                        print(m.body)
            start += slicey
            end += slicey


except psycopg2.Error as error:
    print("Error connecting to station_mentrics DB. Error: {}".format(error))
finally:
    if(cursor):
        cursor.close()
        connection.close()
