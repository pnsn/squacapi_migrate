import os
import psycopg2
import psycopg2.extras
from squacapipy.squacapi import Metric, Channel, Network, Measurement
from datetime import datetime, timedelta
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

    in cron
        /bin/bash -l -c 'cd /home/deploy/squacapi_migrate &&
        source .env && time /var/.virtualenvs/squacapi_migrate/bin/python
        migrate_measurements.py --networks=CC,UW,UO
        --metrics=RMSduration_0p07cm,snr20_0p34cmHP,pctavailable,ngaps >>
        /var/log/pnsn_web/cron.log 2>&1'

Desired metrics
metric_name             metric_id    threshold (per hour)
snr20_0p34cmHP          153             1
RMSduration_0p07cm      110             60
pctavailable            77              98%
ngaps                   78              1

Should get about 43k/day

    station_metrics=# select count(*) from measurements m
    join sncls s on s.id = m.sncl_id
    join metrics met on met.id = m.metric_id
    where s.net in ('UW','UO','CC') and m.starttime >= '2019-10-10'
    and m.starttime < '2019-10-11'
    and met.metric in
    ('ngaps', 'RMSduration_0p07cm','snr20_0p34cmHP','pctavailable');

    count
    -------
    43228
    (1 row)
    427/day if using CC and ngaps only


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


def main():
    parser = argparse.ArgumentParser(
        description="""Migrate station_metrics db measurements to squac db""",
        usage="""source .env && python migrate_measurements.py
                --networks=CC,UW,UO
                --metrics=snr20_0p34cmHP,snr20_0p34cmHP,pctavailable,ngaps
                --starttime=YYYY-mm-dd
                --endtime=YYYY-mm-dd""")

    parser.add_argument('--networks', required=True,
                        help="Comma seperated list of networks")
    parser.add_argument('--metrics', required=True,
                        help="Comma seperated list of metrics")
    parser.add_argument('--starttime',
                        help="metrics created at and after, inclusive")
    parser.add_argument('--endtime',
                        help="metrics created beforem exclusive")
    args = parser.parse_args()

    networks_tup = tuple(n.upper() for n in args.networks.split(","))
    metrics_tup = tuple(m for m in args.metrics.split(","))

    ''' if starttime and endtime are missing find most recent by id and start
        there
    '''
    if args.starttime is None and args.endtime is None:
        f = open("recent_id.txt", 'r+')
        recent_id = f.read()
        # is there anything in file? If not then query one day back
        if len(recent_id) > 0:
            # recent_id = recent_id
            starttime = None
            endtime = None
        else:
            # default to read last  day
            endtime = datetime.now()
            delta = timedelta(days=1)
            starttime = endtime - delta

    else:
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
        sql_by_date = '''SELECT
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
                        AND m.starttime < %s
                        ORDER BY m.id;
                    '''

        sql_by_id = '''SELECT
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
                        AND m.id > %s
                        ORDER BY m.id;
                    '''
        if starttime is None and endtime is None:
            cursor.execute(sql_by_id, (networks_tup, metrics_tup,
                           recent_id))
            print(sql_by_id, (networks_tup, metrics_tup, int(recent_id)))
        else:
            cursor.execute(sql_by_date, (networks_tup, metrics_tup, starttime,
                           endtime))
            print(sql_by_date, (networks_tup, metrics_tup, starttime, endtime))
        measurements = cursor.fetchall()
        payloads = []
        # these are ordered asc, track the morst recent created_at
        if len(measurements) == 0:
            print("nothing new fuck this...")
        recent_id = measurements[-1][0]
        f = open("recent_id.txt", 'w')
        f.write(str(recent_id))
        exit(1)

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
                            next()

                start += slicey
                end += slicey

    except psycopg2.Error as error:
        print("Error connecting to station_mentrics DB. Error: {}".format(error))
    finally:
        if(cursor):
            cursor.close()
            connection.close()


if __name__ == "__main__":
    main()
