#!/usr/bin/env bash

#don't run this if there is an existing process
if [  $(ps -ef | grep squacapi_migrate.py  | grep -v grep | wc -l | tr -s "\n") -eq 0 ]; then 
    cd /home/deploy/squacapi_migrate
    source .env
    /var/.virtualenvs/squacapi_migrate/bin/python migrate_measurements.py --networks=CC,UW,UO --metrics=RMSduration_0p07cm,snr20_0p34cmHP,pctavailable,ngaps >> /var/log/pnsn/squacapi_migrate.log
fi