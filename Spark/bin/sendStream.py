#!/usr/bin/env python

"""Generates a stream to Kafka from a time series csv file.
"""

#!/usr/bin/env python

"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
import time

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    conf = {'bootstrap.servers': "kafka:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    rdr = csv.reader(open(args.filename))
    #next(rdr)  # Skip header
    firstline = True

    while True:
        try:
            if firstline is True:
                line1 = next(rdr, None)
                v0, v1, v2, v3 = line1[0], datetime.strptime(line1[1], '%Y-%m-%d %H:%M'), float(line1[2]), float(line1[3])
                v4, v5, v6, v7 = float(line1[4]), float(line1[5]), str(line1[6]), float(line1[7])
                v8, v9, v10 = float(line1[8]), float(line1[9]), float(line1[10])
                v11, v12 = float(line1[11]), float(line1[12])
                # Convert csv columns to key value pair
                result = {}
                result["Location"] = [v0]
                result["Last Updated"] = v1.strftime('%Y-%m-%d %H:%M')
                result["Temperature (C)"] = v2
                result["Temperature (F)"] = v3
                result["Wind (km/hr)"] = v4
                result["Wind direction (in degree)"] = v5
                result["Wind direction (compass)"] = v6
                result["Pressure (millibars)"] = v7
                result["Precipitation (mm)"] = v8
                result["Humidity"] = v9
                result["Cloud Cover"] = v10
                result["UV Index"] = v11
                result["Wind Gust (km/hr)"] = v12
                # Convert dict to json as message format
                jresult = json.dumps(result)
                firstline = False
                producer.produce(topic, key=p_key, value=jresult, callback=acked)

            else:
                line = next(rdr, None)
                result = {}
                v0, v1, v2, v3 = line1[0], datetime.strptime(line[1], '%Y-%m-%d %H:%M'), float(line[2]), float(line[3])
                v4, v5, v6, v7 = float(line[4]), float(line[5]), str(line[6]), float(line[7])
                v8, v9, v10 = float(line[8]), float(line[9]), float(line[10])
                v11, v12 = float(line[11]), float(line[12])
                # Convert csv columns to key value pair
                result = {}
                result["Location"] = [v0]
                result["Last Updated"] = v1.strftime('%Y-%m-%d %H:%M')
                result["Temperature (C)"] = v2
                result["Temperature (F)"] = v3
                result["Wind (km/hr)"] = v4
                result["Wind direction (in degree)"] = v5
                result["Wind direction (compass)"] = v6
                result["Pressure (millibars)"] = v7
                result["Precipitation (mm)"] = v8
                result["Humidity"] = v9
                result["Cloud Cover"] = v10
                result["UV Index"] = v11
                result["Wind Gust (km/hr)"] = v12
                # Convert dict to json as message format
                jresult = json.dumps(result)
                firstline = False
                producer.produce(topic, key=p_key, value=jresult, callback=acked)
            producer.flush()
            time.sleep(61)
        except TypeError:
            sys.exit()

if __name__ == "__main__":
    main()

