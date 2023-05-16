#!/usr/bin/env python
"""reducer.py"""
import datetime
import sys


def perform_reduce():
    ride_totals = {}
    ride_counts = {}

    for line in sys.stdin:
        line = line.strip()
        key_value_pairs = line.split('\t')
        key = key_value_pairs[:2]
        values = key_value_pairs[2:]
        month, payment_type = key[0], key[1]
        tip, ride_count = float(values[0]), int(values[1])
        # try:
        #     tip = float(tip)
        # except ValueError:
        #     tip = 0.0

        if month not in ride_totals:
            ride_totals[month] = {}
            ride_counts[month] = {}

        if payment_type not in ride_totals[month]:
            ride_totals[month][payment_type] = 0.0
            ride_counts[month][payment_type] = 0

        ride_totals[month][payment_type] += tip * ride_count
        ride_counts[month][payment_type] += ride_count

    sorted_rides = sorted(ride_totals.keys(), key=lambda x: datetime.datetime.strptime(x, '%Y-%m'))

    for month in sorted_rides:
        for payment_type in ride_totals[month]:
            avg_tip = ride_totals[month][payment_type] / ride_counts[month][payment_type]
            print(month, payment_type, avg_tip)


if __name__ == '__main__':
    perform_reduce()
hadoop jar "$HADOOP_MAPRED_HOME"/hadoop-streaming.jar \
-Dmapred.job.name='Taxi streaming job' \
-Dmapred.reduce.tasks=10 \
-Dmapreduce.map.memory.mb=1024 \
-file /tmp/mapreduce/mapper.py -mapper /tmp/mapreduce/mapper.py \
-file /tmp/mapreduce/reducer.py -reducer /tmp/mapreduce/reducer.py \
-input /user/root/2020/2020  -output taxi-output
