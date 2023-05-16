#!/usr/bin/env python
"""mapper.py"""
import re
import sys


def perform_map():
    for line in sys.stdin:
        line = line.strip()
        data = line.split(',')
        pickup_time = data[1]
        time = pickup_time.split()[0]
        month = pickup_time[:7]
        tip = data[13]
        payment_type = data[9]
        if is_valid_year_month(month) and month.split('-')[0] == '2020':
            print('%s\t%s\t%s\t%d' % (month, payment_type, tip, 1))
        else:
            continue


def is_valid_year_month(data):
    pattern = r'^\d{4}-\d{2}$'  # regex pattern for year-month format
    return bool(re.match(pattern, data))


if __name__ == '__main__':
    perform_map()
