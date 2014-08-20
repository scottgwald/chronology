#!/usr/bin/python

import argparse
import csv
import urllib2

from cStringIO import StringIO
from dateutil.parser import parse
from pykronos import KronosClient
from pykronos import TIMESTAMP_FIELD
from zipfile import ZipFile

"""
Loads US presidential elections contribution data for the 2012 presidential
race in the state of Oregon from the US Federal Election Commission into
Kronos.

A sample event dictionary looks like:

{u'@id': u'9809c000-17ed-11e2-8000-0b89d16a9975',
 u'@time': 13504320000000000L,
 u'cand_id': u'P80003338',
 u'cand_nm': u'Obama, Barack',
 u'cmte_id': u'C00431445',
 u'contb_receipt_amt': u'6',
 u'contb_receipt_dt': u'17-OCT-12',
 u'contbr_city': u'PORTLAND',
 u'contbr_employer': u'NOT EMPLOYED',
 u'contbr_nm': u'BROOKS, MAGGIE',
 u'contbr_occupation': u'STUDENT',
 u'contbr_st': u'OR',
 u'contbr_zip': u'972171333',
 u'election_tp': u'G2012',
 u'file_num': u'897092',
 u'form_tp': u'SA17A',
 u'memo_cd': u'',
 u'memo_text': u'',
 u'null': [u''],
 u'receipt_desc': u'',
 u'tran_id': u'C26603850'}
"""

DONATIONS_FILE_NAME = 'P00000001-OR'
DONATIONS_FILE_URL = ('ftp://ftp.fec.gov/FEC/Presidential_Map/2012/P00000001/'
                      '%s.zip' % DONATIONS_FILE_NAME)


def load_test_data(args):
  donations = ZipFile(StringIO(urllib2.urlopen(DONATIONS_FILE_URL).read()))
  donations = StringIO(donations.read('%s.csv' % DONATIONS_FILE_NAME))

  events = []
  rows = csv.DictReader(donations)
  for row in rows:
    row[TIMESTAMP_FIELD] = parse(row['contb_receipt_dt'])
    events.append(row)

  kc = KronosClient(args.kronos_url)
  kc.put({'donations': events})


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('--kronos-url',
                      default='http://localhost:8150',
                      help='The Kronos server to dump data into')
  args = parser.parse_args()
  load_test_data(args)
