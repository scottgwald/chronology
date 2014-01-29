# Importing pykronos and some useful utilities

from pykronos.client import KronosClient
from datetime import datetime
from datetime import timedelta
from dateutil.tz import tzutc

# Creating a client

kc = KronosClient('http://localhost:8151', namespace='demo')
start = datetime.now(tz=tzutc())

# Inserting data

kc.put({'yourproduct.website.pageviews': [
         {'source': 'http://test.com',
          'browser': {'name': 'Firefox', 'version': 26},
          'pages': ['page1.html', 'page2.html']}],
        'yourproduct.website.clicks': [
         {'user': 35, 'num_clicks': 10},
         {'user': 42, 'num_clicks': 2}]})


## Inserting in batches

# Retrieving data

events = kc.get('yourproduct.website.clicks',
                start,
                start + timedelta(minutes=10))
for event in events:
  print event

# Getting a list of streams

# Deleting data
