#!/bin/bash

# If you update the bind address, make sure to update the nginx configuration so
# it knows where to pass requests.
python kronos.py --bind 'unix:/tmp/kronos.sock'
