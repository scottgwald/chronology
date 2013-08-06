#!/bin/bash

OPENID_STATE=$(cat settings.cfg|grep OPENID_STATE|cut -d\  -f3)
rm -rf "$OPENID_STATE"
mkdir --mode=777 "$OPENID_STATE"

# If you change the socket, make sure you update the nginx configuration so that
# it knows where to forward requests.
uwsgi --socket /tmp/jia.sock \
      --chmod-socket \
      --module 'jia:app' \
      --processes 8 \
      --master \
      --protocol=uwsgi \
      --buffer-size=64000 \
      --enable-threads
