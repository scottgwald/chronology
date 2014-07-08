#! /bin/sh

### BEGIN INIT INFO
# Provides:	kronosd
# Description: Kronos time series storage engine
### END INIT INFO

PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
LOGDIR=/var/log/kronos
LOGFILE=$LOGDIR/uwsgi.log
RUNDIR=/var/run/kronos
PIDFILE=$RUNDIR/kronosd.pid
LIBDIR=/usr/lib/kronos

. /lib/lsb/init-functions

set -e

case "$1" in
  start)
	mkdir -p $LOGDIR $RUNDIR
  chown -R kronos:kronos $LOGDIR $RUNDIR

  if [ -e $PIDFILE ]
  then
    echo "kronosd is already running"
    exit 1
  fi

	echo -n "Starting kronosd: "
	if (cd $LIBDIR/uwsgi && ./uwsgi --ini /etc/kronos/uwsgi.ini --pidfile $PIDFILE --daemonize $LOGFILE)
	then
		echo "ok"
	else
		echo "failed"
	fi
	;;

  stop)
  if ! [ -e $PIDFILE ]
  then
    echo "kronosd is not running running"
    exit 1
  fi
	echo -n "Stopping kronosd: "
	if (cd $LIBDIR/uwsgi && ./uwsgi --stop $PIDFILE)
	then
		echo "ok"
	else
		echo "failed"
	fi
	rm -f $PIDFILE
	sleep 1
	;;

  restart|force-reload)
	${0} stop
	${0} start
	;;

  *)
	echo "Usage: /etc/init.d/$NAME {start|stop|restart|force-reload}" >&2
	exit 1
	;;
esac

exit 0
