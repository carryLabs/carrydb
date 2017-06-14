#!/bin/sh
#
# chkconfig: 2345 64 36
# description: carrydb startup scripts
#
carrydb_root=/usr/local/carrydb
carrydb_bin=$ssdb_root/ssdb-server
# each config file for one instance
# configs="/data/carrydb_data/test/carrydb.conf /data/carrydb_data/test2/carrydb.conf"
configs="/data/carrydb_data/test/carrydb.conf"

 
if [ -f /etc/rc.d/init.d/functions ]; then
	. /etc/rc.d/init.d/functions
fi
 
start() {
	for conf in $configs; do
		$carrydb_bin $conf -s restart -d
	done
}
 
stop() {
	for conf in $configs; do
		$carrydb_bin $conf -s stop -d
	done
}
 
# See how we were called.
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        start
        ;;
    *)
        echo $"Usage: $0 {start|stop|restart}"
        ;;
esac
exit $RETVAL
