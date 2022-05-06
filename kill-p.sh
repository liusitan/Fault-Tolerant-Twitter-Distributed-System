#! /bin/sh
lsof -n -i4TCP:$1 | grep LISTEN
pid=`lsof -n -i4TCP:$1 | grep LISTEN | awk '{print $2}'`
echo $pid
## when it is ubuntu, just replace '4TCP' to '6TCP' in line3.
if [ $pid ]
  then
		kill -9 $pid
		echo "[`date "+%Y-%m-%d %H:%M:%S"`] Process kill successfull!"
else
	echo "[`date "+%Y-%m-%d %H:%M:%S"`] No process exists on port $1."