#! /bin/bash
# fast_food_f1.sh start/stop

if [ $# -lt 1 ]
then
	echo "必须传入参数start/stop"
	exit
fi

case $1 in
"start")
	nohup /opt/module/flume/bin/flume-ng agent -n a1 -c /opt/module/flume/conf -f /opt/module/flume/job/kafka_to_hdfs.conf >/opt/module/flume/fast_food.log 2>&1 &
;;

"stop")
	ps -ef| grep flume | grep -v grep | awk '{print $2}' |xargs kill -9
;;
esac
