#! /bin/bash
# kafka 群起脚本

if [ $# -lt 1 ]
then
	echo "必须传入start/stop"
	exit
fi

case $1 in 
"start")
	for host in hadoop102 hadoop103 hadoop104
	do
		ssh $host "/opt/module/kafka/bin/kafka-server-start.sh -daemon /opt/module/kafka/config/server.properties"
	done
;;
"stop")
	for host in hadoop102 hadoop103 hadoop104
	do
		ssh $host "/opt/module/kafka/bin/kafka-server-stop.sh -daemon /opt/module/kafka/config/server.properties"
	done
;;
esac
