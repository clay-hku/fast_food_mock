#! /bin/bash

# 判断是否传入参数
if [ $# -lt 1 ]
then
	echo "必须传入参数start/stop"
	exit
fi

case $1 in
"start")
	# 判断maxwell是否启动
	pid=$(ps -ef | grep Maxwell | grep -v grep)
	# 如果pid不为空（TRUE）执行&&，如果FALSE执行||
	[ "$pid" ] && echo "Maxwell 已启动..." || /opt/module/maxwell/bin/maxwell --config /opt/module/maxwell/config.properties --daemon
;;

"stop")
	pid=$(ps -ef | grep Maxwell | grep -v grep)
	# grep -v反向过滤；awk自动按照空格切分{ print $2 } 代表返回第二个切分的值；xargs代表前面的结果传入后面命令参数
	[ "pid" ] && $(ps -ef | grep Maxwell | grep -v grep | awk '{ print $2 }' | xargs kill -9) || echo "Maxwell没有启动"
;;
esac

