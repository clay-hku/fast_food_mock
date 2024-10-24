#！ /bin/bash
# mysql_to_kafka_inc_init.sh all/表名

# 判断参数是否传入
if [ $# -lt 1 ]
then
	echo "必须输入表名/all"
	exit
fi

import_data(){
	for table in $@
	do
		echo "bootstrap ${table} ..."
		/opt/module/maxwell/bin/maxwell-bootstrap --config /opt/module/maxwell/config.properties --database fast_food --table ${table}
		echo "${table} done."
	done
}

# 根据表名加载数据到kafka
case $1 in
"all")
	import_data customer order_info order_detail order_status_log payment
;;
"customer" | "order_info" | "order_detail" | "order_status_log" | "payment")
    import_data $1
;;
esac
