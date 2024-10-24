#! /bin/bash

#执行所有datax的json
DATAX_HOME=/opt/module/datax

#传入日期
if [ -n "$2" ]
then
	do_date=$2
else
	do_date=$(date -d "1 days ago"+%Y-%m-%d)
fi

# 如果传入的路径不存在，先创建
create_dir(){
	hadoop fs -test -e $1
	if [ $? -eq 1 ] 
	then 
		echo "路径$1不存在，正在创建..."
		hadoop fs -mkdir -p $1
	fi
}

# 执行脚本
datax(){
	for table in $*
	do
	echo "导入表$table..."
	config=$DATAX_HOME/job/import/fast_food.${table}.json
	targetdir=/origin_data/fast_food/db/${table}_full/$do_date
	
	echo "校验目录${targetdir}..."
	create_dir ${targetdir}
	echo "执行datax，导入数据至hdfs..."
	python $DATAX_HOME/bin/datax.py $config -p"-Dtargetdir=${targetdir}" >$DATAX_HOME/log/fast_food_import.log 2>&1
	
	if [[ $? -eq 1 ]]
        then 
            echo "数据导入出错，日志如下: "
            cat $DATAX_HOME/log/fast_food_import.log
        else
            echo "$table导入成功!"
        fi
    done
}

case $1 in
shop | region | promotion | product_spu_attr | product_spu_attr_value | product_spu | product_sku | product_group | product_group_sku | product_category)
    datax $1
    ;;
all)
    datax shop region promotion product_spu_attr product_spu_attr_value product_spu product_sku product_group product_group_sku product_category
    ;;
esac

