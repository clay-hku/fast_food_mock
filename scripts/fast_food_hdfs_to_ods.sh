#! /bin/bash

# 判断是否传入参数
if [ $# -lt 1 ]
then
	echo '请传入表名/all'
	exit
fi

# 判断传入日期
[ $2 ] && dt=$2 || dt=$(date -d '-1 day' +%F)

# load_data
load_data(){
	tableNames=$@
	sql='use fast_food;'
	for table in ${tableNames}
	do
		path=/origin_data/fast_food/db/${table:4}/${dt}
		hadoop fs -test -e ${path}
		if [ $? -eq 0 ]
		then 
			sql="${sql};load data inpath '${path}' overwrite into table ${table} partition(dt='${dt}');"
		else
			echo "${table}没有成功上传，因为${path}不存在"
		fi	
	done
	/opt/module/hive/bin/hive -e "${sql}"
}

case $1 in 
'all')
	load_data "ods_customer_inc" "ods_order_detail_inc" "ods_order_info_inc" "ods_order_status_log_inc" "ods_payment_inc" "ods_product_category_full" "ods_product_group_full" "ods_product_group_sku_full" "ods_product_sku_full" "ods_product_spu_attr_full" "ods_product_spu_attr_value_full" "ods_product_spu_full" "ods_promotion_full" "ods_region_full" "ods_shop_full" 
;;
'ods_order_detail_inc')
    load_data "ods_order_detail_inc"
;;
'ods_order_info_inc')
    load_data "ods_order_info_inc"
;;
'ods_order_status_log_inc')
    load_data "ods_order_status_log_inc"
;;
'ods_payment_inc')
    load_data "ods_payment_inc"
;;
'ods_product_category_full')
    load_data "ods_product_category_full"
;;
'ods_product_group_full')
    load_data "ods_product_group_full"
;;
'ods_product_group_sku_full')
    load_data "ods_product_group_sku_full"
;;
'ods_product_sku_full')
    load_data "ods_product_sku_full"
;;
'ods_product_spu_attr_full')
    load_data "ods_product_spu_attr_full"
;;
'ods_product_spu_attr_value_full')
    load_data "ods_product_spu_attr_value_full"
;;
'ods_product_spu_full')
    load_data "ods_product_spu_full"
;;
'ods_promotion_full')
    load_data "ods_promotion_full"
;;
'ods_region_full')
    load_data "ods_region_full"
;;
'ods_shop_full')
    load_data "ods_shop_full"
;;
esac
