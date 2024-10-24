#! /bin/bash

#执行所有datax的json
DATAX_HOME=/opt/module/datax
DATAX_LOG=/opt/module/datax/fast_food_export.log

handle_dir(){
	export_dir=$1
	for target_file in $(hadoop fs -ls -R ${export_dir} | awk '{print $8}')
	do 
		hadoop fs -test -z ${target_file}
		if [ $? -eq 0 ]
		then 
			echo "${target_file}文件大小为0，删除该文件"
			hadoop fs -rm -f ${target_file}
		fi
	done
}

# 传入表名
datax(){
	for json in $*
	do 
		export_config="${DATAX_HOME}/job/export/fast_food_report.${json}.json"
		export_dir="/warehouse/fast_food/ads/${json}"
		echo "加载datax_json文件....路径为${export_config}"
		hadoop fs -test -e ${export_dir}
		if [ $? -eq 0 ];then
			handle_dir ${export_dir}
			count=$(hadoop fs -count ${export_dir} | awk '{print $2}')
			if [[ count == 0 ]];then 
				echo "${export_dir}路径下没有文件"
			else
				echo "导入${json}表到mysql...
文件路径：${export_dir}
datax_json路径：${export_config}"
				python ${DATAX_HOME}/bin/datax.py ${export_config} -p"-Dexportdir=${export_dir}" > ${DATAX_LOG} 2>&1
				if [[ $? -eq 1 ]];then 
				echo "处理出错..."
				tail -n +20 ${DATAX_LOG}
				fi
			fi 
			else 
				echo "${export_dir}路径不存在"
		fi
	done 
}

case $1 in 
"ads_comment_group_stats" | "ads_comment_shop_stats" | "ads_comment_sku_stats" | "ads_group_hour_stats" | "ads_promotion_promotion_stats" | "ads_promotion_reduce_amount_stats" | "ads_promotion_split_amount_stats" | "ads_ranking_shop_order_amount_top10_stats" | "ads_ranking_sku_reduce_top10_stats" | "ads_sku_hour_stats" | "ads_trade_group_stats" | "ads_trade_hour_stats" | "ads_trade_shop_stats" | "ads_trade_type_shop_stats")
datax $1
;;
"all")
datax "ads_comment_group_stats" "ads_comment_shop_stats" "ads_comment_sku_stats" "ads_group_hour_stats" "ads_promotion_promotion_stats" "ads_promotion_reduce_amount_stats" "ads_promotion_split_amount_stats" "ads_ranking_shop_order_amount_top10_stats" "ads_ranking_sku_reduce_top10_stats" "ads_sku_hour_stats" "ads_trade_group_stats" "ads_trade_hour_stats" "ads_trade_shop_stats" "ads_trade_type_shop_stats"
;;
esac
