#! /bin/bash

# 首日
# 先判断是否首日
init=0
db="fast_food"
if [ -n "$3" ]
then 
	init=1
fi

# 判断是否传入了日期
if [ ${init} -eq 1 ]
then 
# 首日装载
	if [ -n "$2" ]
	then 
		do_date=$2

	else
		echo "请输入日期"
		exit
	fi
else 
# 每日装载
	if [ -n "$2" ]
	then 
		do_date=$2

	else
		do_date=$(date -d "-1 days")
	fi
fi

sql="use ${db};"



# load_sql
if [ ${init} -eq 1 ]
then 
	# 首日sql
	dwd_trade_order_detail_inc="set hive.exec.dynamic.partition.mode=nonstrict;
with order_info as (
    select
        data.id order_id,
        data.promotion_id,
        data.original_amount,
        data.reduce_amount,
        data.actual_amount
    from ods_order_info_inc
    where dt = '${do_date}' and type = 'bootstrap-insert'
),
    order_detail as(
    select
        data.order_info_id order_id,
        data.create_time order_time,
        date_format(data.create_time,'yyyy-MM-dd') order_date,
        data.sku_num,
        data.product_sku_id sku_id,
        data.product_group_id,
        data.amount,
        data.shop_id,
        data.customer_id
    from ods_order_detail_inc
    where dt = '${do_date}' and type = 'bootstrap-insert'
),
    order_t as (
        select
            order_detail.order_id,
            order_time       ,
            order_date       ,
            shop_id          ,
            customer_id      ,
            promotion_id     ,
            sku_id           ,
            product_group_id ,
            sku_num          ,
            amount original_amount  ,
            (amount/original_amount) * reduce_amount reduce_amount  ,
            (amount/original_amount) * actual_amount actual_amount
        from order_detail
        left join order_info
        on order_info.order_id=order_detail.order_id
    )

insert overwrite table dwd_trade_order_detail_inc partition(dt)
select
    order_id         ,
    order_time       ,
    order_date       ,
    shop_id          ,
    customer_id      ,
    promotion_id     ,
    sku_id           ,
    product_group_id ,
    sku_num          ,
    original_amount  ,
    reduce_amount    ,
    actual_amount,
    order_date
from order_t;"

	dwd_trade_payment_inc="set hive.exec.dynamic.partition.mode=nonstrict;
with
    pay          as (
                        select
                            data.order_info_id                          order_id,
                            data.update_time                            pay_time,
                            date_format(data.update_time, 'yyyy-MM-dd') pay_date
                        from ods_order_status_log_inc
                        where dt = '${do_date}'
                          and data.status = '2'
                          and type = 'bootstrap-insert'
                    ),
    order_info   as (
                        select
                            data.id order_id,
                            data.original_amount,
                            data.actual_amount,
                            data.reduce_amount,
                            data.promotion_id
                        from ods_order_info_inc
                        where dt = '${do_date}'
                          and type = 'bootstrap-insert' and  data.status != 1
                    ),
    order_detail as (
                        select
                            data.product_sku_id sku_id,
                            data.product_group_id,
                            data.order_info_id  order_id,
                            data.shop_id,
                            data.customer_id,
                            data.sku_num,
                            data.amount
                        from ods_order_detail_inc
                        where dt = '${do_date}'
                          and type = 'bootstrap-insert'
                    )
-- insert overwrite table dwd_trade_payment_inc partition(dt)
select
    pay.order_id,
    pay_time,
    pay_date,
    shop_id,
    customer_id,
    promotion_id,
    order_detail.sku_id,
    product_group_id,
    sku_num,
    amount                                     original_amount,
    (amount / original_amount) * reduce_amount reduce_amount,
    (amount / original_amount) * actual_amount actual_amount,
    pay_date
from pay
         left join order_info on pay.order_id = order_info.order_id
         left join order_detail on pay.order_id = order_detail.order_id;"
		 
		 dwd_trade_refund_inc="set hive.exec.dynamic.partition.mode=nonstrict;
with
    refund       as (
                        select
                            data.order_info_id                     order_id,
                            data.create_time                            refund_time,
                            date_format(data.create_time, 'yyyy-MM-dd') refund_date
                        from ods_order_status_log_inc
                        where dt = '${do_date}'
                          and type = 'bootstrap-insert'
                            and data.status = 5
                    ),
    order_info   as (
                        select
                            data.id order_id,
                            data.original_amount,
                            data.reduce_amount,
                            data.actual_amount,
                            data.promotion_id
                        from ods_order_info_inc
                        where dt = '${do_date}'
                            and type = 'bootstrap-insert'
                    ),
    order_detail as (
                        select
                            data.amount,
                            data.sku_num,
                            data.customer_id,
                            data.order_info_id order_id,
                            data.product_group_id,
                            data.product_sku_id sku_id,
                            data.shop_id
                        from ods_order_detail_inc
                        where dt =  '${do_date}'
                    )

insert overwrite table dwd_trade_refund_inc partition (dt)
select
    order_info.order_id         ,
    refund_time      ,
    refund_date      ,
    shop_id          ,
    customer_id      ,
    promotion_id     ,
    sku_id           ,
    product_group_id ,
    sku_num          ,
    amount original_amount  ,
    (amount/original_amount) * reduce_amount    ,
    (amount/original_amount) * actual_amount,
    refund_date
from refund
left join order_info on order_info.order_id = refund.order_id
left join order_detail on order_detail.order_id = refund.order_id;"

	dwd_trade_refund_suc_inc="set hive.exec.dynamic.partition.mode=nonstrict;
with
    refund       as (
                        select
                            data.order_info_id                     order_id,
                            data.create_time                            refund_time,
                            date_format(data.create_time, 'yyyy-MM-dd') refund_date
                        from ods_order_status_log_inc
                        where dt = '${do_date}'
                          and type = 'bootstrap-insert'
                            and data.status = 6
                    ),
    order_info   as (
                        select
                            data.id order_id,
                            data.original_amount,
                            data.reduce_amount,
                            data.actual_amount,
                            data.promotion_id
                        from ods_order_info_inc
                        where dt = '${do_date}'
                            and type = 'bootstrap-insert'
                    ),
    order_detail as (
                        select
                            data.amount,
                            data.sku_num,
                            data.customer_id,
                            data.order_info_id order_id,
                            data.product_group_id,
                            data.product_sku_id sku_id,
                            data.shop_id
                        from ods_order_detail_inc
                        where dt =  '${do_date}'
                    )

insert overwrite table dwd_trade_refund_suc_inc partition (dt)
select
    order_info.order_id         ,
    refund_time      ,
    refund_date      ,
    shop_id          ,
    customer_id      ,
    promotion_id     ,
    sku_id           ,
    product_group_id ,
    sku_num          ,
    amount original_amount  ,
    (amount/original_amount) * reduce_amount    ,
    (amount/original_amount) * actual_amount,
    refund_date
from refund
left join order_info on order_info.order_id = refund.order_id
left join order_detail on order_detail.order_id = refund.order_id;"

	dwd_interaction_comment_inc="set hive.exec.dynamic.partition.mode=nonstrict;
with
    order_info   as (
                        select
                            data.id order_id,
                            data.comment,
                            data.rating,
                            data.original_amount,
                            data.reduce_amount,
                            data.actual_amount,
                            data.promotion_id
                        from ods_order_info_inc
                        where dt = '${do_date}'
                          and type = 'bootstrap-insert'
                          and data.status in (4, 5, 6)
                    ),
    order_detail as (
                        select
                            data.amount,
                            data.sku_num,
                            data.customer_id,
                            data.order_info_id  order_id,
                            data.product_group_id,
                            data.product_sku_id sku_id,
                            data.shop_id
                        from ods_order_detail_inc
                        where dt = '${do_date}'
                          and type = 'bootstrap-insert'
                    ),
    cm_t         as (
                        select
                            data.order_info_id                          order_id,
                            data.create_time                            comment_time,
                            date_format(data.create_time, 'yyyy-MM-dd') comment_date
                        from ods_order_status_log_inc
                        where dt = '${do_date}'
                          and type = 'bootstrap-insert'
                          and data.status = 4
                    )

insert overwrite table dwd_interaction_comment_inc partition (dt)
select
    cm_t.order_id,
    comment_time,
    comment_date,
    comment,
    rating,
    shop_id,
    customer_id,
    promotion_id,
    sku_id,
    product_group_id,
    sku_num,
    amount original_amount,
    (amount / original_amount) * reduce_amount,
    (amount / original_amount) * actual_amount,comment_date
from cm_t
         left join order_info on order_info.order_id = cm_t.order_id
         left join order_detail on order_detail.order_id = cm_t.order_id;"

else 
	# 每日sql
	dwd_trade_order_detail_inc="with order_info as (
    select
        data.id order_id,
        data.promotion_id,
        data.original_amount,
        data.reduce_amount,
        data.actual_amount
    from ods_order_info_inc
    where dt = '${do_date}' and type = 'insert'
),
    order_detail as(
    select
        data.order_info_id order_id,
        data.create_time order_time,
        date_format(data.create_time,'yyyy-MM-dd') order_date,
        data.sku_num,
        data.product_sku_id sku_id,
        data.product_group_id,
        data.amount,
        data.shop_id,
        data.customer_id
    from ods_order_detail_inc
    where dt = '${do_date}' and type = 'insert'
),
    order_t as (
        select
            order_detail.order_id,
            order_time       ,
            order_date       ,
            shop_id          ,
            customer_id      ,
            promotion_id     ,
            sku_id           ,
            product_group_id ,
            sku_num          ,
            amount original_amount  ,
            (amount/original_amount) * reduce_amount reduce_amount  ,
            (amount/original_amount) * actual_amount actual_amount
        from order_detail
        left join order_info
        on order_info.order_id=order_detail.order_id
    )

insert overwrite table dwd_trade_order_detail_inc partition(dt='${do_date}')
select
    order_id         ,
    order_time       ,
    order_date       ,
    shop_id          ,
    customer_id      ,
    promotion_id     ,
    sku_id           ,
    product_group_id ,
    sku_num          ,
    original_amount  ,
    reduce_amount    ,
    actual_amount
from order_t;"

dwd_trade_payment_inc="with
    pay          as (
                        select
                            data.order_info_id                          order_id,
                            data.update_time                            pay_time,
                            date_format(data.update_time, 'yyyy-MM-dd') pay_date
                        from ods_order_status_log_inc
                        where dt = '${do_date}'
                          and data.status = '2'
                          and type = 'insert'
                    ),
    order_info   as (
                        select
                            data.id order_id,
                            data.original_amount,
                            data.actual_amount,
                            data.reduce_amount,
                            data.promotion_id
                        from ods_order_info_inc
                        where dt = '${do_date}'
                          and type = 'update' and data.status = 2 and array_contains(map_keys(old), 'status')
                    ),
    order_detail as (
                        select
                            data.product_sku_id sku_id,
                            data.product_group_id,
                            data.order_info_id  order_id,
                            data.shop_id,
                            data.customer_id,
                            data.sku_num,
                            data.amount
                        from ods_order_detail_inc
                        where dt >= date_sub('${do_date}', 1)
                          and type in ('insert', 'bootstrap-insert')
                    )


insert overwrite table dwd_trade_payment_inc partition(dt='${do_date}')
select
    pay.order_id,
    pay_time,
    pay_date,
    shop_id,
    customer_id,
    promotion_id,
    order_detail.sku_id,
    product_group_id,
    sku_num,
    amount                                     original_amount,
    (amount / original_amount) * reduce_amount reduce_amount,
    (amount / original_amount) * actual_amount actual_amount
from pay
         left join order_info on pay.order_id = order_info.order_id
         left join order_detail on pay.order_id = order_detail.order_id;"
		 
		 
		 dwd_trade_refund_inc="with
    order_info   as (
                        select
                            data.id order_id,
                            data.original_amount,
                            data.reduce_amount,
                            data.actual_amount,
                            data.promotion_id,
                            data.update_time refund_time,
                            date_format(data.update_time, 'yyyy-MM-dd') refund_date
                        from ods_order_info_inc
                        where dt = '${do_date}'
                          and data.status = 6
                            and type = 'update'
                    ),
    order_detail as (
                        select
                            data.amount,
                            data.sku_num,
                            data.customer_id,
                            data.order_info_id order_id,
                            data.product_group_id,
                            data.product_sku_id sku_id,
                            data.shop_id
                        from ods_order_detail_inc
                        where dt >= date_sub('${do_date}', 1)
                        and type in ('bootstrap-insert', 'insert')
                    )

insert overwrite table dwd_trade_refund_suc_inc partition(dt='${do_date}')
select
    order_info.order_id         ,
    refund_time      ,
    refund_date      ,
    shop_id          ,
    customer_id      ,
    promotion_id     ,
    sku_id           ,
    product_group_id ,
    sku_num          ,
    amount original_amount  ,
    (amount/original_amount) * reduce_amount    ,
    (amount/original_amount) * actual_amount
from order_info
left join order_detail on order_detail.order_id = order_info.order_id;"

	dwd_trade_refund_suc_inc="with
    order_info   as (
                        select
                            data.id order_id,
                            data.original_amount,
                            data.reduce_amount,
                            data.actual_amount,
                            data.promotion_id,
                            data.update_time refund_time,
                            date_format(data.update_time, 'yyyy-MM-dd') refund_date
                        from ods_order_info_inc
                        where dt = '${do_date}'
                          and data.status = 6
                            and type = 'update'
                    ),
    order_detail as (
                        select
                            data.amount,
                            data.sku_num,
                            data.customer_id,
                            data.order_info_id order_id,
                            data.product_group_id,
                            data.product_sku_id sku_id,
                            data.shop_id
                        from ods_order_detail_inc
                        where dt >= date_sub('${do_date}', 1)
                        and type in ('bootstrap-insert', 'insert')
                    )

insert overwrite table dwd_trade_refund_suc_inc partition(dt='${do_date}')
select
    order_info.order_id         ,
    refund_time      ,
    refund_date      ,
    shop_id          ,
    customer_id      ,
    promotion_id     ,
    sku_id           ,
    product_group_id ,
    sku_num          ,
    amount original_amount  ,
    (amount/original_amount) * reduce_amount    ,
    (amount/original_amount) * actual_amount
from order_info
left join order_detail on order_detail.order_id = order_info.order_id;"
	
	dwd_interaction_comment_inc="with
    order_info   as (
                        select
                            data.id                                     order_id,
                            data.comment,
                            data.rating,
                            data.original_amount,
                            data.reduce_amount,
                            data.actual_amount,
                            data.promotion_id,
                            data.update_time                            comment_time,
                            date_format(data.update_time, 'yyyy-MM-dd') comment_date
                        from ods_order_info_inc
                        where dt = '${do_date}'
                          and type = 'update'
                          and data.status = 4
                    ),
    order_detail as (
                        select
                            data.amount,
                            data.sku_num,
                            data.customer_id,
                            data.order_info_id  order_id,
                            data.product_group_id,
                            data.product_sku_id sku_id,
                            data.shop_id,
                            dt,type
                        from ods_order_detail_inc
                        where dt >= date_sub('${do_date}', 1)
                          and type in ('insert', 'bootstrap-insert')
                    )
insert overwrite table dwd_interaction_comment_inc partition (dt = '${do_date}')
select
    order_info.order_id,
    comment_time,
    comment_date,
    comment,
    rating,
    shop_id,
    customer_id,
    promotion_id,
    sku_id,
    product_group_id,
    sku_num,
    amount original_amount,
    (amount / original_amount) * reduce_amount,
    (amount / original_amount) * actual_amount
from order_info
         left join order_detail on order_detail.order_id = order_info.order_id;"
fi


case $1 in 
"dwd_interaction_comment_inc" | "dwd_trade_order_detail_inc" | "dwd_trade_payment_inc" | "dwd_trade_refund_inc" | "dwd_trade_refund_suc_inc" )
	sql="${sql}${!1}"
;;
"all")
	sql="$sql${dwd_interaction_comment_inc}${dwd_trade_order_detail_inc}${dwd_trade_payment_inc}${dwd_trade_refund_inc}${dwd_trade_refund_suc_inc}"
;;
esac

# 插入表
echo "1. dt=${do_date}"
echo "2. 装载类型：${init}（1为首日，0为每日）"
echo "3. 执行insert语句...."
hive -e "${sql}"
echo "执行完成>>>"
