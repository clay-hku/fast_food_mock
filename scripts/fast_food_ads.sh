#! /bin/bash

if [ -n $2 ]
then 
	do_date=$2
else
	do_date=$(date -d "-1 days")
fi

sql="use fast_food;"

ads_trade_shop_stats="with
    t1 as (
              select
                  '${do_date}'       dt,
                  cast(1 as tinyint) recent_days,
                  shop_id,
                  shop_name,
                  order_count_1d     order_count,
                  order_users_1d     order_users,
                  order_amount_1d    order_amount
              from dws_trade_shop_order_1d
              where dt = '${do_date}'
              union all
              select
                  dt,
                  cast(recent_days as tinyint),
                  shop_id,
                  shop_name,
                  order_count_nd  order_count,
                  order_users_nd  order_users,
                  order_amount_nd order_amount
              from dws_trade_shop_order_nd
              where dt = '${do_date}'
          ),
    t2 as (
              select
                  '${do_date}',
                  recent_days,
                  shop_id,
                  shop_name,
                  sum(refund_amount_1d) refund_amount
              from dws_trade_shop_refund_1d t1d
                       lateral view explode(array(1, 7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by recent_days, shop_id, shop_name

          ),
    t3 as (

              select
                  '${do_date}',
                  recent_days,
                  shop_id,
                  shop_name,
                  sum(actual_amount_1d) actual_amount
              from dws_trade_shop_payment_1d t1d
                       lateral view explode(array(1, 7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by recent_days, shop_id, shop_name

          )
insert
overwrite
table
ads_trade_shop_stats
select *
from ads_trade_shop_stats
union
select
    '${do_date}' dt,
    coalesce(t1.recent_days, t2.recent_days, t3.recent_days),
    coalesce(t1.shop_id, t2.shop_id, t3.shop_id),
    coalesce(t1.shop_name, t2.shop_name, t3.shop_name),
    order_count,
    order_users,
    order_amount,
    actual_amount,
    refund_amount
from t1
         full join t2 on t1.shop_id = t2.shop_id and t1.recent_days = t2.recent_days
         full join t3 on t1.shop_id = t3.shop_id and t1.recent_days = t3.recent_days;"
		 
		 ads_trade_type_shop_stats="with
    t1 as (
              select
                  '${do_date}'         dt,
                  type,
                  type_name,
                  cast(1 as tinyint)   recent_days,
                  sum(order_amount_1d) order_amount,
                  sum(order_count_1d)  order_count
              from dws_trade_shop_order_1d
              where dt = '${do_date}'
              group by type, type_name
              union all
              select
                  '${do_date}'         dt,
                  type,
                  type_name,
                  cast(recent_days as tinyint),
                  sum(order_amount_nd) order_amount,
                  sum(order_count_nd)  order_count
              from dws_trade_shop_order_nd
              where dt = '${do_date}'
              group by type, type_name, recent_days
          ),
    t2 as (
              select
                  '${do_date}'          dt,
                  type,
                  type_name,
                  recent_days,
                  sum(refund_amount_1d) refund_amount
              from dws_trade_shop_refund_1d
                       lateral view explode(array(1, 7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by type, type_name, recent_days
          ),
    t3 as (
              select
                  '${do_date}'          dt,
                  type,
                  type_name,
                  recent_days,
                  sum(actual_amount_1d) actual_amount
              from dws_trade_shop_payment_1d
                       lateral view explode(array(1, 7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by type, type_name, recent_days
          ),
    t4 as (
              select
                  type,
                  type_name,
                  recent_days,
                  count(distinct customer_id) order_users
              from (
                       select
                           dt,
                           customer_id,
                           type,
                           type_name
                       from (
                                select
                                    dt,
                                    customer_id,
                                    shop_id
                                from dwd_trade_order_detail_inc
                            ) ta
                                left join (
                                              select
                                                  id,
                                                  type,
                                                  type_name
                                              from dim_shop_full
                                              where dt = '${do_date}'
                                          ) tb on ta.shop_id = tb.id
                   ) t_ab
                       lateral view explode(array(1, 7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by type, type_name, recent_days
          )

insert
overwrite
table
ads_trade_type_shop_stats
select *
from ads_trade_type_shop_stats
union
select
    t1.dt,
    t1.recent_days,
    t1.type,
    t1.type_name,
    order_count,
    order_users,
    order_amount,
    actual_amount,
    coalesce(refund_amount, 0.0)
from t1
         full join t2 on t1.type = t2.type and t1.recent_days = t2.recent_days
         full join t3 on t1.type = t3.type and t1.recent_days = t3.recent_days
         full join t4 on t1.type = t4.type and t1.recent_days = t4.recent_days;"
		 ads_trade_province_stats="with
    t1 as (
              select
                  province_id,
                  province_name,
                  '${do_date}'         dt,
                  cast(1 as tinyint)   recent_days,
                  sum(order_count_1d)  order_count,
                  sum(order_amount_1d) order_amount
              from dws_trade_shop_order_1d
              where dt = '${do_date}'
              group by province_id, province_name
              union all
              select
                  province_id,
                  province_name,
                  '${do_date}'                 dt,
                  cast(recent_days as tinyint) recent_days,
                  sum(order_count_nd)          order_count,
                  sum(order_amount_nd)         order_amount
              from dws_trade_shop_order_nd
              where dt = '${do_date}'
              group by province_id, province_name, recent_days
          ),
    t2 as (
              select
                  province_id,
                  province_name,
                  '${do_date}'          dt,
                  recent_days,
                  sum(refund_amount_1d) refund_amount
              from dws_trade_shop_refund_1d
                       lateral view explode(array(1, 7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by province_id, province_name, recent_days
          ),
    t3 as (
              select
                  province_id,
                  province_name,
                  '${do_date}'          dt,
                  recent_days,
                  sum(actual_amount_1d) actual_amount
              from dws_trade_shop_payment_1d
                       lateral view explode(array(1, 7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by province_id, province_name, recent_days
          ),
    t4 as (
              select
                  province_id,
                  province_name,
                  count(distinct customer_id) order_users,
                  recent_days
              from (
                       select
                           dt,
                           customer_id,
                           province_id,
                           province_name
                       from (
                                select
                                    shop_id,
                                    customer_id,
                                    dt
                                from dwd_trade_order_detail_inc
                                where dt between date_sub('${do_date}', 29) and '${do_date}'
                            ) ta
                                left join
                            (
                                select
                                    shop_id,
                                    province_id,
                                    province_name
                                from dwm_dim_shop_region_full
                                where dt = '${do_date}'
                            ) tb
                            on ta.shop_id = tb.shop_id
                   ) tab
                       lateral view explode(array(1, 7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by province_id, province_name, recent_days
          )

insert
overwrite
table
ads_trade_type_shop_stats
select *
from ads_trade_type_shop_stats
union
select
    '${do_date}',
    coalesce(t1.recent_days, t3.recent_days),
    coalesce(t1.province_id, t3.province_id),
    coalesce(t1.province_name, t3.province_name),
    order_count,
    order_users,
    order_amount,
    actual_amount,
    refund_amount
from t1
         left join t2 on t1.province_id = t2.province_id and t1.recent_days = t2.recent_days
         full join t3 on t1.province_id = t3.province_id and t1.recent_days = t3.recent_days
         left join t4 on t1.province_id = t4.province_id and t1.recent_days = t4.recent_days;"
		 ads_trade_hour_stats="insert overwrite table ads_trade_hour_stats
select
    dt,
    hour,
    order_amount,
    order_count,
    order_users
from ads_trade_hour_stats
union
select
    '${do_date}'                             dt,
    date_format(order_time, 'yyyy-MM-dd HH') hour,
    count(distinct order_id)                 order_count,
    sum(original_amount)                     order_amount,
    count(distinct customer_id)              order_users
from dwd_trade_order_detail_inc
where dt = '${do_date}'
group by date_format(order_time, 'yyyy-MM-dd HH');"

ads_ranking_sku_reduce_top10_stats="
insert overwrite table ads_ranking_sku_reduce_top10_stats
select
    dt,
    sku_id,
    sku_name,
    reduce_amount,
    rank
from ads_ranking_sku_reduce_top10_stats
union
select
    dt,
    sku_id,
    sku_name,
    order_amount_1d reduce_amount,
    rk
from (
         select
             dt,
             sku_id,
             sku_name,
             order_amount_1d,
             dense_rank() over (order by order_amount_1d desc) as rk
         from dws_trade_sku_order_1d
         where dt = '${do_date}'
     ) t
where rk <= 10;"
ads_ranking_shop_order_amount_top10_stats="insert overwrite table ads_ranking_shop_order_amount_top10_stats
select
    dt,
    shop_id,
    shop_name,
    order_amount,
    rank
from ads_ranking_shop_order_amount_top10_stats
union
select
    dt,
    shop_id,
    shop_name,
    order_amount,
    rank
from (
         select
             dt,
             shop_id,
             shop_name,
             order_amount_1d                                      order_amount,
             dense_rank() over (order by order_amount_1d desc) as rank
         from dws_trade_shop_order_1d
         where dt = '${do_date}'
     ) t
where rank <= 10;"
ads_promotion_reduce_amount_stats="insert overwrite table ads_promotion_reduce_amount_stats
select
    dt,
    recent_days,
    reduce_amount
from ads_promotion_reduce_amount_stats
union
select
    '${do_date}'                   dt,
    recent_days,
    sum(order_promotion_amount_1d) reduce_amount
from dws_trade_shop_order_1d
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
group by recent_days;"
ads_promotion_split_amount_stats="insert overwrite table ads_promotion_split_amount_stats
select
    dt,
    recent_days,
    shop_1_share_amount_1d,
    shop_2_share_amount_1d,
    company_share_amount_1d
from ads_promotion_split_amount_stats
union
select
    '${do_date}'                               dt,
    recent_days,
    sum(if(type = 1, shop_share_amount_1d, 0)) shop_1_share_amount_1d,
    sum(if(type = 2, shop_share_amount_1d, 0)) shop_2_share_amount_1d,
    sum(company_share_amount_1d)               company_share_amount_1d
from (
         select
             dt,
             type,
             shop_share_amount_1d,
             company_share_amount_1d
         from dws_trade_shop_payment_1d
         where dt between date_sub('${do_date}', 29) and '${do_date}'
     ) ta
         lateral view explode(array(1, 7, 30)) tmp as recent_days
where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
group by recent_days;"
ads_promotion_promotion_stats="insert overwrite table ads_promotion_promotion_stats
select
    dt,
    promotion_id,
    promotion_name,
    recent_days,
    reduce_amount,
    order_count,
    order_users
from ads_promotion_promotion_stats
union
select
    dt,
    promotion_id,
    promotion_name,
    recent_days,
    coalesce(reduce_amount, 0),
    coalesce(order_count, 0),
    coalesce(order_users, 0)
from (
         select
             '${do_date}'              dt,
             promotion_id,
             name                      promotion_name,
             cast(1 as tinyint)        recent_days,
             order_promotion_amount_1d reduce_amount,
             order_count_1d            order_count,
             order_user_count_1d       order_users
         from dws_trade_promotion_order_1d
         where dt = '${do_date}'
         union all
         select
             '${do_date}'              dt,
             promotion_id,
             name,
             cast(recent_days as tinyint),
             order_promotion_amount_nd reduce_amount,
             order_count_nd            order_count,
             order_user_count_nd       order_users
         from dws_trade_promotion_order_nd
         where dt = '${do_date}'
     ) t;"
	 ads_sku_hour_stats="insert overwrite table ads_sku_hour_stats
select
    dt,
    sku_id,
    sku_name,
    hour,
    order_amount,
    order_count,
    order_users
from ads_sku_hour_stats
union
select
    dt,
    t.sku_id,
    sku_name,
    hour,
    order_amount,
    order_count,
    order_users
from (
         select
             dt,
             sku_id,
             date_format(order_time, 'yyyy-MM-dd HH') hour,
             sum(original_amount)                     order_amount,
             count(distinct order_id)                 order_count,
             count(distinct customer_id)              order_users
         from dwd_trade_order_detail_inc
         where dt = '${do_date}'
           and sku_id is not null
         group by dt, sku_id, date_format(order_time, 'yyyy-MM-dd HH')
     ) t
         left join
     (
         select
             sku_id,
             sku_name
         from dim_product_full
         where dt = '${do_date}'
     ) t1
     on t1.sku_id = t.sku_id;"
	 ads_trade_group_stats="insert overwrite table ads_trade_group_stats
select
   dt                  ,
    recent_days        ,
    product_group_id   ,
    product_group_name ,
    order_amount       ,
    order_users
from ads_trade_group_stats
union
select
       dt                  ,
    recent_days        ,
    product_group_id   ,
    product_group_name ,
    order_amount       ,
    order_users
from (select
    '${do_date}' dt,
    cast(1 as tinyint) recent_days,
    product_group_id,
    name                product_group_name,
    order_amount_1d     order_amount,
    order_user_count_1d order_users
from dws_trade_product_group_order_1d
where dt = '${do_date}'
union all
select
    '${do_date}',
    cast(recent_days as tinyint),
    product_group_id,
    name product_group_name,
    order_amount_nd,
    order_user_count_nd
from dws_trade_product_group_order_nd
where dt = '${do_date}') t;
"
ads_group_hour_stats="insert overwrite table ads_group_hour_stats
select
    dt,
    product_group_id,
    product_group_name,
    hour,
    order_amount,
    order_count,
    order_users
from ads_group_hour_stats
union
select
    dt,
    t.product_group_id,
    product_group_name,
    hour,
    order_amount,
    order_count,
    order_users
from (
         select
             '${do_date}' dt,
             product_group_id,
             date_format(order_time, 'yyyy-MM-dd HH') hour,
             sum(original_amount)                     order_amount,
             count(distinct order_id)                 order_count,
             count(distinct customer_id)              order_users
         from dwd_trade_order_detail_inc
         where dt = '${do_date}' and product_group_id is not null
         group by product_group_id,  date_format(order_time, 'yyyy-MM-dd HH')
     ) t
left join
    (
        select
            id product_group_id,
            name product_group_name
        from dim_product_group_full
        where dt= '${do_date}'
    ) t1 on t.product_group_id = t1.product_group_id;"
	ads_comment_sku_stats="insert overwrite table ads_comment_sku_stats
select
    dt,
    recent_days,
    sku_id,
    sku_name,
    rating_count,
    rating_5_count,
    rating_5_rate
from ads_comment_sku_stats
union
select
    dt,
    recent_days,
    sku_id,
    sku_name,
    rating_count,
    rating_5_count,
    rating_5_rate
from (
         select
             '${do_date}'                                                     dt,
             cast(1 as tinyint)                                               recent_days,
             sku_id,
             sku_name,
             rating_count_1d                                                  rating_count,
             rating_5_count_1d                                                rating_5_count,
             concat(round(100 * rating_5_count_1d / rating_count_1d, 2), '%') rating_5_rate
         from dws_interaction_sku_rating_1d
         where dt = '${do_date}'
         union all
         select
             '${do_date}'                                                     dt,
             cast(recent_days as tinyint)                                               recent_days,
             sku_id,
             sku_name,
             rating_count_nd                                                  rating_count,
             rating_5_count_nd                                                rating_5_count,
             concat(round(100 * rating_5_count_nd / rating_count_nd, 2), '%') rating_5_rate
         from dws_interaction_sku_rating_nd
         where dt = '${do_date}'
     ) t;
	"

	 ads_comment_shop_stats="insert overwrite table ads_comment_shop_stats
select
    dt,
    recent_days,
    shop_id,
    shop_name,
    rating_count,
    rating_5_count,
    rating_5_rate,
    avg_rating
from ads_comment_shop_stats
union
select
    dt,
    recent_days,
    shop_id,
    shop_name,
    rating_count,
    rating_5_count,
    rating_5_rate,
    avg_rating
from (
         select
             dt,
             cast(1 as tinyint)                                               recent_days,
             shop_id,
             shop_name,
             rating_count_1d                                                  rating_count,
             rating_5_count_1d                                                rating_5_count,
             concat(round(100 * rating_5_count_1d / rating_count_1d, 2), '%') rating_5_rate,
             avg_rating_1d                                                    avg_rating
         from dws_interaction_shop_rating_1d
	where dt = '${do_date}'         
union all
         select
             dt,
             cast(recent_days as tinyint)                                     recent_days,
             shop_id,
             shop_name,
             rating_count_nd                                                  rating_count,
             rating_5_count_nd                                                rating_5_count,
             concat(round(100 * rating_5_count_nd / rating_count_nd, 2), '%') rating_5_rate,
             avg_rating_nd                                                    avg_rating
         from dws_interaction_shop_rating_nd
	where dt = '${do_date}'     
) t;"
	 
	 ads_comment_group_stats="insert overwrite table ads_comment_group_stats
select
    dt,
    recent_days,
    product_group_id,
    product_group_name,
    rating_count,
    rating_5_count,
    rating_5_rate,
    avg_rating
from ads_comment_group_stats
union
select
    dt,
    recent_days,
    product_group_id,
    product_group_name,
    rating_count,
    rating_5_count,
    rating_5_rate,
    avg_rating
from (
         select
             dt,
             cast(1 as tinyint)                                               recent_days,
             product_group_id,
             name                                                             product_group_name,
             rating_count_1d                                                  rating_count,
             rating_5_count_1d                                                rating_5_count,
             concat(round(100 * rating_5_count_1d / rating_count_1d, 2), '%') rating_5_rate,
             avg_rating_1d                                                    avg_rating
         from dws_interaction_product_group_rating_1d
	where dt = '${do_date}'        
 union all
         select
             dt,
             cast(recent_days as tinyint)                                     recent_days,
             product_group_id,
             name                                                             product_group_name,
             rating_count_nd                                                  rating_count,
             rating_5_count_nd                                                rating_5_count,
             concat(round(100 * rating_5_count_nd / rating_count_nd, 2), '%') rating_5_rate,
             avg_rating_nd                                                    avg_rating
         from dws_interaction_product_group_rating_nd
	where dt = '${do_date}'
     ) t;"
	 
case $1 in 
"ads_comment_group_stats" |"ads_comment_shop_stats" |"ads_comment_sku_stats" |"ads_group_hour_stats" |"ads_promotion_promotion_stats" |"ads_promotion_reduce_amount_stats" |"ads_promotion_split_amount_stats" |"ads_ranking_shop_order_amount_top10_stats" |"ads_ranking_sku_reduce_top10_stats" |"ads_sku_hour_stats" |"ads_trade_group_stats" |"ads_trade_hour_stats" |"ads_trade_shop_stats" |"ads_trade_type_shop_stats" )
hive -e "${sql}${!1}"
;;
"all")
hive -e "${sql}${ads_comment_group_stats}${ads_comment_shop_stats}${ads_comment_sku_stats}${ads_group_hour_stats}${ads_promotion_promotion_stats}${ads_promotion_reduce_amount_stats}${ads_promotion_split_amount_stats}${ads_ranking_shop_order_amount_top10_stats}${ads_ranking_sku_reduce_top10_stats}${ads_sku_hour_stats}${ads_trade_group_stats}${ads_trade_hour_stats}${ads_trade_shop_stats}${ads_trade_type_shop_stats}"
;;
esac
