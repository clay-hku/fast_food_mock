#! /bin/bash


db="fast_food"

if [ -n "$2" ]
	then 
		do_date=$2

	else
		echo "请输入日期"
		exit
fi

sql="use ${db};"


dws_trade_shop_order_nd="with
    order_nd        as (
                           select
                               shop_id,
                               recent_days,
                               sum(order_count_1d)            order_count_nd,
                               sum(order_amount_1d)           order_amount_nd,
                               sum(order_promotion_count_1d)  order_promotion_count_nd,
                               sum(order_promotion_amount_1d) order_promotion_amount_nd
                           from dws_trade_shop_order_1d
                                    lateral view explode(array(7, 30)) tmp as recent_days
                           where dt >= date_sub('${do_date}', recent_days - 1) and dt <= '${do_date}'
                           group by shop_id, recent_days
                       ),
    order_detail_nd as (
                           select
                               shop_id,
                               shop_name,
                               type,
                               type_name,
                               region_id,
                               region_name,
                               province_id,
                               province_name
                           from dwm_dim_shop_region_full
                           where dt = '${do_date}'
                       ),
    users_count_nd  as (
                           select
                               shop_id,
                               recent_days,
                               count(distinct customer_id) order_users_nd
                           from dwd_trade_order_detail_inc
                                    lateral view explode(array(7, 30)) tmp as recent_days
                           where dt >= date_sub('${do_date}', recent_days - 1) and dt <= '${do_date}'
                           group by shop_id, recent_days
                       )

insert overwrite table dws_trade_shop_order_nd partition ( dt = '${do_date}' )
select
    order_nd.shop_id,
    shop_name,
    type,
    type_name,
    region_id,
    region_name,
    province_id,
    province_name,
    order_count_nd,
    order_users_nd,
    order_amount_nd,
    order_promotion_count_nd,
    order_promotion_amount_nd,
    order_nd.recent_days
from order_nd
         left join order_detail_nd on order_nd.shop_id = order_detail_nd.shop_id
         left join users_count_nd
                   on users_count_nd.shop_id = order_nd.shop_id and users_count_nd.recent_days = order_nd.recent_days;"


dws_interaction_shop_rating_nd="with
    data_rating_nd  as (
                           select
                               shop_id,
                               recent_days,
                               sum(rating_count_1d)   rating_count_nd,
                               sum(rating_5_count_1d) rating_5_count_nd
                           from dws_interaction_shop_rating_1d
                                    lateral view explode(array(7, 30)) tmp as recent_days
                           where dt >= date_sub('${do_date}', recent_days - 1)
                             and dt <= '${do_date}'
                           group by shop_id, recent_days
                       ),
    data_avg_rating as (
                           select
                               shop_id,
                               recent_days,
                               avg(rating) avg_rating_nd
                           from (
                                    select
                                        shop_id,
                                        dt,
                                        max(rating) rating
                                    from dwd_interaction_comment_inc
                                    where dt between date_sub('${do_date}', 29) and '${do_date}'
                                    group by shop_id, dt, order_id
                                ) t
                                    lateral view explode(array(7, 30)) tmp as recent_days
                           where dt >= date_sub('${do_date}', recent_days - 1)
                             and dt <= '${do_date}'
                           group by shop_id, recent_days
                       ),
    details         as (
                           select
                               shop_id,
                               shop_name,
                               type,
                               type_name,
                               region_id,
                               region_name,
                               province_id,
                               province_name
                           from dwm_dim_shop_region_full
                           where dt = '${do_date}'
                       )

insert overwrite table dws_interaction_shop_rating_nd partition(dt='${do_date}')
select
    data_rating_nd.shop_id,
    shop_name,
    type,
    type_name,
    region_id,
    region_name,
    province_id,
    province_name,
    avg_rating_nd,
    rating_count_nd,
    rating_5_count_nd,
    data_rating_nd.recent_days
from data_rating_nd
         left join data_avg_rating on data_avg_rating.shop_id = data_rating_nd.shop_id and
                                      data_avg_rating.recent_days = data_rating_nd.recent_days
         left join details on data_rating_nd.shop_id = details.shop_id;"

dwd_trade_order_detail_inc="with
    t1 as (
        select
            sku_id,
            recent_days,
            sum(order_count_1d) order_count_nd,
            sum(order_amount_1d) order_amount_nd,
            sum(order_promotion_count_1d) order_promotion_count_nd,
            sum(order_promotion_amount_1d) order_promotion_amount_nd
        from dws_trade_sku_order_1d
        lateral view explode(array(7, 30)) tmp as recent_days
        where dt between date_sub('${do_date}', recent_days-1) and '${do_date}'
        group by sku_id, recent_days
    ),
    sku_info as (
                    select
                        sku_id,
                        sku_name,
                        sku_price,
                        spu_id,
                        spu_name,
                        spu_description,
                        product_category_id,
                        product_category_name,
                        product_category_description
                    from dim_product_full
                    where dt = '${do_date}'
                ),
    users_count as (
        select
            sku_id,
            recent_days,
            count(distinct customer_id) order_users_nd
        from
        (select
             dt,
            customer_id,
            sku_id
        from dwd_trade_order_detail_inc
        where dt between date_sub('${do_date}', 30-1) and '${do_date}'
        and sku_id is not null) t
        lateral view explode(array(7, 30)) tmp as recent_days
        where dt between date_sub('${do_date}', recent_days-1) and '${do_date}'
        group by sku_id, recent_days
                   )

insert overwrite table dws_trade_sku_order_nd partition(dt = '${do_date}')
select
    t1.sku_id,
    sku_name,
    sku_price,
    spu_id,
    spu_name,
    spu_description,
    product_category_id,
    product_category_name,
    product_category_description,
    order_count_nd,
    order_users_nd,
    order_amount_nd,
    order_promotion_count_nd,
    order_promotion_amount_nd,
    t1.recent_days
from t1
         left join users_count on users_count.sku_id = t1.sku_id and users_count.recent_days = t1.recent_days
         left join sku_info on sku_info.sku_id = t1.sku_id;"


dws_interaction_sku_rating_nd="with
    t1 as (
              select
                  sku_id,
                  recent_days,
                  sum(rating_count_1d) rating_count_nd,
                  sum(rating_5_count_1d) rating_5_count_nd
              from dws_interaction_sku_rating_1d
                       lateral view explode(array(7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by sku_id, recent_days
    ),
    t2 as (
        select
    sku_id                       ,
    sku_name                     ,
    sku_price                    ,
    spu_id                       ,
    spu_name                     ,
    spu_description              ,
    product_category_id          ,
    product_category_name        ,
    product_category_description
        from dim_product_full
        where dt = '${do_date}'
          )

insert overwrite table dws_interaction_sku_rating_nd partition(dt = '${do_date}')
select
    t1.sku_id,
    sku_name,
    sku_price,
    spu_id,
    spu_name,
    spu_description,
    product_category_id,
    product_category_name,
    product_category_description,
    rating_count_nd,
    rating_5_count_nd,
    recent_days
from t1
         left join t2 on t1.sku_id = t2.sku_id;"

dws_trade_promotion_order_nd="with
    t1 as (
        select
            promotion_id,
            recent_days,
            sum(order_count_1d) order_count_nd,
            sum(order_promotion_amount_1d) order_promotion_amount_nd
        from dws_trade_promotion_order_1d
        lateral view explode(array(7, 30)) tmp as recent_days
        where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
        group by promotion_id, recent_days
    ),
    t2 as (
        select
            promotion_id,
            recent_days,
            count(distinct customer_id)order_user_count_nd
        from dwd_trade_order_detail_inc
        lateral view explode(array(7,30)) tmp as recent_days
        where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
        and promotion_id is not null
        group by promotion_id, recent_days
          ),
    t3 as (
        select
            id promotion_id              ,
            name                      ,
            company_share             ,
            reduce_amount             ,
            threshold_amount
        from dim_promotion_full
        where dt = '${do_date}'
          )

insert overwrite table dws_trade_promotion_order_nd partition (dt = '${do_date}')
select
    t1.promotion_id              ,
    name                      ,
    company_share             ,
    reduce_amount             ,
    threshold_amount          ,
    order_count_nd            ,
    order_promotion_amount_nd ,
    order_user_count_nd       ,
    t1.recent_days
from t1
left join t2 on t1.promotion_id = t2.promotion_id and t1.recent_days = t2.recent_days
left join t3 on t1.promotion_id = t3.promotion_id;"

dws_trade_product_group_order_nd="with
    t1 as (
        select
            product_group_id,
            recent_days,
            sum(order_amount_1d) order_amount_nd,
            sum(order_reduce_amount_1d) order_reduce_amount_nd
        from dws_trade_product_group_order_1d
        lateral view explode(array(7, 30)) tmp as recent_days
        where dt between date_sub('${do_date}', recent_days -1) and '${do_date}'
        group by product_group_id , recent_days
    ),
    t2 as(
        select
            product_group_id,
            recent_days,
            count(distinct customer_id) order_user_count_nd
        from dwd_trade_order_detail_inc
                lateral view explode(array(7, 30)) tmp as recent_days
        where dt between date_sub('${do_date}', recent_days -1) and '${do_date}'
        and product_group_id is not null
        group by product_group_id , recent_days
         ),
    t3 as (
        select
                id product_group_id       ,
                name                   ,
                original_price         ,
                price                  ,
                sku_group
        from dim_product_group_full
        where dt = '${do_date}'
          )
    insert overwrite table dws_trade_product_group_order_nd partition (dt='${do_date}')
    select 
            t1.product_group_id       ,
            name                   ,
            original_price         ,
            price                  ,
            sku_group              ,
            order_amount_nd        ,
            order_reduce_amount_nd ,
            order_user_count_nd,
            t1.recent_days
    from t1
left join t2 on t1.product_group_id = t2.product_group_id and t1.recent_days = t2.recent_days
left join t3 on t1.product_group_id = t3.product_group_id;"

dws_interaction_product_group_rating_nd="with
    t1 as (
              select
                  product_group_id,
                  recent_days,
                  sum(rating_count_1d)                                                       rating_count_nd,
                  sum(rating_5_count_1d)                                                     rating_5_count_nd,
                  concat(round(sum(rating_5_count_1d) * 100 / sum(rating_count_1d), 2), '%') rating_5_rate_nd
              from dws_interaction_product_group_rating_1d
                       lateral view explode(array(7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by product_group_id, recent_days
          ),
    t2 as (
              select
                  product_group_id,
                  recent_days,
                  avg(rating_s) avg_rating_nd
              from (
                       select
                           product_group_id,
                           max(rating) rating_s,
                           dt
                       from dwd_interaction_comment_inc
                       where dt between date_sub('${do_date}', 29) and '${do_date}'
                         and product_group_id is not null
                       group by dt, product_group_id, order_id
                   ) t
                       lateral view explode(array(7, 30)) tmp as recent_days
              where dt between date_sub('${do_date}', recent_days - 1) and '${do_date}'
              group by product_group_id,recent_days
          ),
    t3 as (
            select
            id product_group_id  ,
            name              ,
            original_price    ,
            price             ,
            sku_group
            from dim_product_group_full
            where dt = '${do_date}'
                  )

insert overwrite table dws_interaction_product_group_rating_nd partition(dt='${do_date}')
select
    t1.product_group_id  ,
    name              ,
    original_price    ,
    price             ,
    sku_group         ,
    rating_count_nd   ,
    rating_5_count_nd ,
    avg_rating_nd     ,
    rating_5_rate_nd  ,
    t1.recent_days
from t1
left join t2 on t1.product_group_id = t2.product_group_id and t1.recent_days  = t2.recent_days
left join t3 on t1.product_group_id = t3.product_group_id;"

case $1 in 
"dws_interaction_product_group_rating_nd" |"dws_interaction_shop_rating_nd" |"dws_interaction_sku_rating_nd" |"dws_trade_product_group_order_nd" |"dws_trade_promotion_order_nd" |"dws_trade_shop_order_nd" |"dws_trade_sku_order_nd")
sql="${sql}${!1}"
;;

"all")
sql="${sql}
${dws_interaction_product_group_rating_nd}
${dws_interaction_shop_rating_nd}
${dws_interaction_sku_rating_nd}
${dws_trade_product_group_order_nd}
${dws_trade_promotion_order_nd}
${dws_trade_shop_order_nd}
${dws_trade_sku_order_nd}"
;;
esac

hive -e "${sql}"
