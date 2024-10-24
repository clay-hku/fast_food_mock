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
sql="${sql}set hive.exec.dynamic.partition.mode=nonstrict;"
init=0
if [ -n "$3" ]
then 
	init=1
fi
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

if [ ${init} -eq 1 ]
then
	dws_trade_shop_order_1d="with
    order_1      as (
                        select
                            shop_id,
                            order_date,
                            sum(original_amount) order_amount_1d,
                            sum(reduce_amount)   order_promotion_amount_1d
                        from dwd_trade_order_detail_inc
                        where dt <= '${do_date}'
                        group by shop_id, order_date
                    ),
    order_2      as (
                        select
                            shop_id,
                            order_date,
                            count(*)            order_count_1d,
                            count(promotion_id) order_promotion_count_1d
                        from (
                                 select
                                     shop_id,
                                     order_date,
                                     order_id,
                                     promotion_id
                                 from dwd_trade_order_detail_inc
                                 where dt <= '${do_date}'
                                 group by shop_id, order_date, order_id, promotion_id
                             ) t
                        group by shop_id, order_date
                    ),
    order_user   as (
                        select
                            shop_id,
                            order_date,
                            count(distinct customer_id) order_users_1d
                        from (
                                 select
                                     shop_id,
                                     order_date,
                                     customer_id
                                 from dwd_trade_order_detail_inc
                                 where dt <= '${do_date}'
                                 group by shop_id, order_date, customer_id
                             ) t
                        group by shop_id, order_date
                    )

insert overwrite table dws_trade_shop_order_1d partition (dt)
select
    order_1.shop_id                   ,
    shop_name                 ,
    type                      ,
    type_name                 ,
    t.region_id                 ,
    region_name               ,
    province_id               ,
    province_name             ,
    order_count_1d            ,
    order_users_1d            ,
    order_amount_1d           ,
    order_promotion_count_1d  ,
    order_promotion_amount_1d,
    order_1.order_date
from order_1
         left join order_2 on order_1.shop_id = order_2.shop_id and order_1.order_date = order_2.order_date
         left join order_user on order_1.shop_id = order_user.shop_id and order_1.order_date = order_user.order_date
        left join (select * from dwm_dim_shop_region_full where dt = '${do_date}') t on order_1.shop_id = t.shop_id;"
		
		
		
		dws_trade_shop_payment_1d="with
    share as (
                 select
                     shop_id,
                     pay_date,
                     coalesce(company_share, 0) company_share,
                     actual_amount,
                     coalesce(reduce_amount, 0) reduce_amount
                 from (
                          select
                              shop_id,
                              pay_date,
                              promotion_id,
                              actual_amount,
                              reduce_amount
                          from dwd_trade_payment_inc
                          where dt <= '${do_date}'
                      ) pay
                          left join (
                                        select
                                            id,
                                            company_share
                                        from dim_promotion_full
                                        where dt = '${do_date}'
                                    ) pm
                                    on pm.id = pay.promotion_id
             ),
    pay   as (
                 select
                     shop_id,
                     pay_date,
                     sum(actual_amount)                       actual_amount_1d,
                     sum(reduce_amount * (1 - company_share)) shop_share_amount_1d,
                     sum(reduce_amount * company_share)       company_share_amount_1d
                 from share
                 group by shop_id, pay_date
             )

insert overwrite table dws_trade_shop_payment_1d partition(dt)
select
    pay.shop_id,
    shop_name,
    type,
    type_name,
    region_id,
    region_name,
    province_id,
    province_name,
    actual_amount_1d,
    shop_share_amount_1d,
    company_share_amount_1d,
    pay.pay_date
from pay
         left join (
                       select *
                       from dwm_dim_shop_region_full
                       where dt = '${do_date}'
                   ) t on t.shop_id = pay.shop_id;
"

		dws_trade_shop_refund_1d="with
    refund as (
                  select
                      shop_id,
                      refund_suc_date,
                      sum(actual_amount) refund_amount_1d
                  from dwd_trade_refund_suc_inc
                  where dt <= '${do_date}'
                  group by shop_id, refund_suc_date
    )

insert overwrite table dws_trade_shop_refund_1d partition(dt)
select
    refund.shop_id          ,
    shop_name        ,
    type             ,
    type_name        ,
    region_id        ,
    region_name      ,
    province_id      ,
    province_name    ,
    refund_amount_1d    ,
    refund_suc_date
from refund
left join (select * from dwm_dim_shop_region_full where dt = '${do_date}') t on t.shop_id = refund.shop_id;"


			dws_interaction_shop_rating_1d="with
    rating as (
                  select
                      shop_id,
                      comment_date,
                      avg(rating)               avg_rating_1d,
                      count(*)                  rating_count_1d,
                      sum(if(rating = 5, 1, 0)) rating_5_count_1d
                  from (
                           select
                               shop_id,
                               comment_date,
                               rating
                           from dwd_interaction_comment_inc
                           where dt <= '${do_date}'
                           group by shop_id, comment_date, rating, order_id
                       ) raw
                  group by shop_id, comment_date
    )


insert overwrite table dws_interaction_shop_rating_1d partition(dt)
select
    rating.shop_id,
    shop_name,
    type,
    type_name,
    region_id,
    region_name,
    province_id,
    province_name,
    avg_rating_1d,
    rating_count_1d,
    rating_5_count_1d,
    comment_date
from rating
         left join (
                       select *
                       from dwm_dim_shop_region_full
                       where dt = '${do_date}'
                   ) t on t.shop_id = rating.shop_id;"
				   
				   
				   dws_trade_sku_order_1d="with
    order_info    as (
                    select
                        sku_id,
                        dt,
                        count(distinct customer_id)                              order_users_1d,
                        count(distinct order_id)                                 order_count_1d,
                        sum(original_amount)                                     order_amount_1d,
                        count(distinct if(promotion_id is null, null, order_id)) order_promotion_count_1d,
                        sum(reduce_amount)                                       order_promotion_amount_1d
                    from dwd_trade_order_detail_inc
                    where dt <= '${do_date}' and sku_id is not null
                    group by sku_id, dt
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
                )
insert overwrite table dws_trade_sku_order_1d partition (dt)
select
    order_info.sku_id                       ,
    sku_name                     ,
    sku_price                    ,
    spu_id                       ,
    spu_name                     ,
    spu_description              ,
    product_category_id          ,
    product_category_name        ,
    product_category_description ,
    order_count_1d               ,
    order_users_1d               ,
    order_amount_1d              ,
    order_promotion_count_1d     ,
    order_promotion_amount_1d,
    dt
from order_info
left join sku_info on sku_info.sku_id = order_info.sku_id;"

		dws_interaction_sku_rating_1d="with
    rating   as (
                    select
                        sku_id,
                        dt,
                        count(*)                       rating_count_1d,
                        count(if(rating = 5, 1, null)) rating_5_count_1d
                    from (
                             select
                                 sku_id,
                                 dt,
                                 order_id,
                                 rating
                             from dwd_interaction_comment_inc
                             where dt <= '${do_date}'
                               and sku_id is not null
                             group by sku_id, dt, order_id, rating
                         ) t
                    group by sku_id, dt
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
                )

insert overwrite table dws_interaction_sku_rating_1d partition (dt)
select
    rating.sku_id,
    sku_name,
    sku_price,
    spu_id,
    spu_name,
    spu_description,
    product_category_id,
    product_category_name,
    product_category_description,
    rating_count_1d,
    rating_5_count_1d,
    dt
from rating
         left join sku_info on rating.sku_id = sku_info.sku_id;"
		 
		 dws_trade_promotion_order_1d="with
    t_order as (
                   select
                       promotion_id,
                       count(distinct order_id)    order_count_1d,
                       count(distinct customer_id) order_user_count_1d,
                       sum(reduce_amount)          order_promotion_amount_1d,
                       dt
                   from dwd_trade_order_detail_inc
                   where dt <= '${do_date}'
                     and promotion_id is not null
                   group by promotion_id, dt
    )
insert overwrite table dws_trade_promotion_order_1d partition(dt)
select
    promotion_id,
    name,
    company_share,
    reduce_amount,
    threshold_amount,
    order_count_1d,
    order_promotion_amount_1d,
    order_user_count_1d,
    dt
from t_order
         left join (
                       select
                           id,
                           name,
                           company_share,
                           reduce_amount,
                           threshold_amount
                       from dim_promotion_full
                       where dt = '${do_date}'
                   ) t1
                   on t_order.promotion_id = t1.id;"
				   
				   dws_trade_product_group_order_1d="with
    product_order  as (
                          select
                              dt,
                              product_group_id,
                              sum(original_amount)        order_amount_1d,
                              sum(reduce_amount)          order_reduce_amount_1d,
                              count(distinct customer_id) order_user_count_1d
                          from dwd_trade_order_detail_inc
                          where dt <= '${do_date}'
                            and product_group_id is not null
                          group by dt, product_group_id
                      ),
    product_detail as (
                          select
                              id product_group_id,
                              name,
                              original_price,
                              price,
                              sku_group
                          from dim_product_group_full
                          where dt = '${do_date}'
                      )

insert overwrite table dws_trade_product_group_order_1d partition(dt)
select
    product_order.product_group_id       ,
    name                   ,
    original_price         ,
    price                  ,
    sku_group              ,
    order_amount_1d        ,
    order_reduce_amount_1d ,
    order_user_count_1d,
    dt
from product_order
left join product_detail using (product_group_id);"

			dws_interaction_product_group_rating_1d="with
    product_rating as (
                          select
                              product_group_id,
                              dt,
                              count(*)                                  rating_count_1d,
                              count(if(rating = 5, 1, null))            rating_5_count_1d,
                              avg(rating)                               avg_rating_1d,
                              concat(round(count(if(rating = 5, 1, null)) * 100/ count(*), 2), '%') rating_5_rate_1d
                          from (
                                   select
                                       order_id,
                                       product_group_id,
                                       dt,
                                       rating
                                   from dwd_interaction_comment_inc
                                   where dt <= '${do_date}'
                                     and product_group_id is not null
                                   group by order_id, product_group_id, dt, rating
                               ) t
                          group by product_group_id, dt
                      ),
    product_detail as (
                          select
                              id product_group_id,
                              name,
                              original_price,
                              price,
                              sku_group
                          from dim_product_group_full
                          where dt = '${do_date}'
                      )
insert overwrite table dws_interaction_product_group_rating_1d partition (dt)
select
    product_rating.product_group_id,
    name,
    original_price,
    price,
    sku_group,
    rating_count_1d,
    rating_5_count_1d,
    avg_rating_1d,
    rating_5_rate_1d,
    dt
from product_rating
         left join product_detail on product_detail.product_group_id = product_rating.product_group_id;"

fi

if [ ${init} -eq 0 ]
then
	dws_trade_shop_order_1d="with
    order_1      as (
                        select
                            shop_id,
                            order_date,
                            sum(original_amount) order_amount_1d,
                            sum(reduce_amount)   order_promotion_amount_1d
                        from dwd_trade_order_detail_inc
                        where dt = '${do_date}'
                        group by shop_id, order_date
                    ),
    order_2      as (
                        select
                            shop_id,
                            order_date,
                            count(*)            order_count_1d,
                            count(promotion_id) order_promotion_count_1d
                        from (
                                 select
                                     shop_id,
                                     order_date,
                                     order_id,
                                     promotion_id
                                 from dwd_trade_order_detail_inc
                                 where dt = '${do_date}'
                                 group by shop_id, order_date, order_id, promotion_id
                             ) t
                        group by shop_id, order_date
                    ),
    order_user   as (
                        select
                            shop_id,
                            order_date,
                            count(distinct customer_id) order_users_1d
                        from (
                                 select
                                     shop_id,
                                     order_date,
                                     customer_id
                                 from dwd_trade_order_detail_inc
                                 where dt = '${do_date}'
                                 group by shop_id, order_date, customer_id
                             ) t
                        group by shop_id, order_date
                    )
insert overwrite table dws_trade_shop_order_1d partition (dt = '${do_date}')
select
    order_1.shop_id,
    shop_name,
    type,
    type_name,
    t.region_id,
    region_name,
    province_id,
    province_name,
    order_count_1d,
    order_users_1d,
    order_amount_1d,
    order_promotion_count_1d,
    order_promotion_amount_1d
from order_1
         left join order_2 on order_1.shop_id = order_2.shop_id and order_1.order_date = order_2.order_date
         left join order_user on order_1.shop_id = order_user.shop_id and order_1.order_date = order_user.order_date
         left join (select * from dwm_dim_shop_region_full where dt = '${do_date}') t on order_1.shop_id = t.shop_id;"
		dws_trade_shop_payment_1d="with
    share as (
                 select
                     shop_id,
                     pay_date,
                     coalesce(company_share, 0) company_share,
                     actual_amount,
                     coalesce(reduce_amount, 0) reduce_amount
                 from (
                          select
                              shop_id,
                              pay_date,
                              promotion_id,
                              actual_amount,
                              reduce_amount
                          from dwd_trade_payment_inc
                          where dt = '${do_date}'
                      ) pay
                          left join (
                                        select
                                            id,
                                            company_share
                                        from dim_promotion_full
                                        where dt = '${do_date}'
                                    ) pm
                                    on pm.id = pay.promotion_id
             ),
    pay   as (
                 select
                     shop_id,
                     pay_date,
                     sum(actual_amount)                       actual_amount_1d,
                     sum(reduce_amount * (1 - company_share)) shop_share_amount_1d,
                     sum(reduce_amount * company_share)       company_share_amount_1d
                 from share
                 group by shop_id, pay_date
             )

insert overwrite table dws_trade_shop_payment_1d partition(dt = '${do_date}')
select
    pay.shop_id,
    shop_name,
    type,
    type_name,
    region_id,
    region_name,
    province_id,
    province_name,
    actual_amount_1d,
    shop_share_amount_1d,
    company_share_amount_1d
from pay
         left join (
                       select *
                       from dwm_dim_shop_region_full
                       where dt = '${do_date}'
                   ) t on t.shop_id = pay.shop_id;"
				   
				   dws_trade_shop_refund_1d="with
    refund as (
                  select
                      shop_id,
                      refund_suc_date,
                      sum(actual_amount) refund_amount_1d
                  from dwd_trade_refund_suc_inc
                  where dt = '${do_date}'
                  group by shop_id, refund_suc_date
    )

insert overwrite table dws_trade_shop_refund_1d partition(dt = '${do_date}')
select
    refund.shop_id          ,
    shop_name        ,
    type             ,
    type_name        ,
    region_id        ,
    region_name      ,
    province_id      ,
    province_name    ,
    refund_amount_1d
from refund
left join (select * from dwm_dim_shop_region_full where dt = '${do_date}') t on t.shop_id = refund.shop_id;"


			dws_interaction_shop_rating_1d="with
    rating as (
                  select
                      shop_id,
                      avg(rating)               avg_rating_1d,
                      count(*)                  rating_count_1d,
                      sum(if(rating = 5, 1, 0)) rating_5_count_1d
                  from (
                           select
                               shop_id,
                               rating
                           from dwd_interaction_comment_inc
                           where dt = '${do_date}'
                           group by shop_id, comment_date, rating, order_id
                       ) raw
                  group by shop_id
    )

insert overwrite table dws_interaction_shop_rating_1d partition(dt = '${do_date}')
select
    rating.shop_id,
    shop_name,
    type,
    type_name,
    region_id,
    region_name,
    province_id,
    province_name,
    avg_rating_1d,
    rating_count_1d,
    rating_5_count_1d
from rating
         left join (
                       select *
                       from dwm_dim_shop_region_full
                       where dt = '${do_date}'
                   ) t on t.shop_id = rating.shop_id;"
				   
				   dws_trade_sku_order_1d="with
    order_info    as (
                    select
                        sku_id,
                        count(distinct customer_id)                              order_users_1d,
                        count(distinct order_id)                                 order_count_1d,
                        sum(original_amount)                                     order_amount_1d,
                        count(distinct if(promotion_id is null, null, order_id)) order_promotion_count_1d,
                        sum(reduce_amount)                                       order_promotion_amount_1d
                    from dwd_trade_order_detail_inc
                    where dt = '${do_date}' and sku_id is not null
                    group by sku_id
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
                )
insert overwrite table dws_trade_sku_order_1d partition(dt='${do_date}')
select
    order_info.sku_id                       ,
    sku_name                     ,
    sku_price                    ,
    spu_id                       ,
    spu_name                     ,
    spu_description              ,
    product_category_id          ,
    product_category_name        ,
    product_category_description ,
    order_count_1d               ,
    order_users_1d               ,
    order_amount_1d              ,
    order_promotion_count_1d     ,
    order_promotion_amount_1d
from order_info
left join sku_info on sku_info.sku_id = order_info.sku_id;"
				dws_interaction_sku_rating_1d="with
    rating   as (
                    select
                        sku_id,
                        count(*)                       rating_count_1d,
                        count(if(rating = 5, 1, null)) rating_5_count_1d
                    from (
                             select
                                 sku_id,
                                 order_id,
                                 rating
                             from dwd_interaction_comment_inc
                             where dt = '${do_date}'
                               and sku_id is not null
                             group by sku_id, order_id, rating
                         ) t
                    group by sku_id
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
                )

insert overwrite table dws_interaction_sku_rating_1d partition (dt = '${do_date}')
select
    rating.sku_id,
    sku_name,
    sku_price,
    spu_id,
    spu_name,
    spu_description,
    product_category_id,
    product_category_name,
    product_category_description,
    rating_count_1d,
    rating_5_count_1d
from rating
         left join sku_info on rating.sku_id = sku_info.sku_id;"
				   dws_trade_promotion_order_1d="with
    t_order as (
                   select
                       promotion_id,
                       count(distinct order_id)    order_count_1d,
                       count(distinct customer_id) order_user_count_1d,
                       sum(reduce_amount)          order_promotion_amount_1d
                   from dwd_trade_order_detail_inc
                   where dt = '${do_date}'
                     and promotion_id is not null
                   group by promotion_id, dt
    )
insert overwrite table dws_trade_promotion_order_1d partition(dt = '${do_date}')
select
    promotion_id,
    name,
    company_share,
    reduce_amount,
    threshold_amount,
    order_count_1d,
    order_promotion_amount_1d,
    order_user_count_1d
from t_order
         left join (
                       select
                           id,
                           name,
                           company_share,
                           reduce_amount,
                           threshold_amount
                       from dim_promotion_full
                       where dt = '${do_date}'
                   ) t1
                   on t_order.promotion_id = t1.id;"
				   dws_trade_product_group_order_1d="with
    product_order  as (
                          select
                              product_group_id,
                              sum(original_amount)        order_amount_1d,
                              sum(reduce_amount)          order_reduce_amount_1d,
                              count(distinct customer_id) order_user_count_1d
                          from dwd_trade_order_detail_inc
                          where dt = '${do_date}'
                            and product_group_id is not null
                          group by product_group_id
                      ),
    product_detail as (
                          select
                              id product_group_id,
                              name,
                              original_price,
                              price,
                              sku_group
                          from dim_product_group_full
                          where dt = '${do_date}'
                      )

insert overwrite table dws_trade_product_group_order_1d partition(dt = '${do_date}')
select
    product_order.product_group_id       ,
    name                   ,
    original_price         ,
    price                  ,
    sku_group              ,
    order_amount_1d        ,
    order_reduce_amount_1d ,
    order_user_count_1d
from product_order
left join product_detail using (product_group_id);"

			dws_interaction_product_group_rating_1d="with
    product_rating as (
                          select
                              product_group_id,
                              count(*)                                  rating_count_1d,
                              count(if(rating = 5, 1, null))            rating_5_count_1d,
                              avg(rating)                               avg_rating_1d,
                              concat(round(count(if(rating = 5, 1, null)) * 100/ count(*), 2), '%') rating_5_rate_1d
                          from (
                                   select
                                       order_id,
                                       product_group_id,
                                       rating
                                   from dwd_interaction_comment_inc
                                   where dt = '${do_date}'
                                     and product_group_id is not null
                                   group by order_id, product_group_id, rating
                               ) t
                          group by product_group_id
                      ),
    product_detail as (
                          select
                              id product_group_id,
                              name,
                              original_price,
                              price,
                              sku_group
                          from dim_product_group_full
                          where dt = '${do_date}'
                      )
insert overwrite table dws_interaction_product_group_rating_1d partition (dt= '${do_date}')
select
    product_rating.product_group_id,
    name,
    original_price,
    price,
    sku_group,
    rating_count_1d,
    rating_5_count_1d,
    avg_rating_1d,
    rating_5_rate_1d
from product_rating
         left join product_detail on product_detail.product_group_id = product_rating.product_group_id;"
		 

fi



case $1 in
"dws_interaction_product_group_rating_1d" | "dws_interaction_shop_rating_1d" | "dws_interaction_sku_rating_1d" | "dws_trade_product_group_order_1d" | "dws_trade_promotion_order_1d" | "dws_trade_shop_order_1d" | "dws_trade_shop_payment_1d" | "dws_trade_shop_refund_1d" | "dws_trade_sku_order_1d" )
sql="${sql}${!1}"
;;
"all")
sql="${sql}${dws_interaction_product_group_rating_1d} ${dws_interaction_shop_rating_1d} ${dws_interaction_sku_rating_1d} ${dws_trade_product_group_order_1d} ${dws_trade_promotion_order_1d} ${dws_trade_shop_order_1d} ${dws_trade_shop_payment_1d} ${dws_trade_shop_refund_1d} ${dws_trade_sku_order_1d} "
;;
esac

hive -e "${sql}"
