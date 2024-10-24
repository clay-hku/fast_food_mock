#! /bin/bash

# 首日装载
db="fast_food"
# 判断是否传入日期参数
if [ -n "$2" ]
then 
	do_date=$2
else
	do_date=$(date -d "-1 days" %F)
fi

sql="use ${db};"

dim_customer_zip="set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dim_customer_zip partition(dt)
select
    id,
    phone_number,
    username,
    start_date,
    if(rn=1, end_date, date_sub('${do_date}', 1)) end_date,
    if(rn=1, end_date, date_sub('${do_date}', 1))
from
    (
    select
        id,
        phone_number,
        username,
        start_date,
        end_date,
        row_number() over (partition by id order by start_date desc) rn
    from
        (
        select
            id,
            phone_number,
            username,
            start_date,
            end_date
        from dim_customer_zip
        where dt = '9999-12-31'
        union all
        select
            id,
            phone_number,
            username,
            '${do_date}' start_date,
            '9999-12-31' end_date
        from (
            select
                data.id,
                data.phone_number,
                data.username,
                row_number() over (partition by data.id order by ts desc) rn
            from ods_customer_inc
            where dt = '${do_date}'
            and type in ('insert', 'update')
             ) t1
        where rn = 1
        ) t2
    ) t3;"


dim_shop_full="insert overwrite table dim_shop_full partition(dt='${do_date}')
select
    id,
    name,
    phone_number,
    type,
    case type when 1 then '直营' when 2 then '加盟' end type_name,
    region_id
from ods_shop_full
where dt = '${do_date}';"



dim_region_full="insert overwrite table dim_region_full partition(dt='${do_date}')
select
    id,
    level,
    case level when 1 then '省级' when 2 then '地级市' end level_name,
    name,
    superior_region,
    zip_code
from ods_region_full
where dt='${do_date}';"

dim_promotion_full="insert overwrite table dim_promotion_full partition (dt='${do_date}')
select
    id,
    company_share,
    name,
    reduce_amount,
    threshold_amount
from ods_promotion_full
where dt = '${do_date}';"

dim_product_full="with attr_n as (
    select
        id spu_attr_id,
        attr_name spu_attr_name,
        product_spu_id spu_id
    from ods_product_spu_attr_full
    where dt = '${do_date}'
),
    attr_v as (
    select
        spu_attr_id,
        collect_list(named_struct('spu_attr_value_id', spu_attr_value_id, 'spu_attr_value_name', spu_attr_value_name)) attr_value
    from
(    select
        id spu_attr_value_id,
        product_spu_attr_id spu_attr_id,
        attr_value spu_attr_value_name
    from ods_product_spu_attr_value_full
    where dt = '${do_date}') tv
    group by spu_attr_id
    ),
    attr as (
    select
        spu_id,
        collect_list(named_struct('spu_attr_id',attr_n.spu_attr_id,'spu_attr_name',attr_n.spu_attr_name
              ,'spu_attr_value',attr_value)) spu_attr
    from attr_n left join attr_v
    on attr_n.spu_attr_id = attr_v.spu_attr_id
    group by spu_id
    )

insert overwrite table dim_product_full partition(dt='${do_date}')
select
    sku.sku_id,
    sku_name,
    sku_price,
    cat.product_category_id,
    product_category_name,
    product_category_description,
    sku.spu_id,
    spu_name,
    spu_description,
    spu_attr
from
    (select
        sk.id sku_id,
        sk.name sku_name,
        sk.price sku_price,
        sk.product_category_id,
        sk.product_spu_id spu_id
    from ods_product_sku_full sk
    where dt = '${do_date}') sku
left join (
    select
        id spu_id,
        description spu_description,
        name spu_name
    from ods_product_spu_full
    where dt = '${do_date}'
    ) spu
on spu.spu_id = sku.spu_id
left join (
    select
        id product_category_id,
        description product_category_description,
        name product_category_name
    from ods_product_category_full
    where dt = '${do_date}'
    ) cat
on cat.product_category_id = sku.product_category_id
left join attr
on attr.spu_id = sku.spu_id;"

dim_product_group_full="insert overwrite table dim_product_group_full partition(dt = '${do_date}')
select
    id             ,
    name           ,
    original_price ,
    price          ,
    sku_group
from
(
    select
        product_group_id,
        collect_list(product_sku_id) sku_group
    from ods_product_group_sku_full
    where dt = '${do_date}'
    group by product_group_id
) t1
left join
(
    select
        id            ,
        name          ,
        original_price,
        price
    from ods_product_group_full
    where dt = '${do_date}'
) t2
on t2.id = t1.product_group_id;
"


case $1 in 
"dim_customer_zip" | "dim_product_full" | "dim_product_group_full" | "dim_promotion_full" | "dim_region_full" | "dim_shop_full")
echo "hive：首日装载数据${!1}..."
hive -e "${sql}${!1}"
echo "装载成功!"
;;
"all")
echo "hive:首日装载数据all..."
hive -e "${sql}${dim_customer_zip}${dim_product_full}${dim_product_group_full}${dim_promotion_full}${dim_region_full}${dim_shop_full}"
echo "装载成功"
;;
esac

