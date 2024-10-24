-- 顾客表（拉链）

drop table if exists dim_customer_zip;
create external table dim_customer_zip
(
    id string comment '顾客id',
    phone_number string comment '顾客电话',
    username string comment '用户名',
    start_date string comment '起始日期',
    end_date string comment '结束日期'
)   comment '顾客拉链表'
partitioned by (dt string)
stored as orc
location '/warehouse/fast_food/dim/dim_customer_zip'
tblproperties ('orc.compress=snappy');


-- 店铺维度表
drop table if exists dim_shop_full;
create external table dim_shop_full
(
    id string comment '商铺id',
    name string comment '商铺名称',
    phone_number string comment '商铺电话',
    type string comment '商铺类型',
    type_name string comment '商铺类型名称',
    region_id string comment '地区id'
)
partitioned by (dt string)
stored as orc
location '/warehouse/fast_food/dim/dim_shop_full'
tblproperties ('orc.compress' = 'snappy');

-- 地区维度表
drop table if exists dim_region_full;
create external table dim_region_full
(
    id string comment '地区id',
    level string comment '行政级别：1省级，2地级市',
    level_name string comment '行政级别名称',
    name string comment '区划名称',
    superior_region string comment '上级区划',
    zip_code string comment '邮编'
)
partitioned by (dt string)
stored as orc
location '/warehouse/fast_food/dim/dim_region_full'
tblproperties ('orc.compress'='snappy');

insert overwrite table dim_region_full partition(dt='2023-06-14')
select
    id,
    level,
    case level when 1 then "省级" when 2 then '地级市' end level_name,
    name,
    superior_region,
    zip_code
from ods_region_full
where dt='2023-06-14';



-- 优惠活动维度表
drop table if exists dim_promotion_full;
create external table dim_promotion_full
(
    id string comment '活动id',
    company_share decimal(19,2) comment '公司负担比例',
    name string comment '活动名称',
    reduce_amount decimal(19,2) comment '满减金额',
    threshold_amount decimal(19,2) comment '满减门槛'
)
partitioned by (dt string)
stored as orc
location '/warehouse/fast_food/dim/dim_promotion_full'
tblproperties ('orc.compress'='snappy');


-- 菜品维度表
drop table if exists dim_product_full;
create external table dim_product_full
(
    sku_id                       string comment 'sku_id',
    sku_name                     string comment '菜品规格名称',
    sku_price                    decimal(19, 2) comment '价格',
    product_category_id          string comment '菜品分类id',
    product_category_name        string comment '菜品分类名称',
    product_category_description string comment '菜品分类描述',
    spu_id                       string comment 'spu_id',
    spu_name                     string comment '菜品名称',
    spu_description              string comment '菜品描述',
    spu_attr                     array<struct<spu_attr_id :string, spu_attr_name :string, spu_attr_value
                                              :array<struct<spu_attr_value_id :string, spu_attr_value_name :string>>>>
)
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dim/dim_product_full'
    tblproperties ('orc.compress' = 'snappy');


-- 套餐维度表
drop table if exists dim_product_group_full;
create external table dim_product_group_full
(
    id             string comment '套餐id',
    name           string comment '套餐名称',
    original_price decimal(19, 2) comment '原始价格',
    price          decimal(19, 2) comment '价格',
    sku_group      array<string> comment '包含的skuid'
) comment '套餐维度表'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dim/dim_product_group_full'
    tblproperties ('orc.compress' = 'snappy');







