drop table if exists dwm_dim_shop_region_full;
create table dwm_dim_shop_region_full
(
    shop_id       string comment '商铺id',
    shop_name     string comment '商铺联系人',
    type          string comment '商铺类型',
    type_name     string comment '商铺类型名称',
    region_id     string comment '地区id',
    region_name   string comment '地区名称',
    province_id   string comment '省份id',
    province_name string comment '省份名称'
) comment '商铺基本信息表'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dwm/dwm_dim_shop_region_full'
    tblproperties ('orc.compress' = 'snappy');


with
    dim_shop     as (
                        select
                            id   shop_id,
                            name shop_name,
                            type,
                            type_name,
                            region_id
                        from dim_shop_full
                        where dt = '2023-06-14'
                    ),
    dim_region   as (
                        select
                            id   region_id,
                            name region_name,
                            superior_region
                        from dim_region_full
                        where dt = '2023-06-14'
                          and level = '2'
                    ),
    dim_province as (
                        select
                            id   province_id,
                            name province_name
                        from dim_region_full
                        where dt = '2023-06-14'
                          and level = '1'
                    ),
    dim_p        as (
                        select
                            shop_id,
                            shop_name,
                            type,
                            type_name,
                            dim_shop.region_id,
                            region_name,
                            province_id,
                            province_name
                        from dim_shop
                                 left join dim_region on dim_shop.region_id = dim_region.region_id
                                 left join dim_province on dim_province.province_id = dim_region.superior_region
                    )
-- 每日装载
insert overwrite table dwm_dim_shop_region_full partition (dt = '2023-06-14')
select
    shop_id,
    shop_name,
    type,
    type_name,
    region_id,
    region_name,
    province_id,
    province_name
from dim_p;

select * from dws_trade_shop_order_1d