-- 店铺粒度下单统计表_1d

drop table if exists dws_trade_shop_order_1d;
create table dws_trade_shop_order_1d
(
    shop_id                   string comment '商铺id',
    shop_name                 string comment '商铺联系人',
    type                      string comment '商铺类型',
    type_name                 string comment '商铺类型名称',
    region_id                 string comment '地区id',
    region_name               string comment '地区名称',
    province_id               string comment '省份id',
    province_name             string comment '省份名称',
    order_count_1d            bigint comment '下单次数',
    order_users_1d            bigint comment '下单人数',
    order_amount_1d           decimal(19, 2) comment '下单金额',
    order_promotion_count_1d  bigint comment '参与活动订单数',
    order_promotion_amount_1d decimal(19, 2) comment '优惠金额'
) comment '店铺粒度下单统计表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_shop_order_1d'
    tblproperties ('orc.compress' = 'snappy');

-- 店铺粒度支付统计表_1d
drop table if exists dws_trade_shop_payment_1d;
create table dws_trade_shop_payment_1d
(
    shop_id                 string comment '商铺id',
    shop_name               string comment '商铺联系人',
    type                    string comment '商铺类型',
    type_name               string comment '商铺类型名称',
    region_id               string comment '地区id',
    region_name             string comment '地区名称',
    province_id             string comment '省份id',
    province_name           string comment '省份名称',
    actual_amount_1d        decimal(19, 2) comment '实收金额',
    shop_share_amount_1d    decimal(19, 2) comment '店铺分担优惠金额',
    company_share_amount_1d decimal(19, 2) comment '公司分担优惠金额'
) comment '店铺粒度支付统计表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_shop_payment_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 店铺粒度支付统计表_1d
drop table if exists dws_trade_shop_refund_1d;
create table dws_trade_shop_refund_1d
(
    shop_id          string comment '商铺id',
    shop_name        string comment '商铺联系人',
    type             string comment '商铺类型',
    type_name        string comment '商铺类型名称',
    region_id        string comment '地区id',
    region_name      string comment '地区名称',
    province_id      string comment '省份id',
    province_name    string comment '省份名称',
    refund_amount_1d decimal(19, 2) comment '退款金额'
) comment '店铺退款统计表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_shop_refund_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 互动域店铺粒度评价统计表_1d
drop table if exists dws_interaction_shop_rating_1d;
create table dws_interaction_shop_rating_1d
(
    shop_id           string comment '商铺id',
    shop_name         string comment '商铺联系人',
    type              string comment '商铺类型',
    type_name         string comment '商铺类型名称',
    region_id         string comment '地区id',
    region_name       string comment '地区名称',
    province_id       string comment '省份id',
    province_name     string comment '省份名称',
    avg_rating_1d     decimal(19, 2) comment '平均分',
    rating_count_1d   bigint comment '评价次数',
    rating_5_count_1d bigint comment '好评次数'
) comment '互动域店铺粒度评价统计表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_interaction_shop_rating_1d'
    tblproperties ('orc.compress' = 'snappy');



-- 交易域菜品规格粒度下单统计表_1d
drop table if exists dws_trade_sku_order_1d;
create table dws_trade_sku_order_1d
(
    sku_id                       string comment 'sku_id',
    sku_name                     string comment '菜品规格名称',
    sku_price                    decimal(19, 2) comment 'sku价格',
    spu_id                       string comment '菜品id',
    spu_name                     string comment '菜品名称',
    spu_description              string comment '菜品描述',
    product_category_id          string comment '分类id',
    product_category_name        string comment '分类名称',
    product_category_description string comment '分类描述',
    order_count_1d               bigint comment '下单次数',
    order_users_1d               bigint comment '下单人数',
    order_amount_1d              decimal(19, 2) comment '下单金额',
    order_promotion_count_1d     bigint comment '参与活动sku数',
    order_promotion_amount_1d    decimal(19, 2) comment '优惠金额'
) comment '交易域菜品规格粒度下单统计表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_sku_order_1d'
    tblproperties ('orc.compress' = 'snappy');

-- 互动域菜品规格粒度评分表_1d
drop table if exists dws_interaction_sku_rating_1d;
create table dws_interaction_sku_rating_1d
(
    sku_id                       string comment 'sku_id',
    sku_name                     string comment '菜品规格名称',
    sku_price                    decimal(19, 2) comment 'sku价格',
    spu_id                       string comment '菜品id',
    spu_name                     string comment '菜品名称',
    spu_description              string comment '菜品描述',
    product_category_id          string comment '分类id',
    product_category_name        string comment '分类名称',
    product_category_description string comment '分类描述',
    rating_count_1d              bigint comment '评价次数',
    rating_5_count_1d            bigint comment '好评次数'
) comment '互动域菜品规格粒度评分表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_interaction_sku_rating_1d'
    tblproperties ('orc.compress' = 'snappy');

-- 交易域活动粒度下单表_1d
drop table if exists dws_trade_promotion_order_1d;
create table dws_trade_promotion_order_1d
(
    promotion_id              string comment '活动id',
    name                      string comment '活动名称',
    company_share             decimal(19, 2) comment '公司承担比例',
    reduce_amount             decimal(19, 2) comment '满减金额',
    threshold_amount          decimal(19, 2) comment '满减门槛',
    order_count_1d            bigint comment '订单数',
    order_promotion_amount_1d decimal(19, 2) comment '优惠金额',
    order_user_count_1d       bigint comment '下单人数'
) comment '交易域活动粒度下单表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_promotion_order_1d'
    tblproperties ('orc.compress' = 'snappy');



-- 交易域套餐粒度下单表_1d

drop table if exists dws_trade_product_group_order_1d;
create table dws_trade_product_group_order_1d
(
    product_group_id       string comment '套餐id',
    name                   string comment '套餐名称',
    original_price         decimal(19, 2) comment '原始金额',
    price                  decimal(19, 2) comment '实际价格',
    sku_group              array<string> comment '套餐包含的sku',
    order_amount_1d        decimal(19, 2) comment '下单金额',
    order_reduce_amount_1d decimal(19, 2) comment '优惠金额',
    order_user_count_1d    bigint comment '下单人数'
) comment '交易域套餐粒度下单表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_product_group_order_1d'
    tblproperties ('orc.compress' = 'snappy');



-- 互动域套餐粒度评价表_1d
drop table if exists dws_interaction_product_group_rating_1d;
create table dws_interaction_product_group_rating_1d
(
    product_group_id  string comment '套餐id',
    name              string comment '套餐名称',
    original_price    decimal(19, 2) comment '原始金额',
    price             decimal(19, 2) comment '实际价格',
    sku_group         array<string> comment '套餐包含的sku',
    rating_count_1d   bigint comment '评价次数',
    rating_5_count_1d bigint comment '好评次数',
    avg_rating_1d     decimal(19, 2) comment '平均评分',
    rating_5_rate_1d  string comment '好评率(%)'
) comment '互动域套餐粒度评价表_1d'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_interaction_product_group_rating_1d'
    tblproperties ('orc.compress' = 'snappy');


-- 交易域店铺粒度下单表_nd
drop table if exists dws_trade_shop_order_nd;
create table dws_trade_shop_order_nd
(
    shop_id                   string comment '商铺id',
    shop_name                 string comment '商铺联系人',
    type                      string comment '商铺类型',
    type_name                 string comment '商铺类型名称',
    region_id                 string comment '地区id',
    region_name               string comment '地区名称',
    province_id               string comment '省份id',
    province_name             string comment '省份名称',
    order_count_nd            bigint comment '下单次数',
    order_users_nd            bigint comment '下单人数',
    order_amount_nd           decimal(19, 2) comment '下单金额',
    order_promotion_count_nd  bigint comment '参与活动订单数',
    order_promotion_amount_nd decimal(19, 2) comment '优惠金额',
    recent_days              string comment '统计时间'
) comment '交易域店铺粒度下单表_nd'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_shop_order_nd'
    tblproperties ('orc.compress' = 'snappy');

-- 互动域店铺粒度评价表_nd

drop table if exists dws_interaction_shop_rating_nd;
create table dws_interaction_shop_rating_nd
(
    shop_id           string comment '商铺id',
    shop_name         string comment '商铺联系人',
    type              string comment '商铺类型',
    type_name         string comment '商铺类型名称',
    region_id         string comment '地区id',
    region_name       string comment '地区名称',
    province_id       string comment '省份id',
    province_name     string comment '省份名称',
    avg_rating_nd     decimal(19, 2) comment '平均分',
    rating_count_nd   bigint comment '评价次数',
    rating_5_count_nd bigint comment '好评次数',
    recent_days         string comment '统计时间'
) comment '互动域店铺粒度评价表_nd'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_interaction_shop_rating_nd'
    tblproperties ('orc.compress' = 'snappy');


-- 交易域菜品规格粒度下单统计表_nd
drop table if exists dws_trade_sku_order_nd;
create table dws_trade_sku_order_nd
(
    sku_id                       string comment 'sku_id',
    sku_name                     string comment '菜品规格名称',
    sku_price                    decimal(19, 2) comment 'sku价格',
    spu_id                       string comment '菜品id',
    spu_name                     string comment '菜品名称',
    spu_description              string comment '菜品描述',
    product_category_id          string comment '分类id',
    product_category_name        string comment '分类名称',
    product_category_description string comment '分类描述',
    order_count_nd               bigint comment '下单次数',
    order_users_nd               bigint comment '下单人数',
    order_amount_nd              decimal(19, 2) comment '下单金额',
    order_promotion_count_nd     bigint comment '参与活动sku数',
    order_promotion_amount_nd    decimal(19, 2) comment '优惠金额',
    recent_days                  string comment '统计日期'
) comment '交易域菜品规格粒度下单统计表_nd'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_sku_order_nd'
    tblproperties ('orc.compress' = 'snappy');




-- 互动域菜品规格粒度评分表_nd
drop table if exists dws_interaction_sku_rating_nd;
create table dws_interaction_sku_rating_nd
(
    sku_id                       string comment 'sku_id',
    sku_name                     string comment '菜品规格名称',
    sku_price                    decimal(19, 2) comment 'sku价格',
    spu_id                       string comment '菜品id',
    spu_name                     string comment '菜品名称',
    spu_description              string comment '菜品描述',
    product_category_id          string comment '分类id',
    product_category_name        string comment '分类名称',
    product_category_description string comment '分类描述',
    rating_count_nd              bigint comment '评价次数',
    rating_5_count_nd            bigint comment '好评次数',
    recent_days                  string comment '统计日期'
) comment '互动域菜品规格粒度评分表_nd'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_interaction_sku_rating_nd'
    tblproperties ('orc.compress' = 'snappy');

-- 交易域活动粒度下单表_nd

drop table if exists dws_trade_promotion_order_nd;
create table dws_trade_promotion_order_nd
(
    promotion_id              string comment '活动id',
    name                      string comment '活动名称',
    company_share             decimal(19, 2) comment '公司承担比例',
    reduce_amount             decimal(19, 2) comment '满减金额',
    threshold_amount          decimal(19, 2) comment '满减门槛',
    order_count_nd            bigint comment '订单数',
    order_promotion_amount_nd decimal(19, 2) comment '优惠金额',
    order_user_count_nd       bigint comment '下单人数',
    recent_days string comment '统计日期'
) comment '交易域活动粒度下单表_nd'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_promotion_order_1d'
    tblproperties ('orc.compress' = 'snappy');

-- 交易域套餐粒度下单表_nd
drop table if exists dws_trade_product_group_order_nd;
create table dws_trade_product_group_order_nd
(
    product_group_id       string comment '套餐id',
    name                   string comment '套餐名称',
    original_price         decimal(19, 2) comment '原始金额',
    price                  decimal(19, 2) comment '实际价格',
    sku_group              array<string> comment '套餐包含的sku',
    order_amount_nd        decimal(19, 2) comment '下单金额',
    order_reduce_amount_nd decimal(19, 2) comment '优惠金额',
    order_user_count_nd    bigint comment '下单人数',
    recent_days            string comment '统计日期'
) comment '交易域套餐粒度下单表_nd'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_trade_product_group_order_nd'
    tblproperties ('orc.compress' = 'snappy');

-- 互动域套餐粒度评价表_nd
drop table if exists dws_interaction_product_group_rating_nd;
create table dws_interaction_product_group_rating_nd
(
    product_group_id  string comment '套餐id',
    name              string comment '套餐名称',
    original_price    decimal(19, 2) comment '原始金额',
    price             decimal(19, 2) comment '实际价格',
    sku_group         array<string> comment '套餐包含的sku',
    rating_count_nd   bigint comment '评价次数',
    rating_5_count_nd bigint comment '好评次数',
    avg_rating_nd     decimal(19, 2) comment '平均评分',
    rating_5_rate_nd  string comment '好评率(%)',
    recent_days         string comment '统计日期'
) comment '互动域套餐粒度评价表_nd'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dws/dws_interaction_product_group_rating_nd'
    tblproperties ('orc.compress' = 'snappy');




