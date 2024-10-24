-- 交易域
-- 交易域店铺粒度统计表
drop table if exists ads_trade_shop_stats;
create table ads_trade_shop_stats
(
    dt            string comment '统计日期',
    recent_days   tinyint comment '统计范围：最近n天',
    shop_id       string comment '店铺id',
    shop_name     string comment '店铺名称',
    order_count   bigint comment '下单次数',
    order_users   bigint comment '下单人数',
    order_amount  decimal(19, 2) comment '下单金额',
    actual_amount decimal(19, 2) comment '实收金额',
    refund_amount decimal(19, 2) comment '退款金额'
) comment '交易域店铺粒度统计表'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_trade_shop_stats';


-- 交易域经营模式粒度统计表
drop table if exists ads_trade_type_shop_stats;
create table ads_trade_type_shop_stats
(
    dt            string comment '统计日期',
    recent_days   tinyint comment '统计范围：最近n天',
    type          string comment '店铺类型',
    type_name     string comment '店铺类型名称',
    order_count   bigint comment '下单次数',
    order_users   bigint comment '下单人数',
    order_amount  decimal(19, 2) comment '下单金额',
    actual_amount decimal(19, 2) comment '实收金额',
    refund_amount decimal(19, 2) comment '退款金额'
) comment '交易域经营模式粒度统计表'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_trade_type_shop_stats';

-- 交易域省份粒度统计表
drop table if exists ads_trade_province_stats;
create table ads_trade_province_stats
(
    dt            string comment '统计日期',
    recent_days   tinyint comment '统计范围：最近n天',
    province_id   string comment '省份id',
    province_name string comment '省份名称',
    order_count   bigint comment '下单次数',
    order_users   bigint comment '下单人数',
    order_amount  decimal(19, 2) comment '下单金额',
    actual_amount decimal(19, 2) comment '实收金额',
    refund_amount decimal(19, 2) comment '退款金额'
) comment '交易域经营模式粒度统计表'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_trade_type_shop_stats';


-- 交易域分时段统计表
drop table if exists ads_trade_hour_stats;
create table ads_trade_hour_stats
(
    dt           string comment '统计日期',
    hour         string comment '统计时段;       格式为 yyyy-MM-dd HH
                                       如：2023-06-14 00 对应的区间为
                                       [2023-06-14 00:00:00, 2023-06-14 01:00:00)',
    order_amount decimal(19, 2) comment '下单金额',
    order_count  bigint comment '下单次数',
    order_users  bigint comment '下单人数'
) comment '交易域分时段统计表'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_trade_hour_stats';

-- 交易主题套餐粒度统计表
drop table if exists ads_trade_group_stats;
create table ads_trade_group_stats
(
    dt                 string comment '统计日期',
    recent_days        tinyint comment '统计范围：最近n天',
    product_group_id   string comment '套餐id',
    product_group_name string comment '套餐名称',
    order_amount       decimal(19, 2) comment '下单金额',
    order_users        bigint comment '下单人数'
) comment '交易主题套餐粒度统计表'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_trade_group_stats';


-- 排行菜品减免活动top10
drop table if exists ads_ranking_sku_reduce_top10_stats;
create table ads_ranking_sku_reduce_top10_stats
(
    dt            string comment '统计日期',
    sku_id        string comment '商品id',
    sku_name      string comment '商品名称',
    reduce_amount decimal(19, 2) comment '减免金额',
    rank          tinyint comment '菜品排名'
) comment '排行菜品减免活动top10'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_ranking_sku_reduce_top10_stats';


-- 排行店铺下单金额top10
drop table if exists ads_ranking_shop_order_amount_top10_stats;
create table ads_ranking_shop_order_amount_top10_stats
(
    dt           string comment '统计日期',
    shop_id      string comment '店铺id',
    shop_name    string comment '店铺名称',
    order_amount decimal(19, 2) comment '下单金额',
    rank         tinyint comment '店铺排名'
) comment '排行菜品减免活动top10'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_ranking_shop_order_amount_top10_stats';


-- 活动累计下单金额减免
drop table if exists ads_promotion_reduce_amount_stats;
create table ads_promotion_reduce_amount_stats
(
    dt            string comment '统计日期',
    recent_days   tinyint comment '统计范围：最近n天',
    reduce_amount decimal(19, 2) comment '减免金额'
) comment '活动累计下单金额减免'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_promotion_reduce_amount_stats';


-- 活动累计分摊支出金额
drop table if exists ads_promotion_split_amount_stats;
create table ads_promotion_split_amount_stats
(
    dt                      string comment '统计日期',
    recent_days             tinyint comment '统计范围：最近n天',
    shop_1_share_amount_1d  decimal(19, 2) comment '直营店铺分担优惠金额',
    shop_2_share_amount_1d  decimal(19, 2) comment '加盟店铺分担优惠金额',
    company_share_amount_1d decimal(19, 2) comment '公司分担优惠金额'
) comment '活动累计分摊支出金额'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_promotion_split_amount_stats';

-- 活动分活动统计表
drop table if exists ads_promotion_promotion_stats;
create table ads_promotion_promotion_stats
(
    dt             string comment '统计日期',
    promotion_id   string comment '活动id',
    promotion_name string comment '活动名称',
    recent_days    tinyint comment '统计范围：最近n天',
    reduce_amount  decimal(19, 2) comment '累计减免下单金额',
    order_count    bigint comment '订单数',
    order_users    bigint comment '下单人数'
) comment '活动主题分活动统计表'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_promotion_promotion_stats';


-- 菜品规格主题，时段，交易统计
drop table if exists ads_sku_hour_stats;
create table ads_sku_hour_stats
(
    dt           string comment '统计日期',
    sku_id       string comment 'sku_id',
    sku_name     string comment '菜品规格名称',
    hour         string comment '统计时段;       格式为 yyyy-MM-dd HH
                                       如：2023-06-14 00 对应的区间为
                                       [2023-06-14 00:00:00, 2023-06-14 01:00:00)',
    order_amount decimal(19, 2) comment '下单金额',
    order_count  bigint comment '下单次数',
    order_users  bigint comment '下单人数'
) comment '菜品规格主题，时段，交易统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_sku_hour_stats';

-- ads_group_hour_stats
-- 套餐主题，时段，交易统计
drop table if exists ads_group_hour_stats;
create table ads_group_hour_stats
(
    dt                 string comment '统计日期',
    product_group_id   string comment '套餐id',
    product_group_name string comment '套餐名称',
    hour               string comment '统计时段;       格式为 yyyy-MM-dd HH
                                       如：2023-06-14 00 对应的区间为
                                       [2023-06-14 00:00:00, 2023-06-14 01:00:00)',
    order_amount       decimal(19, 2) comment '下单金额',
    order_count        bigint comment '下单次数',
    order_users        bigint comment '下单人数'
) comment '套餐主题主题，时段，交易统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_group_hour_stats';

-- 评论主题，菜品粒度统计
drop table if exists ads_comment_sku_stats;
create table ads_comment_sku_stats
(
    dt             string comment '统计日期',
    recent_days    tinyint comment '统计时段：最近n天',
    sku_id         string comment 'sku_id',
    sku_name       string comment '菜品规格名称',
    rating_count   bigint comment '评价次数',
    rating_5_count bigint comment '好评次数',
    rating_5_rate  string comment '好评率(%)'
) comment '评论主题，菜品粒度统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_comment_sku_stats';


-- 评论主题, 店铺粒度统计
drop table if exists ads_comment_shop_stats;
create table ads_comment_shop_stats
(
    dt             string comment '统计日期',
    recent_days    tinyint comment '统计时段：最近n天',
    shop_id        string comment '店铺id',
    shop_name      string comment '店铺名称',
    rating_count   bigint comment '评价次数',
    rating_5_count bigint comment '好评次数',
    rating_5_rate  string comment '好评率(%)',
    avg_rating     decimal(19, 2) comment '平均评分'
) comment '评论主题, 店铺粒度统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_comment_shop_stats';


-- 评论主题，套餐粒度统计
drop table if exists ads_comment_group_stats;
create table ads_comment_group_stats
(
    dt                 string comment '统计日期',
    recent_days        tinyint comment '统计时段：最近n天',
    product_group_id   string comment '套餐id',
    product_group_name string comment '套餐名称',
    rating_count       bigint comment '评价次数',
    rating_5_count     bigint comment '好评次数',
    rating_5_rate      string comment '好评率(%)',
    avg_rating         decimal(19, 2) comment '平均评分'
) comment '评论主题, 套餐粒度统计'
    row format delimited fields terminated by '\t'
    location '/warehouse/fast_food/ads/ads_comment_group_stats';



show tables;
 "ads_comment_group_stats" "ads_comment_shop_stats" "ads_comment_sku_stats" "ads_group_hour_stats" "ads_promotion_promotion_stats" "ads_promotion_reduce_amount_stats" "ads_promotion_split_amount_stats" "ads_ranking_shop_order_amount_top10_stats" "ads_ranking_sku_reduce_top10_stats" "ads_sku_hour_stats" "ads_trade_group_stats" "ads_trade_hour_stats" "ads_trade_shop_stats" "ads_trade_type_shop_stats"
