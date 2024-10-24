-- 下单表
-- 粒度：一个订单的一个sku
drop table if exists dwd_trade_order_detail_inc;
create external table dwd_trade_order_detail_inc
(
    order_id         string comment '订单id',
    order_time       string comment '下单时间',
    order_date       string comment '下单日期',
    shop_id          string comment '下单店铺',
    customer_id      string comment '下单用户',
    promotion_id     string comment '关联优惠记录',
    sku_id           string comment 'sku_id',
    product_group_id string comment '套餐id',
    sku_num          bigint comment 'sku数量',
    original_amount  decimal(19, 2) comment '原始价格',
    reduce_amount    decimal(19, 2) comment '优惠金额',
    actual_amount    decimal(19, 2) comment '实际金额'
) comment "交易域下单事务性事实表"
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dwd/dwd_trade_order_detail_inc'
    tblproperties ('orc.compress' = 'snappy');

-- 支付成功表
-- 粒度：订单id+菜品id+顾客

drop table if exists dwd_trade_payment_inc;
create external table dwd_trade_payment_inc
(
    order_id         string comment '订单id',
    pay_time         string comment '支付时间',
    pay_date         string comment '支付日期',
    shop_id          string comment '支付店铺',
    customer_id      string comment '支付用户',
    promotion_id     string comment '关联优惠记录',
    sku_id           string comment 'sku_id',
    product_group_id string comment '套餐id',
    sku_num          bigint comment 'sku数量',
    original_amount  decimal(19, 2) comment '原始价格',
    reduce_amount    decimal(19, 2) comment '优惠金额',
    actual_amount    decimal(19, 2) comment '实际金额'
) comment '交易域支付成功事务事实表'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dwd/dwd_trade_payment_inc'
    tblproperties ('orc.compress' = 'snappy');


-- 退单表
drop table if exists dwd_trade_refund_inc;
create external table dwd_trade_refund_inc
(
    order_id         string comment '订单id',
    refund_time      string comment '退单时间',
    refund_date      string comment '退单日期',
    shop_id          string comment '退单店铺',
    customer_id      string comment '退单用户',
    promotion_id     string comment '关联优惠记录',
    sku_id           string comment 'sku_id',
    product_group_id string comment '套餐id',
    sku_num          bigint comment 'sku数量',
    original_amount  decimal(19, 2) comment '原始价格',
    reduce_amount    decimal(19, 2) comment '优惠金额',
    actual_amount    decimal(19, 2) comment '实际金额'
) comment '交易域退单事务事实表'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dwd/dwd_trade_refund_inc'
    tblproperties ('orc.compress' = 'snappy');

-- 退单成功表
drop table if exists dwd_trade_refund_suc_inc;
create external table dwd_trade_refund_suc_inc
(
    order_id         string comment '订单id',
    refund_suc_time  string comment '退款时间',
    refund_suc_date  string comment '退款日期',
    shop_id          string comment '退款店铺',
    customer_id      string comment '退款用户',
    promotion_id     string comment '关联优惠记录',
    sku_id           string comment 'sku_id',
    product_group_id string comment '套餐id',
    sku_num          bigint comment 'sku数量',
    original_amount  decimal(19, 2) comment '原始价格',
    reduce_amount    decimal(19, 2) comment '优惠金额',
    actual_amount    decimal(19, 2) comment '实际金额'
) comment '交易域退单事务事实表'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dwd/dwd_trade_refund_suc_inc'
    tblproperties ('orc.compress' = 'snappy');


-- 互动域评价表
-- 粒度： 订单 + 顾客 + sku
drop table if exists dwd_interaction_comment_inc;
create external table dwd_interaction_comment_inc
(
    order_id         string comment '订单id',
    comment_time     string comment '评价时间',
    comment_date     string comment '评价日期',
    `comment`        string comment '评价内容',
    rating           bigint comment '评分',
    shop_id          string comment '评价店铺',
    customer_id      string comment '评价用户',
    promotion_id     string comment '关联优惠记录',
    sku_id           string comment 'sku_id',
    product_group_id string comment '套餐id',
    sku_num          bigint comment 'sku数量',
    original_amount  decimal(19, 2) comment '原始价格',
    reduce_amount    decimal(19, 2) comment '优惠金额',
    actual_amount    decimal(19, 2) comment '实际金额'
) comment '交易域退单事务事实表'
    partitioned by (dt string)
    stored as orc
    location '/warehouse/fast_food/dwd/dwd_interaction_comment_inc'
    tblproperties ('orc.compress' = 'snappy');


show tables;
