-- 一、数据仓库构建
--1：创建数据库 
    -- 1.1 创建ods库
    create database if not exists ods_didi;
    -- 1.2 创建dw库
    create database if not exists dw_didi;
    -- 1.3 创建app库
    create database if not exists app_didi;
--2：创建表
-- 2.1 创建订单表结构
    create table if not exists ods_didi.t_user_order(
        orderId string comment '订单id',
        telephone string comment '打车用户手机',
        lng string comment '用户发起打车的经度',
        lat string comment '用户发起打车的纬度',
        province string comment '所在省份',
        city string comment '所在城市',
        es_money double comment '预估打车费用',
        gender string comment '用户信息 - 性别',
        profession string comment '用户信息 - 行业',
        age_range string comment '年龄段（70后、80后、...）',
        tip double comment '小费',
        subscribe integer comment '是否预约（0 - 非预约、1 - 预约）',
        sub_time string comment '预约时间',
        is_agent integer comment '是否代叫（0 - 本人、1 - 代叫）',
        agent_telephone string comment '预约人手机',
        order_time string comment '预约时间'
    )
    partitioned by (dt string comment '时间分区') 
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ; 
--2.2 创建取消订单表
create table if not exists ods_didi.t_user_cancel_order(
    orderId string comment '订单ID',
    cstm_telephone string comment '客户联系电话',
    lng string comment '取消订单的经度',
    lat string comment '取消订单的纬度',
    province string comment '所在省份',
    city string comment '所在城市',
    es_distance double comment '预估距离',
    gender string comment '性别',
    profession string comment '行业',
    age_range string comment '年龄段',
    reason integer comment '取消订单原因（1 - 选择了其他交通方式、2 - 与司机达成一致，取消订单、3 - 投诉司机没来接我、4 - 已不需要用车、5 - 无理由取消订单）',
    cancel_time string comment '取消时间'
)
partitioned by (dt string comment '时间分区') 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ; 
--2.3 创建订单支付表
create table if not exists ods_didi.t_user_pay_order(
    id string comment '支付订单ID',
    orderId string comment '订单ID',
    lng string comment '目的地的经度（支付地址）',
    lat string comment '目的地的纬度（支付地址）',
    province string comment '省份',
    city string comment '城市',
    total_money double comment '车费总价',
    real_pay_money double comment '实际支付总额',
    passenger_additional_money double comment '乘客额外加价',
    base_money double comment '车费合计',
    has_coupon integer comment '是否使用优惠券（0 - 不使用、1 - 使用）',
    coupon_total double comment '优惠券合计',
    pay_way integer comment '支付方式（0 - 微信支付、1 - 支付宝支付、3 - QQ钱包支付、4 - 一网通银行卡支付）',
    mileage double comment '里程（单位公里）',
    pay_time string comment '支付时间'
)
partitioned by (dt string comment '时间分区') 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ; 

--2.4创建用户评价表
create table if not exists ods_didi.t_user_evaluate(
    id string comment '评价日志唯一ID',
    orderId string comment '订单ID',
    passenger_telephone string comment '用户电话',
    passenger_province string comment '用户所在省份',
    passenger_city string comment '用户所在城市',
    eva_level integer comment '评价等级（1 - 一颗星、... 5 - 五星）',
    eva_time string comment '评价时间'
)
partitioned by (dt string comment '时间分区') 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ; 


--3:给表加载数据
--3.1、创建本地路径，上传源日志文件
mkdir -p /export/data/didi

--3.2、通过load命令给表加载数据，并指定分区
load data local inpath '/export/data/didi/order.csv' into table t_user_order partition (dt='2020-04-12');
load data local inpath '/export/data/didi/cancel_order.csv' into table t_user_cancel_order partition (dt='2020-04-12');
load data local inpath '/export/data/didi/pay.csv' into table t_user_pay_order partition (dt='2020-04-12');
load data local inpath '/export/data/didi/evaluate.csv' into table t_user_evaluate partition (dt='2020-04-12');


--4:数据预处理
--建表
create table if not exists dw_didi.t_user_order_wide(
    orderId string comment '订单id',
    telephone string comment '打车用户手机',
    lng string comment '用户发起打车的经度',
    lat string comment '用户发起打车的纬度',
    province string comment '所在省份',
    city string comment '所在城市',
    es_money double comment '预估打车费用',
    gender string comment '用户信息 - 性别',
    profession string comment '用户信息 - 行业',
    age_range string comment '年龄段（70后、80后、...）',
    tip double comment '小费',
    subscribe integer comment '是否预约（0 - 非预约、1 - 预约）',
    subscribe_name string comment '是否预约名称',
    sub_time string comment '预约时间',
    is_agent integer comment '是否代叫（0 - 本人、1 - 代叫）',
    is_agent_name string comment '是否代叫名称',
    agent_telephone string comment '预约人手机',
    order_date string comment '预约时间，yyyy-MM-dd',
    order_year string comment '年',
    order_month string comment '月',
    order_day string comment '日',
    order_hour string comment '小时',
    order_time_range string comment '时间段',
    order_time string comment '预约时间'
)
partitioned by (dt string comment '时间分区') 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ; 

--转宽表HQL语句
select 
    orderId,
    telephone,
    lng,
    lat,
    province,
    city,
    es_money,
    gender,
    profession,
    age_range,
    tip,
    subscribe,
    case when subscribe = 0 then '非预约'
         when subscribe = 1 then'预约'
    end as subscribe_name,
    sub_time,
    is_agent,
    case when is_agent = 0 then '本人'
         when is_agent = 1 then '代叫'
    end as is_agent_name,
    agent_telephone,
    date_format(order_time, 'yyyy-MM-dd') as order_date,
    year(date_format(order_time, 'yyyy-MM-dd')) as order_year,
    month(date_format(order_time, 'yyyy-MM-dd')) as order_month,
    day(date_format(order_time, 'yyyy-MM-dd')) as order_day,
    hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) as order_hour,
    case when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 1 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 5 then '凌晨'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 5 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 8 then '早上'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 8 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 11 then '上午'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 11 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 13 then '中午'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 13 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 17 then '下午'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 17 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 19 then '晚上'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 19 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 20 then '半夜'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 20 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 24 then '深夜'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) >= 0 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 1 then '深夜'
         else 'N/A'
    end as order_time_range,
    date_format(order_time, 'yyyy-MM-dd HH:mm') as order_time
from ods_didi.t_user_order where dt = '2020-04-12' and length(order_time) > 8 ;

--7.3 	将数据加载到dw层宽表
insert overwrite table dw_didi.t_user_order_wide partition(dt='2020-04-12')
select 
    orderId,
    telephone,
    lng,
    lat,
    province,
    city,
    es_money,
    gender,
    profession,
    age_range,
    tip,
    subscribe,
    case when subscribe = 0 then '非预约'
         when subscribe = 1 then'预约'
    end as subscribe_name,
    sub_time,
    is_agent,
    case when is_agent = 0 then '本人'
         when is_agent = 1 then '代叫'
    end as is_agent_name,
    agent_telephone,
    date_format(order_time, 'yyyy-MM-dd') as order_date,
    year(date_format(order_time, 'yyyy-MM-dd')) as order_year,
    month(date_format(order_time, 'yyyy-MM-dd')) as order_month,
    day(date_format(order_time, 'yyyy-MM-dd')) as order_day,
    hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) as order_hour,
    case when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 1 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 5 then '凌晨'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 5 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 8 then '早上'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 8 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 11 then '上午'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 11 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 13 then '中午'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 13 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 17 then '下午'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 17 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 19 then '晚上'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 19 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 20 then '半夜'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) > 20 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 24 then '深夜'
         when hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) >= 0 and hour(date_format(order_time, 'yyyy-MM-dd HH:mm')) <= 1 then '深夜'
         else 'N/A'
    end as order_time_range,
    date_format(order_time, 'yyyy-MM-dd HH:mm') as order_time
from ods_didi.t_user_order where dt = '2020-04-12' and length(order_time) > 8
;


--5:指标分析
---------------------总订单笔数----------------------------
-- 1. 计算4月12日总订单笔数
select 
    count(orderid) as total_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
;

--2：APP层建表
-- 创建保存日期对应订单笔数的app表
create table if not exists app_didi.t_order_total(
    date string comment '日期（年月日)',
    count integer comment '订单笔数'
)
partitioned by (month string comment '年月，yyyy-MM')
row format delimited fields terminated by ','
;

--3：加载数据到app表
insert overwrite table app_didi.t_order_total partition(month='2020-04')
select 
    '2020-04-12',
    count(orderid) as total_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
;
---------------------预约和非预约用户占比----------------------------
-- 1. 计算4月12日总订单笔数
select
    subscribe_name,
    count(*) as order_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
group by
    subscribe_name
;

--2：APP层建表
-- 创建保存日期对应订单笔数的app表
create table if not exists app_didi.t_order_subscribe_total(
    date string comment '日期',
    subscribe_name string comment '是否预约',
    count integer comment '订单数量'
)
partitioned by (month string comment '年月，yyyy-MM')
row format delimited fields terminated by ','
;

--3：加载数据到app表
insert overwrite table app_didi.t_order_subscribe_total partition(month = '2020-04')
select
    '2020-04-12',
    subscribe_name,
    count(*) as order_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
group by
    subscribe_name
;

---------------------不同时段的占比分析----------------------------
--1、编写HQL语句
select
    order_time_range,
    count(*) as order_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
group by
    order_time_range
--2、创建APP层表

create table if not exists app_didi.t_order_timerange_total(
    date string comment '日期',
    timerange string comment '时间段',
    count integer comment '订单数量'
)
partitioned by (month string comment '年月，yyyy-MM')
row format delimited fields terminated by ','
;

--3、加载数据到APP表
insert overwrite table app_didi.t_order_timerange_total partition(month = '2020-04')
select
    '2020-04-12',
    order_time_range,
    count(*) as order_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
group by
    order_time_range
;

--------不同地域订单占比--------------------
--1、编写HQL
select
    province,
    count(*) as order_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
group by
    province
;

--2、创建APP表
create table if not exists app_didi.t_order_province_total(
    date string comment '日期',
    province string comment '省份',
    count integer comment '订单数量'
)
partitioned by (month string comment '年月，yyyy-MM')
row format delimited fields terminated by ','
;

--3、数据加载到APP表
insert overwrite table app_didi.t_order_province_total partition(month = '2020-04')
select
    '2020-04-12',
    province,
    count(*) as order_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
group by
    province
order by order_cnt desc
;


--------不同年龄段，不同时段订单占比--------------------
--1、编写HQL
select
      age_range,
     order_time_range,
    count(*) as order_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
group by
    age_range,
    order_time_range
;

--2、创建APP表
create table if not exists app_didi.t_order_age_and_time_range_total(
    date string comment '日期',
    age_range string comment '年龄段',
    order_time_range string comment '时段',
    count integer comment '订单数量'
)
partitioned by (month string comment '年月，yyyy-MM')
row format delimited fields terminated by ','
;

--3、加载数据到APP表
insert overwrite table app_didi.t_order_age_and_time_range_total partition(month = '2020-04')
select
      '2020-04-12',
      age_range,
     order_time_range,
    count(*) as order_cnt
from
    dw_didi.t_user_order_wide
where
    dt = '2020-04-12'
group by
    age_range,
    order_time_range
;


--6:Sqoop安装 
-- 准备工作

#进入Sqoop安装目录
cd /export/server/sqoop-1.4.7
#验证sqoop是否工作
bin/sqoop list-databases \
--connect jdbc:mysql://192.168.88.100:3306/ \
--username root \
--password 123456 

--mysql创建目标数据库和目标表
    #创建目标数据库
    create database if not exists app_didi;
     
    #创建订单总笔数目标表
    create table if not exists app_didi.t_order_total(
        order_date date,
        count int
    );

    #创建预约订单/非预约订单统计目标表
    create table if  not exists app_didi.t_order_subscribe_total(
        order_date date ,
        subscribe_name varchar(20) ,
        count int
    );



    #创建不同时段订单统计目标表
    create table if not exists app_didi.t_order_timerange_total(
        order_date date ,
        timerange varchar(20) ,
        count int 
    );

    #创建不同地域订单统计目标表
    create table if not exists app_didi.t_order_province_total(
     order_date date ,
     province varchar(20) ,
     count int 
   );

   #创建不同年龄段，不同时段订单目标表
   create table if not exists app_didi.t_order_age_and_time_range_total(
    order_date date ,
    age_range varchar(20) ,
    order_time_range varchar(20) ,
    count int 
 );
