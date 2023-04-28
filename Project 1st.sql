use olist_dataset_project;

SHOW variables LIKE "secure_file_priv";

CREATE TABLE  olist_customers_dataset (
customer_id varchar(255),	
customer_unique_id varchar(255),	
customer_zip_code_prefix varchar(255),	
customer_city varchar(255),	
customer_state varchar(255)
   );

CREATE TABLE  olist_geolocation_dataset (
    geolocation_zip_code_prefix varchar(255),
    geolocation_lat varchar(255),
    geolocation_lng varchar(25),
    geolocation_city varchar(255),
    geolocation_state varchar(255)
);

CREATE TABLE  olist_order_items_dataset  (
   order_id	varchar(255),
   order_item_id varchar(255),
   product_id varchar(255),
   seller_id varchar(255), 
   shipping_limit_date varchar(255),
   price varchar(255),
   freight_value varchar(255)
   );

CREATE TABLE  olist_order_payments_dataset (
order_id varchar(255),
payment_sequential	varchar(255),
payment_type	varchar(255),
payment_installments	varchar(255),
payment_value	varchar(255)
   );

drop table olist_order_reviews_dataset;
CREATE TABLE  olist_order_reviews_dataset (
review_id varchar(255),	
order_id varchar(255),	
review_score varchar(255),	
review_comment_title varchar(255),	
review_comment_message varchar(5000),	
review_creation_date varchar(255),	
review_answer_timestamp varchar(255)
   );

CREATE TABLE  olist_orders_dataset (
order_id varchar(255),	
customer_id varchar(255),	
order_status varchar(255),	
order_purchase_timestamp varchar(255),	
order_approved_at varchar(255),	
order_delivered_carrier_date varchar(255),	
order_delivered_customer_date varchar(255),	
order_estimated_delivery_date varchar(255)
   );

CREATE TABLE  olist_products_dataset (
product_id varchar(255),	
product_category_name varchar(255),	
product_name_lenght varchar(255),	
product_description_lenght varchar(255),	
product_photos_qty varchar(255),	
product_weight_g varchar(255),	
product_length_cm varchar(255),	
product_height_cm varchar(255),	
product_width_cm varchar(255)
   );

CREATE TABLE  olist_sellers_dataset (
seller_id varchar(255),	
seller_zip_code_prefix varchar(255),	
seller_city varchar(255),	
seller_state varchar(255)
   );

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_sellers_dataset.csv' INTO TABLE olist_sellers_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_products_dataset.csv' INTO TABLE olist_products_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_orders_dataset.csv' INTO TABLE olist_orders_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_order_reviews_dataset.csv' INTO TABLE olist_order_reviews_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_order_payments_dataset.csv' INTO TABLE olist_order_payments_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_order_items_dataset.csv' INTO TABLE olist_order_items_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_geolocation_dataset.csv' INTO TABLE olist_geolocation_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\olist_customers_dataset.csv' INTO TABLE olist_customers_dataset
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'  
  LINES terminated by '\n'
  ignore 1 lines;
  
  use olist_dataset_project;
  alter table olist_customers_dataset rename customers;
  alter table olist_geolocation_dataset rename geolocation;
  alter table olist_order_items_dataset rename order_items;
  alter table olist_order_payments_dataset rename payment;
  alter table olist_order_reviews_dataset rename review;
  alter table olist_orders_dataset rename orders;
  alter table olist_products_dataset rename product;
  alter table olist_sellers_dataset rename seller;

create table kpi1_table 
select orders.order_id, orders.customer_id, orders.order_status, orders.order_purchase_timestamp,
   orders.order_delivered_customer_date, payment.payment_type, payment.payment_value
   from orders
   inner join payment on
   orders.order_id=payment.order_id;
   
   
  create table kpi2_table  
select review.review_id, review.order_id, review.review_score,
    payment.payment_type, payment.payment_value
   from review
   inner join payment on
   review.order_id=payment.order_id;
   
   
create table kpi3_table 
select orders.order_id, orders.customer_id, orders.order_status, orders.order_purchase_timestamp, orders.order_delivered_customer_date,
	order_items.product_id,
    product.product_category_name
   from orders
   inner join order_items  on
   orders.order_id=order_items.order_id
   inner join product on
   order_items.product_id=product.product_id;
   

create table kpi4_table 
select order_items.order_id, order_items.product_id, order_items.price,
    payment.payment_type, payment.payment_value,
    orders.customer_id, orders.order_status,
    customers.customer_city
   from order_items
   inner join payment  on
   order_items.order_id=payment.order_id
   inner join orders on
   payment.order_id=orders.order_id
   inner join customers on 
   orders.customer_id=customers.customer_id;
   
   
   
create table kpi5_table 
select orders.order_id, orders.order_status, orders.order_purchase_timestamp, orders.order_delivered_customer_date,
	review.review_id, review.review_score
   from orders
   inner join review  on
   orders.order_id=review.order_id;
   
#####################################################################################################################################
   
   -- 1). WEEKDAY VS WEEKEND PAYMENT STATISTICS:
alter table kpi1_table add column day_type varchar(500);

select * from kpi1_table;

set sql_safe_updates= 0;

update kpi1_table
set day_type= if(DAYOFWEEK(order_purchase_timestamp) IN (1, 7),"Weekend","Weekday");

SELECT 
day_type,
concat('$',round(sum(payment_value),2)) as Payment
from kpi1_table
group by day_type;

######################################################################################################################################

-- 2).Number of Orders with review score 5 and payment type as credit card
select 
review_score as Review_Score,
count(order_id) as Number_of_Orders,
payment_type as Payment_type
from kpi2_table
where review_score=5
and payment_type= "credit_card";

######################################################################################################################################

-- 3) Average number of days taken for order_delivered_customer_date for pet_shop

select 
product_category_name as Product_Name,
round(avg(datediff(order_delivered_customer_date, order_purchase_timestamp)),0) as Average_days_taken_to_deliver_customer
from kpi3_table
where product_category_name="pet_shop";

######################################################################################################################################

-- 4) Average price and payment values from customers of sao paulo city

select 
customer_city,
concat('$',format(avg(price),2)) as Avg_Price,
concat('$',format(avg(payment_value),2)) as Avg_Payment_value 
from kpi4_table
where customer_city="sao paulo";

######################################################################################################################################

-- 5) Relationship between shipping days (order_delivered_customer_date - order_purchase_timestamp) Vs review scores.
select review_score as Review_score, round(avg(datediff(order_delivered_customer_date,order_purchase_timestamp)),0) as Avg_Shipping_Days
from kpi5_table
group by review_score
order by review_score;

######################################################################################################################################
   
   
