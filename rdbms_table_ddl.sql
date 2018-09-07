create table top_categories (product_category VARCHAR(255), category_count BIGINT);
create table top_categories_hive (product_category VARCHAR(255), category_count BIGINT);

create table top_products_in_categories(product_category VARCHAR(255), product_name VARCHAR(255), product_count bigint(20));
create table top_products_in_categories_hive(product_category VARCHAR(255), product_name VARCHAR(255), product_count bigint(20));

create table top_countries_by_total (country varchar(255), total DECIMAL(10,2));
create table top_countries_by_total_hive (country varchar(255), total DECIMAL(10,2));