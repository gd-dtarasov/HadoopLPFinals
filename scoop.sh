sqoop export --connect jdbc:mysql://localhost:3306/dtarasov --username root --password cloudera --table top_categories_hive --hcatalog-database dtarasov --hcatalog-table top_categories
sqoop export --connect jdbc:mysql://localhost:3306/dtarasov --username root --password cloudera --table top_products_in_categories_hive --hcatalog-database dtarasov --hcatalog-table top_products_in_categories
 sqoop export --connect jdbc:mysql://localhost:3306/dtarasov --username root --password cloudera --table top_countries_by_total_hive --hcatalog-database dtarasov --hcatalog-table top_countries_by_total