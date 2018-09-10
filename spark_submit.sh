spark2-submit --class su.test.spark.TopCategories --master local[*] /home/cloudera/spark-processing-assembly-0.1.jar /user/cloudera/flume/events/*/*/* "jdbc:mysql://localhost:3306/dtarasov?user=root&password=cloudera"

spark2-submit --class su.test.spark.TopProductsInCategory --master local[*] /home/cloudera/spark-processing-assembly-0.1.jar /user/cloudera/flume/events/*/*/* "jdbc:mysql://localhost:3306/dtarasov?user=root&password=cloudera"

spark2-submit --class su.test.spark.TopCountries --master local[*] /home/cloudera/spark-processing-assembly-0.1.jar /user/cloudera/flume/events/*/*/* /user/cloudera/geo/blocks /user/cloudera/geo/locations "jdbc:mysql://localhost:3306/dtarasov?user=root&password=cloudera"
