from pyspark import SparkContext
from pyspark.sql import HiveContext

cdr_file_path = "src/main/resources/ORC-processcdrbolt-88-474-1499200945693.orc"
edr_flow_path = "src/main/resources/2017-07-22-flow.orc"
edr_http_path = "src/main/resources/2017-07-22-http.orc"

sc = SparkContext("local", "Simple App")
sqlContext = HiveContext(sc)
sqlContext.setConf("spark.sql.orc.filterPushdown", "true")

cdr_df = sqlContext.read.format("orc").load(cdr_file_path)
edr_flow_df = sqlContext.read.format("orc").load(edr_flow_path)
edr_http_df = sqlContext.read.format("orc").load(edr_http_path)

cdr_df.printSchema()
cdr_df.filter(cdr_df.market == "CAROLINA").show()
cdr_df.groupBy(cdr_df.market).avg("time_to_answer_sec").orderBy(cdr_df.market.asc()).show()
cdr_df.groupBy(cdr_df.market).avg("time_to_answer_sec").orderBy(cdr_df.market.desc()).show()

edr_flow_df.printSchema()
edr_flow_df.show()

edr_http_df.printSchema()
edr_http_df.show()


sc.stop()