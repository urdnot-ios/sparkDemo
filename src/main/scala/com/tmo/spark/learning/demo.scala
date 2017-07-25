package com.tmo.spark.learning

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object demo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Demo")
      .setMaster("local[*]")
      //.set("spark.hadoop.hadoop.security.authentication", "kerberos")
      //.set("spark.hadoop.hadoop.security.authorization", "true")
      //.set("spark.hadoop.yarn.resourcemanager.principal", "rm/_HOST@HDPQUANTUMSTG.COM")
      //.set("spark.hadoop.dfs.namenode.kerberos.principal", "nn/_HOST@HDPQUANTUMSTG.COM")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new HiveContext(sc)
    val cdrFilePath = "src/main/resources/ORC-processcdrbolt-88-474-1499200945693.orc"
    val edrFlowFilePath = "src/main/resources/2017-07-22-flow.orc"
    val edrHttpFilePath = "src/main/resources/2017-07-22-http.orc"
    val cdrDF = sqlContext.read.format("orc").load(cdrFilePath)
    val edrFlowDF = sqlContext.read.format("orc").load(cdrFilePath)
    val edrHTTPDF = sqlContext.read.format("orc").load(cdrFilePath)

    cdrDF.printSchema()
    cdrDF.filter(cdrDF("market") === "CAROLINA").show()
    cdrDF.groupBy(cdrDF("market")).avg("time_to_answer_sec").orderBy(cdrDF("market").asc).show()
    cdrDF.groupBy(cdrDF("market")).avg("time_to_answer_sec").orderBy(cdrDF("market").desc).show()


    //edrFlowDF.show()
    edrFlowDF.join(edrHTTPDF, edrFlowDF("imsi") === edrHTTPDF("imsi")).show()

    edrFlowDF.join(edrHTTPDF, edrFlowDF("imsi") === edrHTTPDF("imsi"), "left_outer").show()

    edrFlowDF.join(edrHTTPDF, edrFlowDF("imsi") === edrHTTPDF("imsi"), "right_outer").show()

    edrFlowDF.join(edrHTTPDF, edrFlowDF("imsi") === edrHTTPDF("imsi"), "full_outer").show()

    edrFlowDF.join(edrHTTPDF, edrFlowDF("imsi") === edrHTTPDF("imsi"), "left_semi").show()
    //edrHTTPDF.show()
    sc.stop()
  }
}
