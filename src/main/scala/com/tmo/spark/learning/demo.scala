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
    val edrFlowDF = sqlContext.read.format("orc").load(edrFlowFilePath)
    val edrHTTPDF = sqlContext.read.format("orc").load(edrHttpFilePath)

    //cdrDF.printSchema()
    //cdrDF.filter(cdrDF("market") === "CAROLINA").show()
    //cdrDF.groupBy(cdrDF("market")).avg("time_to_answer_sec").orderBy(cdrDF("market").asc).show()
    //cdrDF.groupBy(cdrDF("market")).avg("time_to_answer_sec").orderBy(cdrDF("market").desc).show()


    //edrFlowDF.show()
    //edrHTTPDF.show()
    //edrFlowDF.printSchema()
    //edrHTTPDF.printSchema()

    val innerBad = edrFlowDF.join(edrHTTPDF, edrFlowDF("tac") === edrHTTPDF("tac"))
      .select(edrFlowDF("download_bytes"),edrFlowDF("tac"), edrHTTPDF("transaction_downlink_bytes"))
    innerBad.show()
    import sqlContext.implicits._
    val httpFiltered = edrHTTPDF.select(edrHTTPDF("transaction_downlink_bytes").cast("Int"), edrHTTPDF("tac"))
    val flowFiltered = edrFlowDF.select($"tac",$"download_bytes".cast("Int"))

    val innerBetter = flowFiltered.join(httpFiltered, flowFiltered("tac") === httpFiltered("tac"))
      .select(flowFiltered("download_bytes"),flowFiltered("tac"), httpFiltered("transaction_downlink_bytes"))
    innerBetter.show()


    // GroupBy
    // val badPrefilterJoin = edrFlowDF.join(edrHTTPDF).groupBy(edrFlowDF("enodebid"),$"download_bytes",$"transaction_downlink_bytes").agg($"download_bytes".cast("Int") + $"transaction_downlink_bytes")
    // val betterPrefilterJoin = edrFlowDF.join(httpFiltered).groupBy(edrFlowDF("enodebid"),$"download_bytes",$"transaction_downlink_bytes").agg($"download_bytes".cast("Int") + $"transaction_downlink_bytes")

    //betterPrefilterJoin1.show()
    val betterGroupBy = innerBetter.groupBy($"tac").sum("transaction_downlink_bytes")
    betterGroupBy.show()
    val betterGroupByRdd = innerBad.map(row => (row(1).toString, row(2).toString.toInt))
    betterGroupByRdd.reduceByKey(_ + _).toDF().show()

    /*
    Automatically determine the number of reducers for joins and groupbys: Currently in Spark SQL, you need to control the degree of parallelism post-shuffle using “SET spark.sql.shuffle.partitions=[num_tasks];”
     */
    //scoreRDD.reduceByKey((x, y) => if(x > y) x else y)
    //bestScoreData.join(addressRDD)
    //badPrefilterJoin.toRdd.reduceByKey( (x, y) => if(x._1 > y._1) x else y )

      //val innerBetter = edrFlowDF.join(httpFiltered, edrFlowDF("enodebid") === httpFiltered("enodebid")).select(edrFlowDF("download_bytes"),edrFlowDF("enodebid"), httpFiltered("transaction_downlink_bytes"))
    //innerBetter.show()
    //import sqlContext.implicits._
    //edrFlowDF.select(edrFlowDF("download_bytes"),edrFlowDF("enodebid"),edrFlowDF("download_bytes")).join(edrHTTPDF, edrFlowDF("enodebid") === edrHTTPDF("enodebid")).select(edrFlowDF("enodebid"), $"download_bytes", $"transaction_downlink_bytes").show()

    //import sqlContext.implicits._
    //edrFlowDF.join(edrHTTPDF, edrFlowDF("enodebid") === edrHTTPDF("enodebid"), "left_outer").select(edrFlowDF("enodebid"), $"referer", $"ip_src_addr", $"dst_port").show()

    //edrFlowDF.join(edrHTTPDF, edrFlowDF("imsi") === edrHTTPDF("imsi"), "right_outer").show()

    //edrFlowDF.join(edrHTTPDF, edrFlowDF("imsi") === edrHTTPDF("imsi"), "full_outer").show()

    //edrFlowDF.join(edrHTTPDF, edrFlowDF("imsi") === edrHTTPDF("imsi"), "left_semi").show()
    //edrHTTPDF.show()
    sc.stop()
  }
}
