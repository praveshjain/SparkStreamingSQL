package org.apache.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object StreamParquet {

  var i: Int = 1

  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("HdfsWordCount")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD
    val ssc = new StreamingContext(sc, Seconds(2))

    val d1 = sc.parallelize(Array(("a", 10), ("b", 3))).map(p => Person(p._1, p._2))
    d1.saveAsParquetFile("/home/pravesh/Desktop/stream.parquet")
    val pFile = sqlContext.parquetFile("/home/pravesh/Desktop/stream.parquet")
    pFile.registerAsTable("data")

    //    var p1 = new Person("randomGuy",21)
    //    var rdd1 = sc.parallelize(Array(p1))
    //    rdd1.registerAsTable("data")

    val lines = ssc.textFileStream("/home/pravesh/Desktop/people/")
    lines.foreachRDD(rdd => {
      val person = rdd.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
      //      person.foreach(println)
      //      var name = "/home/pravesh/Desktop/new" + i + ".parquet" 
      //      person.saveAsParquetFile(name)
      //      val pFile2 = sqlContext.parquetFile(name)
      //      pFile2.insertInto("data")
      person.insertInto("data")
    })

    //    val result = sqlContext.sql("select * from data")
    //    result.foreach(println)
    ssc.start()
    ssc.stop(false, true)
    //    ssc.awaitTermination()

    val result = sqlContext.sql("select * from data")
    result.foreach(println)

  }
}