package org.apache.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.io.File
import java.io.FileWriter

object StreamSQL2 {

  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("HdfsWordCount")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD
    val ssc = new StreamingContext(sc, Seconds(2))

    var d1 = sc.parallelize(Array(("a", 10), ("b", 3))).map(p => Person(p._1, p._2))
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
      d1 = d1.union(person)
    })
//    val lines2 = ssc.textFileStream("/home/pravesh/Desktop/people2/")
//
//    lines2.foreachRDD(rdd => {
//      val person = rdd.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
//      //      person.foreach(println)
//      //      var name = "/home/pravesh/Desktop/new" + i + ".parquet" 
//      //      person.saveAsParquetFile(name)
//      //      val pFile2 = sqlContext.parquetFile(name)
//      //      pFile2.insertInto("data")
//      person.insertInto("data2")
//    })
//    
//    //    val result = sqlContext.sql("select * from data")
//    //    result.foreach(println)
    ssc.start()
    ssc.stop(false, true)
    //    ssc.awaitTermination()

    //    val result = sqlContext.sql("select data2.name, data.age FROM data2 INNER JOIN data ON data2.age=data.age")
//    d1.foreach(println)
    d1.registerAsTable("data")
    println("printing data")
    val result = sqlContext.sql("select * from data")
    result.foreach(x=>println(x))
    println("printing data2")
//    val result2 = sqlContext.sql("select * from data2")
//    result2.foreach(println)
  }
}