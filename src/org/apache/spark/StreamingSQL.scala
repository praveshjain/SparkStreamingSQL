package org.apache.spark

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import java.io.File
import java.io.FileWriter
import java.io.PrintWriter

object StreamSQL {

  case class Person(name: String, age: Int)

  def using[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName: String, data: String) =
    using(new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }

  def appendToFile(fileName: String, textData: String) =
    using(new FileWriter(fileName, true)) {
      fileWriter =>
        using(new PrintWriter(fileWriter)) {
          printWriter => printWriter.println(textData)
        }
    }

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("HdfsWordCount")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD
    val ssc = new StreamingContext(sc, Seconds(2))

    val d1 = sc.parallelize(Array(("a", 10), ("b", 3))).map(p => Person(p._1, p._2))
    d1.saveAsParquetFile("/home/pravesh/Desktop/stream.parquet")
    d1.saveAsParquetFile("/home/pravesh/Desktop/stream2.parquet")
    val pFile = sqlContext.parquetFile("/home/pravesh/Desktop/stream.parquet")
    val pFile2 = sqlContext.parquetFile("/home/pravesh/Desktop/stream2.parquet")
    pFile.registerAsTable("data")
    pFile2.registerAsTable("data2")

    var newFile:Int = 0
    val lines = ssc.textFileStream("/home/pravesh/Desktop/people/")
    lines.foreachRDD(rdd => {
      val person = rdd.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
      //      person.foreach(println)
      //      var name = "/home/pravesh/Desktop/new" + i + ".parquet" 
      //      person.saveAsParquetFile(name)
      //      val pFile2 = sqlContext.parquetFile(name)
      //      pFile2.insertInto("data")
      person.insertInto("data")
      newFile = person.collect.size
    })

    val lines2 = ssc.textFileStream("/home/pravesh/Desktop/people2/")

    lines2.foreachRDD(rdd => {
      val person = rdd.map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
      //      person.foreach(println)
      //      var name = "/home/pravesh/Desktop/new" + i + ".parquet" 
      //      person.saveAsParquetFile(name)
      //      val pFile2 = sqlContext.parquetFile(name)
      //      pFile2.insertInto("data")
      person.insertInto("data2")
      if (person.collect.size>0 || newFile>0){
      val result = sqlContext.sql("select data.name FROM data LEFT JOIN data2 ON data.age=data2.age ORDER BY data.name")
      result.foreach(x => println(x))

      result.foreach(x => appendToFile("/home/pravesh/Desktop/example.txt", x.toString))
      }
//      System.exit(1)
    })

    //    val result = sqlContext.sql("select * from data")
    //    result.foreach(println)
    ssc.start()
    //    ssc.stop(false, true)
    ssc.awaitTermination()

    //    println("printing data")
    //    val result = sqlContext.sql("select * from data")
    //    result.foreach(println)
    //    println("printing data2")
    //    val result2 = sqlContext.sql("select * from data2")
    //    result2.foreach(println)
  }
}