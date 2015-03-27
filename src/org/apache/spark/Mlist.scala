package org.apache.spark

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.Duration

object Mlist {
  
  case class Persons(name: String, age: Int)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("HdfsWordCount")
    val sc = new SparkContext(sparkConf)
    // Create the context
    val ssc = new StreamingContext(sc, Seconds(2))
    
    val lines = ssc.textFileStream("C:/Users/pravesh.jain/Desktop/people/")
    lines.foreachRDD(rdd=>rdd.foreach(println))
    
    val sqc = new SQLContext(sc);
    import sqc.createSchemaRDD
    
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    
    lines.foreachRDD(rdd=>{
      rdd.map(_.split(",")).map(p => Persons(p(0), p(1).trim.toInt)).registerAsTable("data")
      val teenagers = sqc.sql("SELECT name FROM data WHERE age >= 13 AND age <= 19")
      teenagers.foreach(println)
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}