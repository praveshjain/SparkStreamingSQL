package org.apache.spark

import java.nio.file.{ Paths, Files }
import java.nio.charset.StandardCharsets

import org.apache.spark.streaming.StreamingContext
import org.netlib.util.Second
import org.apache.spark.streaming.Seconds

case class Person(name: String, age: Int)

object SQL {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("Sql")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD

    // Define the schema using a case class.
    // Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit, 
    // you can use custom classes that implement the Product interface.

    // Create an RDD of Person objects and register it as a table.
    val people = sc.textFile("/home/pravesh/Desktop/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    val names = sc.textFile("/home/pravesh/Desktop/names.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))

    people.registerAsTable("people")
    names.registerAsTable("names")

    // SQL statements can be run by using the sql methods provided by sqlContext.
    val teenagers = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    val teenagers2 = sqlContext.sql("SELECT name FROM names WHERE age >= 13 AND age <= 19")

    // The results of SQL queries are SchemaRDDs and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by ordinal.
    //    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)
    //    teenagers2.map(t => "Name: " + t(0)).collect().foreach(println)

    val joins = sqlContext.sql("SELECT people.name, names.age FROM people INNER JOIN names ON people.age=names.age")
//    joins.map(t => "Name: " + t(0)).collect().foreach(println)

    joins.foreach(println)
    val x = joins.collect()
    x.foreach(println)
  }
}