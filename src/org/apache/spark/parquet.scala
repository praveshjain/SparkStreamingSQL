package org.apache.spark

object parquet {

  case class Person(name: String, age: Int)

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("HdfsWordCount")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD

    //    val people = sc.textFile("C:/Users/pravesh.jain/Desktop/people/people.txt").map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    //    people.saveAsParquetFile("C:/Users/pravesh.jain/Desktop/people/people.parquet")
    //
    //    val parquetFile = sqlContext.parquetFile("C:/Users/pravesh.jain/Desktop/people/people.parquet")

    val d1 = sc.parallelize(Array(("a", 10), ("b", 3))).map(p => Person(p._1, p._2))
    d1.saveAsParquetFile("/home/pravesh/Desktop/p1.parquet")
    val d2 = sc.parallelize(Array(("a", 10), ("b", 3), ("c", 5))).map(p => Person(p._1, p._2))
    d2.saveAsParquetFile("/home/pravesh/Desktop/p2.parquet")
    val f1 = sqlContext.parquetFile("/home/pravesh/Desktop/p1.parquet")
    val f2 = sqlContext.parquetFile("/home/pravesh/Desktop/p2.parquet")
    f1.registerAsTable("logs")
    f2.insertInto("logs")
    val result = sqlContext.sql("select * from logs")
    println("Now Printing");
    result.foreach(println)
    sqlContext.sql("select count(*) from logs").foreach(println)
  }
}