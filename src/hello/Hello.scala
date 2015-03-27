package hello

import java.io.File
import java.io.FileWriter

object Hello {

  def using[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName: String, data: String) =
    using(new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }

  def main(args: Array[String]) {
    val data = "Fivestringsinafile"

  writeToFile("/home/pravesh/Desktop/example.txt", data)}
}