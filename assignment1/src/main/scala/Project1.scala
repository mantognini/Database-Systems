package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Project1 {
  type Path = String

  case class Settings(
    input: Path,
    output: Path
  )

  def extractSettings(args: Array[String]): Settings = {
    if (args.length != 2)
      throw new IllegalArgumentException("Incorrect number of parameters")

    Settings(args(0), args(1))
  }

  def main(args: Array[String]): Unit = {
    val s = extractSettings(args)

    val conf = new SparkConf().setAppName("mantogni.Project1")
    val sc = new SparkContext(conf)
  }
}
