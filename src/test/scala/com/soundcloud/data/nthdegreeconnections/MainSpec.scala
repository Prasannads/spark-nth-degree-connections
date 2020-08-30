package com.soundcloud.data.nthdegreeconnections

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, concat_ws, size}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funspec.AnyFunSpec

import scala.util.Success

class MainSpec extends AnyFunSpec {

  var sparkSession: SparkSession = _
  var inputDf: DataFrame         = _

  sparkSession = SparkSession
    .builder()
    .appName("nth_degree_connections_test")
    .config("spark.testing.memory", "2147480000")
    .master("local[*]")
    .getOrCreate()

  val rootLogger: Logger = Logger.getLogger(getClass)
  rootLogger.setLevel(Level.ERROR)

  val inputRdd: RDD[Row] = sparkSession.sparkContext.parallelize(
    Seq(
      Row("davidbowie", "omid"),
      Row("davidbowie", "kim"),
      Row("kim", "torsten"),
      Row("torsten", "omid"),
      Row("brendan", "torsten"),
      Row("ziggy", "davidbowie"),
      Row("mick", "ziggy")
    )
  )

  val edgeSchema: StructType = StructType(
    List(
      StructField("user", StringType, nullable = false),
      StructField("connection", StringType, nullable = false)
    )
  )

  inputDf = sparkSession.createDataFrame(inputRdd, edgeSchema)

  describe("Calculate 2nd degree computations test") {

    val degrees = 2

    val connectedComponents =
      new NthDegreeConnections(inputDf, degrees)

    val nthDegreesdf = connectedComponents.run() match {
      case Success(nthDegreesdf) =>
        Right(nthDegreesdf).value
    }

    it("should return the same number of output rows as number of distinct users in input") {
      assert(nthDegreesdf.count() === 7)
    }

    it("should return the connections in sorted order") {

      val brendanConnections = nthDegreesdf
        .filter(col("user") === "brendan")
        .select(concat_ws(",", col("connections")))
        .rdd
        .map(r => r(0).toString)
        .collect()
        .mkString(",")

      assert("kim,omid,torsten" === brendanConnections)
      assert("omid,kim,torsten" !== brendanConnections)
      assert("torsten,kim,omid" !== brendanConnections)
    }

    it("should return the right number of connections") {

      val mickConnections = nthDegreesdf
        .filter(col("user") === "mick")
        .select(size(col("connections")))
        .rdd
        .map(r => r(0).toString)
        .collect()
        .head
        .toInt

      assert(mickConnections === 2)
    }

    it("should return the result itself in a sorted order") {

      val connections = nthDegreesdf
        .select(col("user"))
        .rdd
        .map(r => r(0).toString)
        .collect()
        .mkString(",")

      assert("brendan,davidbowie,kim,mick,omid,torsten,ziggy" === connections)
    }
  }

  describe("Calculate 4th degree computations test") {

    val degrees = 4

    val connectedComponents =
      new NthDegreeConnections(inputDf, degrees)

    val nthDegreesdf = connectedComponents.run() match {
      case Success(nthDegreesdf) =>
        Right(nthDegreesdf).value
    }

    it("should return the same number of output rows as number of distinct users in input") {
      assert(nthDegreesdf.count() === 7)
    }

    it("should return the right number of connections") {

      val brendanConnections = nthDegreesdf
        .filter(col("user") === "brendan")
        .select(concat_ws(",", col("connections")))
        .rdd
        .map(r => r(0).toString)
        .collect()
        .mkString(",")

      assert("davidbowie,kim,omid,torsten,ziggy" === brendanConnections)
    }
  }

}
