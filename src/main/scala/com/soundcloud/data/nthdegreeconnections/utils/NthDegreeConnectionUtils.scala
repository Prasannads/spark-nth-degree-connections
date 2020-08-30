package com.soundcloud.data.nthdegreeconnections.utils

import com.soundcloud.data.nthdegreeconnections.udfs.SparkUdfs.mergeSeq
import com.soundcloud.data.nthdegreeconnections.utils.SparkUtils.{createSparkSession, readCSV, writeCSV}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, col, concat_ws}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object NthDegreeConnectionUtils {

  def createEdgesDataFrame(inputFile: String): DataFrame = {
    val edgeSchema: StructType = StructType(
      List(
        StructField("user", StringType, nullable = false),
        StructField("connection", StringType, nullable = false)
      )
    )

    val sparkSession = createSparkSession("nth_degree_connections")
    readCSV(sparkSession, inputFile, "false", "\t", edgeSchema)
  }

  def writeNthDegreeConnDataFrame(nthDegreedf: DataFrame, outputFile: String): Unit = {
    //val finalNthDegreeConnections = expandArrayColumns(nthDegreedf, "connections").orderBy("user")

    val finalNthDegreeConnections =
      nthDegreedf
        .withColumn("connections", concat_ws("\t", mergeSeq(array(col("user")), col("connections"))))
        .drop("user")
    writeCSV(finalNthDegreeConnections, outputFile, "false", "\t")
  }

}
