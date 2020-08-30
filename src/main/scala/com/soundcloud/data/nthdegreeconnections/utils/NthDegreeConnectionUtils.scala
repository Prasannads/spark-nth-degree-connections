package com.soundcloud.data.nthdegreeconnections.utils

import com.soundcloud.data.nthdegreeconnections.utils.SparkUtils.{createSparkSession, expandArrayColumns, readCSV}
import org.apache.spark.sql.DataFrame
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
    val finalNthDegreeConnections = expandArrayColumns(nthDegreedf, "connections").orderBy("user")
    finalNthDegreeConnections.show()
    //writeCSV(finalNthDegreeConnections, outputFile, "false", "\t")
  }

}
