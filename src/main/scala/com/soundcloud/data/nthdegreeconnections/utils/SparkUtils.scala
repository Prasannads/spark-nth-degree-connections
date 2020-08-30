package com.soundcloud.data.nthdegreeconnections.utils

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkUtils {

  /**
   * Create Spark Session
   * @param appName Name of the application.
   * @return Spark Session
   */
  def createSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  /**
   * Read File and create dataframe
   * @param sparkSession Spark Session.
   * @param file Input File with location.
   * @param header Header to be written or not. "true" or "false"
   * @param delimiter Type of delimiter. "," or "\t"
   * @param schema Schema of the file.
   * @return Dataframe.
   */
  def readCSV(
      sparkSession: SparkSession,
      file: String,
      header: String,
      delimiter: String,
      schema: StructType
  ): DataFrame = {
    sparkSession.read
      .option("delimiter", delimiter)
      .option("header", header)
      .schema(schema)
      .csv(file)
  }

  /**
   * Write Dataframe as CSV file.
   * @param dataFrame Dataframe to write to a file.
   * @param outputLocation Output Location.
   * @param header Header to be written or not. "true" or "false"
   * @param delimiter Type of delimiter. "," or "\t"
   */
  def writeCSV(dataFrame: DataFrame, outputLocation: String, header: String, delimiter: String): Unit = {
    dataFrame.write.option("header", header).option("delimiter", delimiter).csv(outputLocation)
  }

}