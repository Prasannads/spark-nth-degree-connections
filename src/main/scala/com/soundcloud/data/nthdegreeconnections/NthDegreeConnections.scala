package com.soundcloud.data.nthdegreeconnections

import com.soundcloud.data.nthdegreeconnections.udfs.SparkUdfs._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{array, array_except, col, collect_list}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.annotation.tailrec

class NthDegreeConnections(val edgesDataFrame: DataFrame, val degrees: Int) extends Serializable with LazyLogging {

  def run(): DataFrame = {

    /**
     * Swap users to create a complete set of connections.Because there can be
     * A -> B or B -> A but not both.
     * */

    val swappedEdgesDF: DataFrame =
      edgesDataFrame.select(col("connection").alias("user"), col("user").alias("connection"))
    val allEdgesDf: Dataset[Row] = edgesDataFrame.union(swappedEdgesDF)

    // Add a column as an array with users (A,B) as they are 1st degree connections by default.
    val allEdgesRefinedDf = allEdgesDf.withColumn("connections", array(col("user"), col("connection")))

    // Recursive call on finding the Nth degree connection.
    val calculatedDegreesdf = calculateDegreesRecursive(allEdgesRefinedDf, allEdgesDf)

    // Post data cleaning and transformations.
    val nthDegreedf = calculatedDegreesdf
      .alias("v1")
      // join is required on the same table here as we did swap of users and connections above.
      .join(calculatedDegreesdf.alias("v2"), col("v1.user") === col("v2.connection"), "left")
      .select(col("v1.user"), mergeSeq(col("v1.connections"), col("v2.connections")).alias("connections"))
      .distinct()
      .groupBy(col("v1.user"))
      .agg(flattenSeq(collect_list(col("connections"))).alias("connections"))
      .withColumn("connections", array_except(col("connections"), array("user")))
      .orderBy(col("user"))

    nthDegreedf
  }

  /**
   * Recursive function with Termination Condition as when the iterate reaches (degrees - 1)
   * The logic here is to find the connections' connection add them to First Degree connection and
   * repeat it until the desired level of degree has been reached.
   * ----------------------------------   ---------------------
   * |        allEdgesRefinedDf       |   |    allEdgesDf     |
   * ----------------------------------   ---------------------
   * |  user | connection| connections|   |  user | connection|
   * ----------------------------------   ---------------------
   * |   A   |    B      | (A,B)      |   |   B   |    C      |
   * ----------------------------------   ---------------------
   * Result of 1 Iteration:
   * * ----------------------------------
   * * |        allEdgesRefinedDf       |
   * * ----------------------------------
   * * |  user | connection| connections|
   * * ----------------------------------
   * * |   A   |    C      | (A,B, C)   |
   * * ----------------------------------
   * @param allEdgesRefinedDf DataFrame of Edges with 1st Degree connections.
   * @param allEdgesDf Dataframe of Edges.
   * @param iteration Iteration always start at 1
   * @return Dataframe with degree connections to the level of N.
   */
  @tailrec
  private def calculateDegreesRecursive(
      allEdgesRefinedDf: DataFrame,
      allEdgesDf: DataFrame,
      iteration: Int = 1
  ): DataFrame = {
    val joinedDf = allEdgesRefinedDf
      .alias("all_edges_refined")
      .join(allEdgesDf.alias("all_edges"), col("all_edges_refined.connection") === col("all_edges.user"))

    val calculatedDegreesdf = joinedDf.select(
      col("all_edges_refined.user").alias("user"),
      col("all_edges.connection").alias("connection"),
      appendToSeq(col("all_edges_refined.connections"), col("all_edges.connection")).alias("connections")
    )

    if(iteration == (degrees - 1)) {
      calculatedDegreesdf
    } else {
      calculateDegreesRecursive(calculatedDegreesdf, allEdgesDf, iteration + 1)
    }
  }

}
