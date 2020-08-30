package com.soundcloud.data.nthdegreeconnections

import com.soundcloud.data.nthdegreeconnections.utils.NthDegreeConnectionUtils._
import com.typesafe.scalalogging.LazyLogging

import scala.annotation.tailrec
import scala.util.{Failure, Success}

object AppMain extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val usage =
      """
      Usage: calculate-connections.jar [--input-file input] [--degrees degrees] [--output-file output]
      """

    val defaultOptions: Map[String, Any] = Map(
      "inputFile"  -> "",
      "degrees"    -> 0,
      "outputFile" -> ""
    )

    @tailrec
    def parseArgs(list: List[String], options: Map[String, Any]): Map[String, Any] = {
      list match {
        case Nil => options
        case "--input-file" :: value :: tail =>
          parseArgs(tail, options ++ Map("inputFile" -> value))
        case "--degrees" :: value :: tail =>
          parseArgs(tail, options ++ Map("degrees" -> value.toInt))
        case "--output-file" :: value :: tail =>
          parseArgs(tail, options ++ Map("outputFile" -> value))
        case option :: _ =>
          println("Unknown option " + option)
          println(usage)
          sys.exit(1)
      }
    }

    val options = parseArgs(args.toList, defaultOptions)

    val inputFile  = options("inputFile").asInstanceOf[String]
    val degrees    = options("degrees").asInstanceOf[Int]
    val outputFile = options("outputFile").asInstanceOf[String]

    val nthDegreeConnections = new NthDegreeConnections(createEdgesDataFrame(inputFile), degrees)
    nthDegreeConnections.run() match {
      case Success(nthDegreedf) => writeNthDegreeConnDataFrame(nthDegreedf, outputFile)
      case Failure(exception) =>
        logger.error(s"Nth Degree calculation failed with exception ${exception.getMessage}")
        sys.exit(1)
    }

  }

}
