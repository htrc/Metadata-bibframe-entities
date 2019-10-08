package org.hathitrust.htrc.tools.ef.metadata.bibframeentities

import com.gilt.gfc.time.Timer
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.hathitrust.htrc.tools.ef.metadata.bibframeentities.Helper._
import org.hathitrust.htrc.tools.spark.errorhandling.ErrorAccumulator
import org.hathitrust.htrc.tools.spark.errorhandling.RddExtensions._

import scala.language.reflectiveCalls
import scala.xml.{Elem, XML}

object Main {
  val appName: String = "bibframe-entities"

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)
    val inputPath = conf.inputPath().toString
    val outputPath = conf.outputPath().toString
    val numPartitions = conf.numPartitions.toOption

    conf.outputPath().mkdirs()

    // set up logging destination
    conf.sparkLog.foreach(System.setProperty("spark.logFile", _))
    System.setProperty("logLevel", conf.logLevel().toUpperCase)

    // set up Spark context
    val sparkConf = new SparkConf()
    sparkConf.setAppName(appName)
    sparkConf.setIfMissing("spark.master", "local[*]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    implicit val sc: SparkContext = spark.sparkContext

    logger.info("Starting...")

    // record start time
    val t0 = System.nanoTime()

    val xmlParseErrorAccumulator = new ErrorAccumulator[(String, String), String](_._1)
    val bibframeXmlRDD =
      (numPartitions match {
        case Some(n) => sc.sequenceFile[String, String](inputPath, minPartitions = n)
        case None => sc.sequenceFile[String, String](inputPath)
      })
      .tryMapValues(XML.loadString)(xmlParseErrorAccumulator)

    val extractEntitiesErrorAccumulator = new ErrorAccumulator[(String, Elem), String](_._1)
    val entities = bibframeXmlRDD.tryMapValues(extractEntities)(extractEntitiesErrorAccumulator)

    entities.cache()

    val worldCatUrlsDF = entities.flatMap(_._2._1).toDF().distinct()
    val viafEntitiesDF = entities.flatMap(_._2._2).toDF().distinct()
    val locEntitiesDF = entities.flatMap(_._2._3).toDF().distinct()

    val allEntities = worldCatUrlsDF union viafEntitiesDF union locEntitiesDF

    allEntities.write.option("header", "false").json(outputPath + "/entities")

    if (xmlParseErrorAccumulator.nonEmpty || extractEntitiesErrorAccumulator.nonEmpty)
      logger.info("Writing error report(s)...")

    // save any errors to the output folder
    if (xmlParseErrorAccumulator.nonEmpty)
      xmlParseErrorAccumulator.saveErrors(new Path(outputPath, "xmlparse_errors.txt"), _)

    if (extractEntitiesErrorAccumulator.nonEmpty)
      extractEntitiesErrorAccumulator.saveErrors(new Path(outputPath, "extractentities_errors.txt"), _.toString)

    // record elapsed time and report it
    val t1 = System.nanoTime()
    val elapsed = t1 - t0

    logger.info(f"All done in ${Timer.pretty(elapsed)}")
  }

}
