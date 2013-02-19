package edu.berkeley.cs.amplab.pipedream

import org.apache.log4j.{Level, Logger}
  /**
   * set all loggers to the given log level.  Returns a map of the value of every logger
   * @param level
   * @param loggers
   * @return
   */
package object SparkUtils {
  def setLogLevels(level: org.apache.log4j.Level, loggers: TraversableOnce[String]) = {
    loggers.map{
      loggerName =>
        val logger = Logger.getLogger(loggerName)
        val prevLevel = logger.getLevel()
        logger.setLevel(level)
        loggerName -> prevLevel
    }.toMap
  }

  def setLogLevels(loggers: Map[String, org.apache.log4j.Level]) = {
    loggers.map{case (k,v) => Logger.getLogger(k).setLevel(v)}
  }

  /**
   * turn off most of spark logging.  Returns a map of the previous values so you can turn logging back to its
   * former values
   */
  def silenceSpark() = {
    setLogLevels(Level.WARN, Seq("spark", "org.eclipse.jetty", "akka"))
  }
}
