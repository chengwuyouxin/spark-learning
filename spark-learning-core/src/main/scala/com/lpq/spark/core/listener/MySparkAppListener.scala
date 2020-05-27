package com.lpq.spark.core.listener

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}


class MySparkAppListener extends SparkListener with Logging {

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appname = applicationStart.appName

    logError("================"+appname)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    logError("===========application end time=========="+applicationEnd.time)
  }

}