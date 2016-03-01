package com.virdis.jobs

import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.streaming.api.environment.{StreamExecutionEnvironment, LocalStreamEnvironment}

/**
  * Create a simple local streaming environment
 */
object MyExecutionEnv {

  def env: StreamExecutionEnvironment = {
    val config = new Configuration()
    // start dash board
    config.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
    // log file required
    config.setString(ConfigConstants.JOB_MANAGER_WEB_LOG_PATH_KEY, "./data/logFile.txt")

    // local stream execution contex t
    new LocalStreamEnvironment(config)
  }
}
