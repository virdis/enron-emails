package com.virdis.jobs

import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.joda.time.DateTime

/**
  * Created by sandeep on 3/1/16.
  */
object MyExecutionEnv {

  def setup: StreamExecutionEnvironment = {

    /**
      * Setup the stream based on "ingestion time"
      */

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    /**
      * Register JodaDateTime Serializer
      */

    env.getConfig().registerTypeWithKryoSerializer(classOf[DateTime], classOf[JodaDateTimeSerializer])

    env
  }
}
