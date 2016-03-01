package com.virdis.jobs

import com.virdis.models.EnronEmail
import com.virdis.streamingsource.EnronEmailSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream

/**
  * This job calculates the number of emails each person received each day
 */

object EmailCount {

  def main(args: Array[String]) {
    val path = "./data/enron_with_categories"

    /**
      *Setup the stream based on "ingestion time"
     */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)


    /**
      *Add email data source
     */

    val emails: DataStream[EnronEmail] = env.addSource(new EnronEmailSource(path))

    emails.addSink(println(_))

    env.execute("Email Count")
  }
}
