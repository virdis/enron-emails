package com.virdis.jobs

import com.virdis.models.EnronEmail
import com.virdis.streamingsource.EnronEmailSource
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.joda.time.DateTime

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

    env.getConfig().registerTypeWithKryoSerializer(classOf[DateTime], new JodaDateTimeSerializer())

    /**
      *Add email data source
     */

    val emails: DataStream[EnronEmail] = env.addSource(new EnronEmailSource(path))

    val counts: DataStream[((String,Int), Long)] = emails
      .flatMap { email => email.recipients.map(mailId => ((mailId, email.day), 1L)) }
      .keyBy(0).sum(1)

    counts.print()

    env.execute("Email Count")
  }
}
