package com.virdis.jobs

import com.virdis.models.EnronEmail
import com.virdis.streamingsource.EnronEmailSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import com.virdis.common.Utils._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows

/**
  * This job calculates the number of emails a person received each day
 */

object EmailCount {

  def main(args: Array[String]) {
    val path = "./data/enron_with_categories"

    val env: StreamExecutionEnvironment = MyExecutionEnv.setup

    /**
      * Add email data source
     */

    val emails: DataStream[EnronEmail] = env.addSource(new EnronEmailSource(path))

    val counts: DataStream[((String,String), Long)] = emails
      .flatMap { email => email.recipients.map(mailId => ((mailId, dateFormatter.print(email.createdAt)), 1L)) }
      .keyBy(0).sum(1)

    counts.addSink {
      res =>
        println("== Count =="+res)
    }

    env.execute("Email Count")
  }
}
