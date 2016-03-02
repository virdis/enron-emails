package com.virdis.jobs

import com.virdis.models.{ResponseCalculator, EnronEmail}
import com.virdis.streamingsource.EnronEmailSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import com.virdis.common.Utils._
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.util.Collector

/**
  *
  * Assumption - If the email subject line is empty , skip it.
 */
object ResponseTime {

  def main(args: Array[String]): Unit = {

    val path = "./data/enron_with_categories"

    val env: StreamExecutionEnvironment = MyExecutionEnv.setup

    /**
      * Add email data source
      */

    val emails: DataStream[EnronEmail] = env.addSource(new EnronEmailSource(path))

    /**
      *  Filter all emails with empty subject line
      */
    val filterStream: DataStream[EnronEmail] = emails.filter( ! _.isSubjectEmpty )

    /**
      *  Flatten Email Stream to get recipients
      */

    val stream1: DataStream[ResponseCalculator] = filterStream
      .flatMap(em => em.recipients.map(maildId => ResponseCalculator(em.sender, maildId, em.subject, em.createdAt.getMillis)))


    /**
      *  Self Join Stream where
      *  recipient from first stream matches sender from second stream
      *  sender from first stream matches recipient from second stream
      *  and subject from first stream matches subject with second stream
      *
      *  Bucket all data from joined stream in a 15 seconds buckey
      */
    val joinedStream = stream1.join(stream1)
      .where(email1 =>   (email1.to, email1.sender, email1.subject))
      .equalTo(email2 => (email2.sender, email2.to, email2.subject))
        .window(TumblingTimeWindows.of(Time.seconds(15)))

    /**
      *  An apply function describes the processing that needs to happen
      *  before emitting the elements
      */
    val applyJoin = joinedStream.apply {
      (em1: ResponseCalculator, em2: ResponseCalculator, out: Collector[(ResponseCalculator,ResponseCalculator,Long)]) => {
        val delta = scala.math.abs(em1.timestamp - em2.timestamp)
        out.collect((em1, em2, delta))
      }
    }

    val minDelta = applyJoin.keyBy(_._3).minBy(2)

    minDelta.addSink {
      res =>
        println("Apply Join : Email 1 "+res._1 +" Email 2 "+res._2 +" Time Delta "+res._3)
    }

    env.execute("Response Time")

  }
}
