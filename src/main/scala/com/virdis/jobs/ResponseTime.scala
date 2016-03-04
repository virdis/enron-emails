package com.virdis.jobs

import com.virdis.models.{ResponseCalculator, EnronEmail}
import com.virdis.streamingsource.EnronEmailSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{TumblingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
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
      * create stream of original emails
      */
    val originalEmails: DataStream[EnronEmail] = filterStream.filter(_.isOriginal)

    /**
      * create stream of reply emails
      */
    val replyEmails: DataStream[EnronEmail] = filterStream.filter( !_.isOriginal )



    def flattenStream(ds: DataStream[EnronEmail]): DataStream[ResponseCalculator] = {
      ds.flatMap(em => em.recipients.map(mailid => ResponseCalculator(em.sender, mailid, em.subject, em.createdAt.getMillis)))
    }

    /**
      *  Flatten Email Stream to get recipients
      */

    val flattenOrgEmails: DataStream[ResponseCalculator] = flattenStream(originalEmails)

    val flattenRepEmails: DataStream[ResponseCalculator] = flattenStream(replyEmails)

    /**
      *  Join Original Email Stream (oe) with Reply Email Stream (re)
      *
      *  Join condition: oe.sender == re.recipient && oe.recipient == re.sender && oe.subject == re.subject
      *
      *  Bucket all data from joined stream into a window
      */

    val joinedStream = flattenOrgEmails.join(flattenRepEmails)
      .where(oe =>   (oe.sender, oe.to, oe.subject))
      .equalTo(re => (re.to, re.sender, re.subject))
      .window(TumblingTimeWindows.of(Time.seconds(150)))

    /**
      *  An apply function describes the processing that needs to happen
      *  before emitting the elements
      */
    val applyJoin = joinedStream.apply {
      (oe: ResponseCalculator, re: ResponseCalculator, out: Collector[(ResponseCalculator,Long)]) => {
        val delta = scala.math.abs(re.timestamp - oe.timestamp)
        out.collect((re, delta))
      }
    }

    /**
      * keyby the subject and find min by time delta
     */

    val minDelta = applyJoin.keyBy(re => (re._1.subject)).min(1)

    minDelta.addSink {
      res =>
        println("==Response Email== "+res._1 +" ==Time Delta== "+res._2)
    }

    env.execute("Response Time")

  }
}
