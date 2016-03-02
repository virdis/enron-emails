package com.virdis.jobs

import com.virdis.models.EnronEmail
import com.virdis.streamingsource.EnronEmailSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import com.virdis.common.Utils._


/**
  *  Job calculates max broadcast emails by sender
  */

object BroadCastEmailTypeCount {

  def main(args: Array[String]): Unit = {

    val path = "./data/enron_with_categories"

    val env: StreamExecutionEnvironment = MyExecutionEnv.setup

    /**
      * Add email data source
      */

    val emails: DataStream[EnronEmail] = env.addSource(new EnronEmailSource(path))

    /**
      *  Filter email with broadcast label
      */
    val broadCastEmails: DataStream[EnronEmail] = emails.filter(_.label == LABEL_BROADCAST)

    val count: DataStream[(String, Long)] = broadCastEmails.map(e => (e.sender, 1L))

    /**
      *  Sum by mailid
      */
    val sumEmailsById = count.keyBy(_._1).sum(1)

    /**
      * max by sender
      */
    val maxBySender = sumEmailsById.keyBy(_._1).maxBy(1)

    maxBySender.addSink {
      res =>
        println("BroadCast Email Count"+res)
    }

    env.execute("BroadCast Email Type Count")


  }
}
