package com.virdis.jobs

import com.virdis.models.{DirectEmail, EnronEmail}
import com.virdis.streamingsource.EnronEmailSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._
import com.virdis.common.Utils._
import scala.collection.immutable.TreeSet


/**
  *  Job calculates the max direct and broadcast emails
  */

object DirectEmailTypeCount {

  def main(args: Array[String]): Unit = {



    val path = "./data/enron_with_categories"

    val env: StreamExecutionEnvironment = MyExecutionEnv.setup

    /**
      * Add email data source
      */

    val emails: DataStream[EnronEmail] = env.addSource(new EnronEmailSource(path))

    /**
      * Filter emails by direct label
      */

    val filteredEmails: DataStream[EnronEmail] = emails.filter(_.label == LABEL_DIRECT)

    /**
      * Flatten the stream to get recipient ids
      */
    val flattenedDirectEmails: DataStream[String] = filteredEmails.flatMap(email => email.recipients)

    /**
      * map the stream to DirectEmails
      */
    val directEmails: DataStream[DirectEmail] = flattenedDirectEmails.map(id => DirectEmail(id, 1L))

    /**
      *  Count emails by id
      */
    val countDirectEmails: DataStream[DirectEmail] = directEmails.keyBy("mailId").sum("count")

    // TODO : Figure out how to write a custom Sink to get the result in desired way
    /**
      *  Max emails with State
      *
        *val topkDirectEmails: DataStream[(DirectEmail, TreeSet[DirectEmail])] = countDirectEmails.keyBy("mailId").mapWithState ((d: DirectEmail, topk: Option[TreeSet[DirectEmail]]) =>
      *topk match {
            *case Some(tree) => {
              *val upTree = updateTree(d, tree)
              *((d, upTree), Some(upTree))
            *}
            *case None => {
      *val initTree = initializeTree(d)
      *((d, initTree), Some(initTree))
            *}
      *}
        *)
        *topkDirectEmails.addSink {
          *res =>
            *println("Result : "+res)

        *}

    */

    /**
      * max count
      */

    countDirectEmails.keyBy("mailId").max("count").addSink {
        res =>
            println("Direct Emails Count : "+res)
    }


    env.execute("Direct Email Type Count")

  }
}
