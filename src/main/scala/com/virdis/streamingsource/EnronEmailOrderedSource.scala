package com.virdis.streamingsource

import com.virdis.models.EnronEmail
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/*
    EnronEmailOrderedSource generates a stream of Enron Emails.
    dataPath is used to provide the path to the directory which contains
    the subdirectories which contains the emails.

    For this stream source we are going to use the "ingestion time" as the time of
    the event.

 */
class EnronEmailOrderedSource(dataPath: String) extends SourceFunction[EnronEmail] {
  override def cancel(): Unit = ???

  override def run(ctx: SourceContext[EnronEmail]): Unit = ???
}
