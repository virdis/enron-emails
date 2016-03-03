package com.virdis.streamingsource

import com.virdis.common.fileReader
import com.virdis.models.EnronEmail
import com.virdis.parser.EmailParser
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
    EnronEmailOrderedSource generates a stream of Enron Emails.
    dataPath is used to provide the path to the directory which contains
    the subdirectories which contains the emails.

    For this stream source we are going to use the "ingestion time" as the time of
    the event.

 */
class EnronEmailSource(dataPath: String) extends SourceFunction[EnronEmail] {

  @volatile
  private var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
  }

  override def run(ctx: SourceContext[EnronEmail]): Unit = {

  /**
    * Get all files from subdirectories
    * We are interested only in the Email Content till the X-From line
    * once we read the email up to X-From Line we break from reading the file further
    *
    */
    val files = EmailParser.listAllTxtFiles(dataPath)
    files.foreach {
      file =>
        fileReader.using(scala.io.Source.fromFile(file)) {
          src =>
            val lines: Iterator[String] = src.getLines()
            val emailContent = new StringBuilder()
            var stopReading = true
            while(isRunning && lines.hasNext && stopReading) {
              val line = lines.next()
              emailContent.append(line)
              if (line.contains(EmailParser.X_FILENAME)) stopReading = false
            }
            val email: Option[EnronEmail] = EmailParser.buildEmail(emailContent.toString())

            /**
                emit email to stream
             */
            if (email.nonEmpty) ctx.collect(email.get)

          /**
            * Un comment line to see all skipped and malformed files
            **/
          //else println("Filename : "+file.getAbsolutePath)

        }
    }
  }
}