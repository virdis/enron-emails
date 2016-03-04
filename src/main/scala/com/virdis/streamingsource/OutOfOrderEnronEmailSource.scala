package com.virdis.streamingsource

import com.virdis.common.fileReader
import com.virdis.models.EnronEmail
import com.virdis.parser.EmailParser
import org.apache.flink.streaming.api.functions.source.EventTimeSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.watermark.Watermark
import org.joda.time.DateTime

/**
  * dataPath - directory for emails
  * delay - time to buffer before emitting events

  * PLEASE DO NOT RUN THIS , TRYING TO HANDLE OUT OF ORDER EVENTS BUT THE TIME DELTA BETWEEN EMAILS IS A LOT
  * DONT WANT TO SPEND TOO MUCH TIME FIXING THIS, SINCE REAL STREAMS DONT HAVE THIS KIND OFF TIME SKEW.

  * @param dataPath
  * @param delay
  */

class OutOfOrderEnronEmailSource(dataPath: String, delay: Int) extends EventTimeSourceFunction[EnronEmail]{

  val delayInMillis: Long = delay * 1000

  @volatile
  private var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
  }

  override def run(ctx: SourceContext[EnronEmail]): Unit = {

    calculateWaterMarkAndEmit(ctx)
  }

  def calculateWaterMarkAndEmit(context: SourceContext[EnronEmail]) = {
    val streamStartTime = DateTime.now().getMillis
    var firstEmailTs = 0L
    var nextWaterMarkTs = 0L
    var nextWaterMarkEmitTs = 0L
    // counter to track the first email
    var count = 0

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

            if (email.nonEmpty) {

              if (count < 1) {
                firstEmailTs = email.get.createdAt.getMillis
                nextWaterMarkTs = firstEmailTs + delayInMillis
                nextWaterMarkEmitTs = calculateTimes(streamStartTime, firstEmailTs, nextWaterMarkTs)
                // emit first event
                context.collectWithTimestamp(email.get, email.get.createdAt.getMillis)

              } else {
                val emailTs = email.get.createdAt.getMillis
                val now = DateTime.now().getMillis
                val emitEmailTs = calculateTimes(streamStartTime, firstEmailTs, emailTs)

                // calculate wait times
                val emailWait = emitEmailTs - now
                val waterMarkWait = nextWaterMarkEmitTs - now

                if (emailWait < waterMarkWait) {
                  Thread.sleep(if (emailWait > 0) emailWait else 0)
                } else if (emailWait > waterMarkWait) {
                  Thread.sleep(if (waterMarkWait > 0) waterMarkWait else 0)
                  context.emitWatermark(new Watermark(nextWaterMarkTs))
                  nextWaterMarkTs = nextWaterMarkEmitTs + delayInMillis
                  nextWaterMarkEmitTs = calculateTimes(streamStartTime, firstEmailTs, nextWaterMarkTs)

                  val waitDelta = emailWait - waterMarkWait
                  Thread.sleep(if (waitDelta > 0) waitDelta else 0)

                } else if (emailWait == waterMarkWait) {
                  Thread.sleep(if (waterMarkWait > 0) waterMarkWait else 0)

                  context.emitWatermark(new Watermark(nextWaterMarkTs))
                  nextWaterMarkEmitTs = calculateTimes(streamStartTime, firstEmailTs, nextWaterMarkTs)
                }

              }
              // emit event
              context.collectWithTimestamp(email.get, email.get.createdAt.getMillis)

            }


            count += 1

        }
    }
  }

  def calculateTimes(processStartTs: Long, evtStartTime: Long, time: Long): Long = {
    val diff = evtStartTime - time
    processStartTs +  diff
  }
}
