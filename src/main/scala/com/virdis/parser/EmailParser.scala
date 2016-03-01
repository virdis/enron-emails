package com.virdis.parser

import java.io.File
import java.util.Date

import com.virdis.common.fileReader
import com.virdis.models.EnronEmail
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.control.NonFatal


object EmailParser {
  @transient
  val EMAIL_DATE_FORMATTER = DateTimeFormat.forPattern("EEE, d MMM y H:m:s Z (z)")

  val DATE_MARKER = "Date:"
  val FROM_MARKER = "From:"
  val X_FROM_MARKER = "X-From:"
  val TO_MARKER = "To:"
  val CC_MARKER = "Cc:"
  val BCC_MARKER = "Bcc:"
  val SUBJECT_MARKER = "Subject:"
  val EMAIL_FORWARD_MARKER = "FW:"
  val EMAIL_REPLY_MARKER = "RE:"
  val MIME_VERSION_MARKER = "Mime-Version:"


  def listAllTxtFiles(path: String): List[File] = {
    val file = new File(path)
    val subDirectories = file.listFiles.filter(_.isDirectory)
    subDirectories.flatMap(files => files.listFiles.filter(f => f.getName.contains("txt"))).toList
  }

  def parseDate(line: String): DateTime = {
    val date = line.split(DATE_MARKER)(1)
    EMAIL_DATE_FORMATTER.parseDateTime( date.trim )
  }

  def senderEmail(line: String): String = {
    line.split(FROM_MARKER)(1).trim
  }

  def extractMultipleEmails(e: String): Set[String] = {
    if (e.contains(",")) {
      // add strings that are emails
      e.split(",").foldLeft(Set.empty[String])((acc,a) => if (!a.contains("@")) acc else acc + a.trim)
    } else {
      Set(e.trim)
    }
  }

  /**
    *check presence of "," . if present we know there are multiple emails
    *extract emails, trim and add them to a set
  */
  def recipientEmailsByTags(line: String, tag: String): Set[String] = {
    if (line.contains(tag)) {
      val email = line.split(tag)(1)
      extractMultipleEmails(email)
    } else {
      extractMultipleEmails(line)
    }
  }


  /**
      *We need to be case insensitive while looking for "RE" or" "FW" in the Subject line
   */
  def subject(line: String): (String,Boolean) = {
    val subject = line.split(SUBJECT_MARKER)(1)

    if (subject.toLowerCase.contains(EMAIL_FORWARD_MARKER.toLowerCase)) {
      (subject.toLowerCase.split(EMAIL_FORWARD_MARKER.toLowerCase)(1).trim, false)
    } else if (subject.toLowerCase.contains(EMAIL_REPLY_MARKER.toLowerCase)) {
      (subject.toLowerCase.split(EMAIL_REPLY_MARKER.toLowerCase)(1).trim, false)
    } else {
      (subject.trim.toLowerCase, true)
    }
  }

  /**
      *We can find email recipients with following tags:
      *1. To:
      *2. Cc:
      *3. Bcc:
      *There is a chance that email ids can be repeated in the To, Cc and the Bcc section. To over come that
      *we can get the emails by each tag and then merge them into a Set removing duplicate.
   */
  def mergeRecipients(to: Set[String], cc: Set[String], bcc: Set[String]) = to ++ cc ++ bcc

  def buildEmail(emailContent: String): Option[EnronEmail] = {

    var to: Set[String] = Set.empty[String]
    var cc: Set[String] = Set.empty[String]
    var bcc: Set[String] = Set.empty[String]
    val enronEmail = new EnronEmail()
    try {

        val date  = parseDate(emailContent.substring(emailContent.indexOf(DATE_MARKER), emailContent.indexOf(FROM_MARKER)))
        enronEmail.day = date.getDayOfYear
        enronEmail.timeStamp = date.getMillis
        enronEmail.sender = senderEmail(emailContent.substring(emailContent.indexOf(FROM_MARKER), emailContent.indexOf(TO_MARKER)))
        to = recipientEmailsByTags(emailContent.substring(emailContent.indexOf(TO_MARKER), emailContent.indexOf(SUBJECT_MARKER)),
          TO_MARKER)

        val indexOfCC = emailContent.indexOf(CC_MARKER)
        // if Cc: tag is present use it to extract Subject
       if (indexOfCC != -1) {
          val subFlag = subject(emailContent.substring(emailContent.indexOf(SUBJECT_MARKER), indexOfCC))
          cc = recipientEmailsByTags(emailContent.substring(indexOfCC, emailContent.indexOf(MIME_VERSION_MARKER)), CC_MARKER)
          enronEmail.subject    = subFlag._1
          enronEmail.isOriginal = subFlag._2
        } else {
          val subFlag = subject(emailContent.substring(emailContent.indexOf(SUBJECT_MARKER), emailContent.indexOf(MIME_VERSION_MARKER)))
          enronEmail.subject    = subFlag._1
          enronEmail.isOriginal = subFlag._2
        }
        // if Bcc: tag is present use it to extract recipients
        val indexOfBcc = emailContent.indexOf(BCC_MARKER)
        if (indexOfBcc != -1) {
          bcc = recipientEmailsByTags(emailContent.substring(indexOfBcc, emailContent.indexOf(X_FROM_MARKER)), BCC_MARKER)
        }
        enronEmail.recipients =  mergeRecipients(to, cc, bcc)
        Option(enronEmail)

    } catch {
      case NonFatal(e) => {
        /**
            *TODO: User Proper Logger
         */
        println("Exception -  Message "+e.getMessage)
        println("Exception - Cause "+e.getCause)
        println("Exception - Stacktrace ")
        e.printStackTrace(System.out)
        None
      }
    }

  }

}
