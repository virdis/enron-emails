package com.virdis.parser

import java.io.File

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


object EmailParser {

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

  /*
    check presence of "," . if present we know there are multiple emails
    extract emails, trim and add them to a set
  */
  def recipientEmailsByTags(line: String, tag: String): Set[String] = {
    if (line.contains(tag)) {
      val email = line.split(tag)(1)
      extractMultipleEmails(email)
    } else {
      extractMultipleEmails(line)
    }
  }


  /*
      We need to be case insensitive while looking for "RE" or" "FW" in the Subject line
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


}
