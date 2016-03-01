package com.virdis.parser

import java.io.File

import com.virdis.models.EnronEmail
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.control.NonFatal


object EmailParser {

  val EMAIL_DATE_FORMATTER = DateTimeFormat.forPattern("EEE, d MMM y H:m:s Z (z)")

  val DATE_MARKER = "Date:"
  val FROM_MARKER = "From:"
  val TO_MARKER = "To:"
  val CC_MARKER = "Cc:"
  val BCC_MARKER = "Bcc:"
  val SUBJECT_MARKER = "Subject:"
  val EMAIL_FORWARD_MARKER = "FW:"
  val EMAIL_REPLY_MARKER = "RE:"
  val MIME_VERSION_MARKER = "Mime-Version:"
  val X_FILENAME = "X-FileName:"
  val X_FROM_MARKER = "X-From:"
  val X_TO_MARKER = "X-To:"

  /**
    * Takes a path and returns list of all the files
    *
    * @param path
    * @return List[Path]
    */
  def listAllTxtFiles(path: String): List[File] = {
    val file = new File(path)
    val subDirectories = file.listFiles.filter(_.isDirectory)
    subDirectories.flatMap(files => files.listFiles.filter(f => f.getName.contains("txt"))).toList
  }

  /**
    * extracts the DateTime from the input
    *
    * @param line
    * @return
    */
  def parseDate(line: String): DateTime = {
    val date = line.split(DATE_MARKER)(1)
    EMAIL_DATE_FORMATTER.parseDateTime(date.trim)
  }

  /**
    * extracts sender email from input
    *
    * @param line
    * @return
    */
  def senderEmail(line: String): String = {
    line.split(FROM_MARKER)(1).trim
  }

  /**
    * extracts emails and return a Set
    *
    * @param e
    * @return
    */
  def extractMultipleEmails(e: String): Set[String] = {
    if (e.contains(",")) {
      // add strings that are emails
      e.split(",").foldLeft(Set.empty[String])((acc, a) => if (!a.contains("@")) acc else acc + a.trim)
    } else {
      Set(e.trim)
    }
  }


  /**
    * check presence of "," . if present we know there are multiple emails
    * extract emails, trim and add them to a set
    *
    * @param line
    * @param tag
    * @return
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
    * extracts subject from input
    *
    * We need to be case insensitive while looking for "RE" or" "FW" in the Subject line
    * so that we can Match Re or Fwd
    *
    * There are emails with empty subject line we should account for that appropriately
    * and return a empty subject
    *
    * @param line
    * @return
    */
  def subject(line: String): (String, Boolean) = {
    def extractSubject(str: String, marker: String) = {
      val res = str.toLowerCase().split(marker.toLowerCase())
      if (res.size > 1) (res(1).trim, false)
      else ("", false)
    }

    val subject = line.split(SUBJECT_MARKER)(1)

    if (subject.toLowerCase.contains(EMAIL_FORWARD_MARKER.toLowerCase)) {
      extractSubject(subject, EMAIL_FORWARD_MARKER)
    } else if (subject.toLowerCase.contains(EMAIL_REPLY_MARKER.toLowerCase)) {
      extractSubject(subject, EMAIL_REPLY_MARKER)
    } else {
      (subject.trim.toLowerCase, true)
    }
  }


  /**
    * We can find email recipients with following tags:
    * 1. To:
    * 2. Cc:
    * 3. Bcc:
    * There is a chance that email ids can be repeated in the To, Cc and the Bcc section. To over come that
    * we can get the emails by each tag and then merge them into a Set removing duplicate.
    *
    * @param to
    * @param cc
    * @param bcc
    * @return Set[String]
    */
  def mergeRecipients(to: Set[String], cc: Set[String], bcc: Set[String]): Set[String] = to ++ cc ++ bcc

  /**
    * this is to check if the input contains both
    * To: and X-To: tags also
    * From: and X-From tags
    * if the indices distance is 2 then both indices
    * are for the same tag
    *
    * @param tag
    * @param xTag
    * @return Boolean
    */
  def tagsNotSame(tag: Int, xTag: Int): Boolean = {
    !(scala.math.abs(xTag - tag) == 2)
  }

  /**
    * check if To: From: X-To: and X-From tag indices
    * and figure out if To: and From: Tags exist in the input
    *
    * @param toTag
    * @param xToTag
    * @param fromTag
    * @param xFromToTag
    * @return Boolean
    */

  def checkTagIndices(toTag: Int, xToTag: Int, fromTag: Int, xFromToTag: Int): Boolean = {
    val toFlag = tagsNotSame(toTag, xToTag)
    val fromFlag = tagsNotSame(fromTag, xFromToTag)

    toFlag && fromFlag

  }

  /**
    * Given input check if there is
    * email sender data and
    * email recipient data
    * and return the data check flag and the indices
    *
    * @param line
    * @return (Boolean, Int, Int, Int, Int)
    */

  def isSenderRecipientDataPresent(line: String): (Boolean, Int, Int, Int, Int) = {
    val from = line.indexOf(FROM_MARKER)
    val to = line.indexOf(TO_MARKER)
    val xfrom = line.indexOf(X_FROM_MARKER)
    val xto = line.indexOf(X_TO_MARKER)

    (checkTagIndices(to, xto, from, xfrom), to, xto, from, xfrom)
  }


  /**
    *
    * Assumption
    *
    * 1. If the email subject line is empty handle it appropriately
    * 2. If To: or From: tags are be missing, do not process the email.
    *
    * The idea to extract data from the string is to look for indices
    * of tags. Once you have the indices you can call substring and pull
    * out the part of the string with data in it. Then use appropriate functions
    * to parse the data.
    *
    * @param emailContent
    * @return Option[EnronEmail]
    */

  // TODO: If there is time then extract data from X-From and X-To tags

  def buildEmail(emailContent: String): Option[EnronEmail] = {

    var to: Set[String] = Set.empty[String]
    var cc: Set[String] = Set.empty[String]
    var bcc: Set[String] = Set.empty[String]
    val enronEmail = new EnronEmail()
    try {

      val (dataFlag, toTagIdx, xToTagIdx, fromTagIdx, xFromTagIdx) = isSenderRecipientDataPresent(emailContent)

      if (!dataFlag) return None

      enronEmail.createdAt = parseDate(emailContent.substring(emailContent.indexOf(DATE_MARKER),  fromTagIdx))

      enronEmail.sender = senderEmail(emailContent.substring(fromTagIdx , toTagIdx))
      to = recipientEmailsByTags(emailContent.substring(toTagIdx, emailContent.indexOf(SUBJECT_MARKER)),
        TO_MARKER)

      val idxCCTag = emailContent.indexOf(CC_MARKER)

      /**
        * if Cc: tag is present use it to extract Subject
        */
      if (idxCCTag != -1) {
        val subFlag = subject(emailContent.substring(emailContent.indexOf(SUBJECT_MARKER), idxCCTag))
        cc = recipientEmailsByTags(emailContent.substring(idxCCTag, emailContent.indexOf(MIME_VERSION_MARKER)), CC_MARKER)
        enronEmail.subject = subFlag._1
        enronEmail.isOriginal = subFlag._2
      } else {
        val subFlag = subject(emailContent.substring(emailContent.indexOf(SUBJECT_MARKER), emailContent.indexOf(MIME_VERSION_MARKER)))
        enronEmail.subject = subFlag._1
        enronEmail.isOriginal = subFlag._2
      }

      /**
        * if Bcc: tag is present use it to extract recipients
        */
      val idxBccTag = emailContent.indexOf(BCC_MARKER)
      if (idxBccTag != -1) {
        bcc = recipientEmailsByTags(emailContent.substring(idxBccTag, xFromTagIdx), BCC_MARKER)
      }
      enronEmail.recipients = mergeRecipients(to, cc, bcc)
      Option(enronEmail)

    } catch {
      case NonFatal(e) => {
        /**
          * TODO: User Proper Logger
          */
        println("Exception -  Message " + e.getMessage)
        println("Exception - Cause " + e.getCause)
        println("Exception - Stacktrace ")
        e.printStackTrace(System.out)
        None
      }
    }

  }

}
