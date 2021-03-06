package com.virdis.models

import com.virdis.common.Utils._
import org.joda.time.DateTime



/**
  *
  * EnronEmail Domain Model map to a single Email.
  *
  * createdAt represent the day of email creation
  * sender represents the email creator
  * recipients represent the people who received the emails
  * ( Recipients can be found in the to , Cc, Bcc tags )
  * subject represents the email subject
  * isOrignal represents whether the email was original, reply or fwd
  *
  * @param createdAt
  * @param sender
  * @param recipients
  * @param subject
  * @param isOriginal
  */
class EnronEmail(
                  var createdAt: DateTime,
                  var sender: String,
                  var recipients: Set[String],
                  var subject: String,
                  var isOriginal: Boolean
                ) {

  /**
    * Default constructor - POJO requirement
    */
  def this() {
    this(new DateTime(0), "", Set.empty[String], "", false)
  }

  def label: String = if (recipients.size > 1) LABEL_BROADCAST else LABEL_DIRECT

  def isSubjectEmpty: Boolean = subject.isEmpty

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("[ Sender: " + sender)
    sb.append(", subject: " + subject)
    sb.append(", recipients: " + recipients)
    sb.append(", createdAt: " +createdAt)
    sb.append(", isOriginal: " + isOriginal)
    sb.append(" ]")
    sb.toString()
  }
}

case class DirectEmail(mailId: String, count: Long)

case class ResponseCalculator(sender: String, to: String, subject: String, timestamp: Long)