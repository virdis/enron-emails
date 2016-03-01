package com.virdis.models

import com.virdis.common.Constants._



/**
  *
  * EnronEmail Domain Model map to a single Email.
  *
  * day represent the day of email creation
  * timestamp represent the unix time of email creation
  * sender represents the email creator
  * recipients represent the people who received the emails
  * ( Recipients can be found in the to , Cc, Bcc tags )
  * subject represents the email subject
  * isOrignal represents whether the email was original, reply or fwd
  *
  *
  * @param day
  * @param timeStamp
  * @param sender
  * @param recipients
  * @param subject
  * @param isOriginal
  */
class EnronEmail(
                  var day: Int,
                  var timeStamp: Long,
                  var sender: String,
                  var recipients: Set[String],
                  var subject: String,
                  var isOriginal: Boolean) {

  /**
    * Default constructor - POJO requirement
    */
  def this() {
    this(0, 0L, "", Set.empty[String], "", false)
  }

  def label: String = if (recipients.size > 1) LABEL_BROADCAST else LABEL_DIRECT

  def isSubjectEmpty: Boolean = subject.isEmpty

  override def toString: String = {
    val sb: StringBuilder = new StringBuilder
    sb.append("[ Sender: " + sender)
    sb.append(", subject: " + subject)
    sb.append(", recipients: " + recipients)
    sb.append(", day: " + day)
    sb.append(", isOriginal: " + isOriginal)
    sb.append(" ]")
    sb.toString()
  }
}
