package com.virdis.models

import org.joda.time.DateTime
import com.virdis.common.Constants._
/**
  * Created by sandeep on 2/28/16.
  */
case class EnronEmail(date: DateTime,
                      sender: String,
                      recipients: Set[String] = Set.empty[String],
                      subject: String,
                      isOriginal: Boolean) {
  def label: String = if (recipients.size > 1) LABEL_BROADCAST else LABEL_DIRECT
}
