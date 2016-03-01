package com.virdis.models

import com.virdis.common.Constants._

 class EnronEmail(
                   var day: Int,
                   var timeStamp : Long,
                   var sender: String,
                   var recipients: Set[String],
                   var subject: String,
                   var isOriginal: Boolean ) {

   def this() {
     this(0, 0L, "", Set.empty[String], "", false)
   }

   def label: String = if (recipients.size > 1) LABEL_BROADCAST else LABEL_DIRECT
}
