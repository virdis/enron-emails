package com.virdis.models

import org.joda.time.DateTime

/**
  * Created by sandeep on 2/28/16.
  */
case class EnronEmail(date: DateTime,
                      sender: String,
                      recipients: Set[String] = Set.empty[String],
                      isOriginal: Boolean)
