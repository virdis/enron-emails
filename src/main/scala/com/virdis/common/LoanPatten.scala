package com.virdis.common

import util.control.Exception._

/*
    Pattern used for any resource that is suppose to be closed after usage so that
    there is no resource leakage.
 */
trait LoanPatten {

  type Closeable = { def close() }

  def using[R <: Closeable, A](resource: R)(f: R => A): A = {
    try {
      f(resource)
    } finally {
      ignoring(classOf[Throwable]) apply {
        resource.close()
      }
    }
  }
}

object fileReader extends LoanPatten