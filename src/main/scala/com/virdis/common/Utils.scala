package com.virdis.common

import org.joda.time.format.DateTimeFormat

import scala.collection.immutable.TreeSet
import com.virdis.models.DirectEmail
/**
  * Created by sandeep on 2/28/16.
  */
object Utils {

  val LABEL_DIRECT = "direct"
  val LABEL_BROADCAST = "broadcast"

  val dateFormatter = DateTimeFormat.forPattern("MM/dd/yyyy")

  implicit val treeOrder = new Ordering[DirectEmail](){
    def compare(x: DirectEmail, y: DirectEmail) = x.count compare y.count
  }

  def initializeTree(x: DirectEmail): TreeSet[DirectEmail] = {
   val tree = new TreeSet[DirectEmail]()(treeOrder)
    tree + x
  }

  def updateTree(x: DirectEmail, tree: TreeSet[DirectEmail]): TreeSet[DirectEmail] = {
    val de = tree.find(ele => ele.mailId == x.mailId && x.count > ele.count)
    if (de.nonEmpty) {
      val t = tree - de.get
      t + x
    } else {
      if (tree.size > 5) {
        val minDE = tree.min
        if (x.count > minDE.count) {
          val t = tree - minDE
          t + x
        } else tree
      } else {
        tree + x
      }
    }

  }

  def foldCount(s: (String, Long), r: DirectEmail) : (String, Long) = (r.mailId, s._2 + r.count)
}
