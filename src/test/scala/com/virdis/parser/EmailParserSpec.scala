package com.virdis.parser

import com.virdis.CommonSpecs
import org.joda.time.DateTime

class EmailParserSpec extends CommonSpecs {

  def fixture = {
    new {
      val path = "./data/testData"
      val dates = List(
        "Date: Wed, 11 Jul 2001 08:29:22 -0700 (PDT)",
        "Date: Tue, 26 Jun 2001 10:31:05 -0700 (PDT)",
        "Date: Thu, 3 Aug 2000 12:11:00 -0700 (PDT)",
        "Date: Thu, 26 Jul 2001 15:17:52 -0700 (PDT)",
        "Date: Mon, 10 Apr 2000 05:00:00 -0700 (PDT)",
        "Date: Fri, 23 Mar 2001 02:03:00 -0800 (PST)",
        "Date: Fri, 11 Aug 2000 01:59:00 -0700 (PDT)"
      )
      val senderEmail = "From: legalonline-compliance@enron.com"

      val recipientEmailsToTag = List(
        "To: rkean@starband.net",
        "To: ann.schmidt@enron.com, bryan.seyfried@enron.com, dcasse@whwg.com, ",
        "jennifer.thome@enron.com",
        "steven.kean@enron.com, susan.mara@enron.com, mike.roan@enron.com, "
      )

      val recipientEmailsCCTag = List(
        "Cc: rex04@msn.com, dkreiman@mcleodusa.net,  kean@rice.edu, ",
        "kean.philip@mcleodusa.net, skean@enron.com, dkean@starband.net",
        "Cc: a..hope@enron.com",
        "Cc: nigel.sellens@enron.com, mary.joyce@enron.com"
      )

      val recipientEmailsBCCTag = List(
        "Bcc: rex04@msn.com, dkreiman@mcleodusa.net, kat.wedig@netzero.net, kean@rice.edu, ",
        "kean.philip@mcleodusa.net, skean@enron.com, dkean@starband.net"
      )

      val subjects = List(
        "Subject: RE: Confidential Concern",
        "Subject: Re: Thank You",
        "Subject: FW: Concern",
        "Subject: Energy Issues"
      )
    }
  }

  "EmailParser" should "list all .txt files in subdirectories" in {
    val f = fixture
    val allTxtFiles = EmailParser.listAllTxtFiles(f.path)
    allTxtFiles should have length 10
  }

  "EmailParser" should "parse dates" in {
    val f = fixture
    f.dates.map(EmailParser.parseDate).foldLeft(true)((acc,a) => a.getClass == classOf[DateTime] && acc) should be (true)
  }

  "EmailParser" should "parse sender email" in {
    val f = fixture
    EmailParser.senderEmail(f.senderEmail) should be ("legalonline-compliance@enron.com")
  }

  "EmailParser" should "parse recipient emails from To Tag" in {
    val f = fixture
    f.recipientEmailsToTag.map(s => EmailParser.recipientEmailsByTags(s, EmailParser.TO_MARKER))
      .foldLeft(Set.empty[String])((acc,a) => a ++ acc) should contain theSameElementsAs Vector("rkean@starband.net",
      "ann.schmidt@enron.com","bryan.seyfried@enron.com", "dcasse@whwg.com", "jennifer.thome@enron.com",
      "steven.kean@enron.com", "susan.mara@enron.com", "mike.roan@enron.com"
    )
  }

  "EmailParser" should "parse recipient emails from CC Tag" in {
    val f = fixture
    f.recipientEmailsCCTag.map(s => EmailParser.recipientEmailsByTags(s, EmailParser.CC_MARKER))
      .foldLeft(Set.empty[String])((acc, a) => a ++ acc) should contain theSameElementsAs Vector(
      "rex04@msn.com", "dkreiman@mcleodusa.net", "kean@rice.edu",
      "kean.philip@mcleodusa.net", "skean@enron.com" ,"dkean@starband.net",
      "a..hope@enron.com",
      "nigel.sellens@enron.com", "mary.joyce@enron.com"
    )
  }

  "EmailParser" should "parse recipient emails from BCC Tag" in {
    val f = fixture
    f.recipientEmailsBCCTag.map(s => EmailParser.recipientEmailsByTags(s, EmailParser.BCC_MARKER))
      .foldLeft(Set.empty[String])((acc, a) => a ++ acc) should contain theSameElementsAs Vector(
      "rex04@msn.com", "dkreiman@mcleodusa.net", "kat.wedig@netzero.net", "kean@rice.edu",
      "kean.philip@mcleodusa.net", "skean@enron.com", "dkean@starband.net"
    )
  }

  "EmailParser" should "parse Subject" in {
    val f = fixture
    f.subjects.map(EmailParser.subject) should contain theSameElementsAs Vector(
      ("Confidential Concern".toLowerCase, false),
      ("Thank You".toLowerCase, false),
      ("Concern".toLowerCase, false),
      ("Energy Issues".toLowerCase, true)
    )
  }
}
