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
        "Subject: Energy Issues",
        "Subject: Re:",
        "Subject: FW:"
      )

      val sampleEmailOne =
        """
          |Message-ID: <22981892.1075860405119.JavaMail.evans@thyme>
          |Date: Fri, 23 Mar 2001 02:03:00 -0800 (PST)
          |From: miyung.buster@enron.com
          |To: ann.schmidt@enron.com, bryan.seyfried@enron.com, dcasse@whwg.com,
          |	dg27@pacbell.net, elizabeth.linnell@enron.com, filuntz@aol.com,
          |	james.steffes@enron.com, janet.butler@enron.com,
          |	jeannie.mandelker@enron.com, jeff.dasovich@enron.com,
          |	joe.hartsoe@enron.com, john.neslage@enron.com,
          |	john.sherriff@enron.com, joseph.alamo@enron.com,
          |	karen.denne@enron.com, lysa.akin@enron.com,
          |	margaret.carson@enron.com, mark.palmer@enron.com,
          |	mark.schroeder@enron.com, markus.fiala@enron.com,
          |	mary.hain@enron.com, michael.brown@enron.com, mike.dahlke@enron.com,
          |	mona.petrochko@enron.com, nicholas.o'day@enron.com,
          |	paul.kaufman@enron.com, peggy.mahoney@enron.com,
          |	peter.styles@enron.com, richard.shapiro@enron.com,
          |	rob.bradley@enron.com, sandra.mccubbin@enron.com,
          |	shelley.corman@enron.com, stella.chan@enron.com,
          |	steven.kean@enron.com, susan.mara@enron.com, mike.roan@enron.com,
          |	alex.parsons@enron.com, andrew.morrison@enron.com, lipsen@cisco.com,
          |	janel.guerrero@enron.com, shirley.hudler@enron.com,
          |	kathleen.sullivan@enron.com, tom.briggs@enron.com,
          |	linda.robertson@enron.com, lora.sullivan@enron.com,
          |	jennifer.thome@enron.com
          |Subject: Energy Issues
          |Mime-Version: 1.0
          |Content-Type: text/plain; charset=ANSI_X3.4-1968
          |Content-Transfer-Encoding: quoted-printable
          |X-From: Miyung Buster
        """.stripMargin

      val data1 =
        """
          |Message-ID: <19472575.1075846142367.JavaMail.evans@thyme>
          |Date: Fri, 24 Oct 1997 07:00:00 -0700 (PDT)
          |From: steven.kean@enron.com
          |Subject:  Meet with Choice Team team Diane, Mark, Luke, Vicki, Harry, Jane,
          | Klauberg, Rick, Jim, Robin Ross, Jay Flaherty, Dan Clearfield, Amy Leader
          | in 6C2
          |Mime-Version: 1.0
          |Content-Type: text/plain; charset=us-ascii
          |Content-Transfer-Encoding: 7bit
          |X-From: Steven J Kean
          |X-To:
          |X-cc:
          |X-bcc:
          |X-Folder: \Steven_Kean_Dec2000_1\Notes Folders\All documents
          |X-Origin: KEAN-S
          |X-FileName: skean.nsf
          |
        """.stripMargin
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

  "EmailParser" should "parse Subject and handle empty Subject Line" in {
    val f = fixture
    f.subjects.map(EmailParser.subject) should contain theSameElementsAs Vector(
      ("Confidential Concern".toLowerCase, false),
      ("Thank You".toLowerCase, false),
      ("Concern".toLowerCase, false),
      ("Energy Issues".toLowerCase, true),
      ("", false),
      ("", false)
    )
  }

  "EmailParser" should "build EnronEmail" in {
    val f = fixture
    val enm = EmailParser.buildEmail(f.sampleEmailOne).get
    enm.subject should be ("Energy Issues".toLowerCase())
    enm.recipients.size should be (46)
    enm.sender should be ("miyung.buster@enron.com")
  }

  "EmailParser" should "check if sender/recipient data exist" in {
    val f = fixture
    EmailParser.isSenderRecipientDataPresent(f.data1)._1 should be (false)
    EmailParser.isSenderRecipientDataPresent(f.sampleEmailOne)._1 should be (true)
  }
}
