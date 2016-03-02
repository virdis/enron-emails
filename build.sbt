resolvers in ThisBuild ++= Seq("Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal)

name := "enron-emails"

version := "0.1"

organization := "com.virdis"

scalaVersion in ThisBuild := "2.11.7"

val flinkVersion = "0.10.2"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies  ++ Seq(
      "org.scalatest" % "scalatest_2.11" % "2.2.6" % "test",
      "org.apache.flink" % "flink-contrib-parent_2.11" % "0.10.2",
      "de.javakaffee" % "kryo-serializers" % "0.37"
    )
  )

mainClass in assembly := Some("com.virdis.jobs.EmailCount")

assemblyJarName in assembly := "enron.jar"

// make run command include the provided dependencies
run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run))

// exclude Scala library from assembly
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)