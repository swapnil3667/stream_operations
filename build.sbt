test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList(ps @ _*) if ps.last endsWith "package-info.class" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "jersey-module-version" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "log4j.properties" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "overview.html" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "plugin.xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "jboss-beans.xml" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "parquet.thrift" => MergeStrategy.first
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("javax", "transaction", xs @ _*) => MergeStrategy.first
  case PathList("javax", "xml", xs @ _*) => MergeStrategy.first
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", "common", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", "protobuf", xs @ _*) => MergeStrategy.first
  case PathList("org", "codehaus", "jackson", xs @ _*) => MergeStrategy.first
  case PathList("org", "jboss", "netty", xs @ _*) => MergeStrategy.first
  case PathList("org", "aopalliance", "aop", xs @ _*) => MergeStrategy.first
  case PathList("org", "aopalliance", "intercept", xs @ _*) => MergeStrategy.first
  case PathList("org", "slf4j", "impl", xs @ _*) => MergeStrategy.first
  case PathList("parquet", xs @ _*) => MergeStrategy.first
  case PathList("org", "spark-project", "jetty", xs @ _*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", "minlog", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard

  case PathList(ps @ _*) if ps.last contains "LogParam" => MergeStrategy.last

  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.eclipse.jetty.**" -> "org.spark-project.jetty.@1").inAll
)

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.rakuten.digital",
      scalaVersion := "2.10.4",
      version      := "0.1.0"
    )),
    name := "kinesis_stream",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0" % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0" % Provided,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.0",
    libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.10.0-M4",
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.1.0",
  //  libraryDependencies += "net.databinder.dispatch" % "dispatch-core_2.10" % "0.11.3",
    libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0",
    libraryDependencies += "com.typesafe" % "config" % "1.3.0",
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0",
 //   libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.1",
    libraryDependencies += "postgresql" % "postgresql" % "9.1-901.jdbc4",
    libraryDependencies += "com.rabbitmq" % "amqp-client" % "3.6.6",
    libraryDependencies += "org.apache.kafka" % "connect-json" % "0.9.0.0",
    libraryDependencies += "org.yaml" % "snakeyaml" % "1.11",
    libraryDependencies += "org.apache.kafka" %% "kafka" % "0.10.1.0" exclude("org.scalatest", "scalatest_2.10")
  ).
  settings(
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    crossPaths := false,
    autoScalaLibrary := false
  )