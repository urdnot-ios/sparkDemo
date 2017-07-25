name := "sparkDemo"

version := "1.0"

scalaVersion := "2.10.5"

resolvers += "T-Mobile Engineering internal" at "http://10.92.139.24:8081/nexus/content/repositories/thirdparty/"
resolvers += "T-Mobile Engineering internal - snapshots" at "http://10.92.139.24:8081/nexus/content/repositories/snapshots/"
resolvers += "Public Hortonworks Maven Repo" at "http://repo.hortonworks.com/content/groups/public/"

updateOptions := updateOptions.value.withLatestSnapshots(false)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1"excludeAll (
    ExclusionRule(organization = "org.scala-lang"),
    ExclusionRule("org.apache.commons", "commons-lang3"),
    ExclusionRule("org.slf4j", "slf4j-core"),
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("org.slf4j","slf4j-log4j12"),
    ExclusionRule("commons-codec", "commons-codec"),
    ExclusionRule("commons-logging","commons-logging"),
    ExclusionRule("org.apache.zookeeper","zookeeper"),
    ExclusionRule("org.codehaus.jackson","jackson-jaxrs")
  ),
  "org.apache.spark" %% "spark-sql" % "1.6.1"excludeAll (
    ExclusionRule("org.slf4j", "slf4j-core"),
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("org.slf4j","slf4j-log4j12"),
    ExclusionRule("commons-codec", "commons-codec")
  ),
  "org.apache.spark" %% "spark-hive" % "1.6.1"excludeAll (
    ExclusionRule("org.slf4j", "slf4j-core"),
    ExclusionRule("org.slf4j", "slf4j-api"),
    ExclusionRule("org.slf4j","slf4j-log4j12"),
    ExclusionRule("commons-codec", "commons-codec")
  ),
  "org.apache.logging.log4j" % "log4j-core" % "2.7",
  "org.apache.logging.log4j" % "log4j-api" % "2.7",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "junit" % "junit" % "4.11" % "test"
)
