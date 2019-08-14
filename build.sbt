
name := "CapstoneDEND"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided",
  "org.xerial" % "sqlite-jdbc" % "3.28.0",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3" excludeAll ExclusionRule(organization = "javax.servlet")
)

// set custom mergeStrategy to avoid merge conflicts when importing hadoop-aws jar
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}