name := "querybuilder"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.scalikejdbc" %% "scalikejdbc"        % "3.3.2",
  "org.scalikejdbc" %% "scalikejdbc-test"   % "3.3.2"   % "test",
  "com.h2database"  %  "h2"                 % "1.4.197",
  "ch.qos.logback"  %  "logback-classic"    % "1.2.3",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.json4s" %% "json4s-native" % "3.6.3",
  "org.json4s" %% "json4s-jackson" % "3.6.3",
  "org.typelevel" %% "cats-core" % "1.5.0"
)
