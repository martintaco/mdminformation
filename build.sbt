name := "MDMinformation"

version := "0.1"

scalaVersion := "2.11.8"

organization := "Belcorp"

resolvers ++= Seq(
  "Redshift" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release",
  "jitpack.io" at "https://jitpack.io",
  "Artima Maven Repository" at "http://repo.artima.com/releases"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.443"
/*libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided"
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.443" % "provided"*/
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.1"

libraryDependencies += "com.github.databricks" % "spark-redshift" % "8adfe95a25"
libraryDependencies += "org.rogach" %% "scallop" % "3.1.2"

libraryDependencies += "com.zaxxer" % "HikariCP" % "3.1.0"
libraryDependencies += "com.amazon.redshift" % "redshift-jdbc42" % "1.2.15.1025"
libraryDependencies += "org.jooq" % "jooq" % "3.11.4"
libraryDependencies += "org.jooq" % "jooq-meta" % "3.11.4"

libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.7"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5" % "test"


dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.9"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.9"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @_*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

test in assembly := {}