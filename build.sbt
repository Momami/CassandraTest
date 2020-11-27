name := "CassandraProject"

version := "0.1"

scalaVersion := "2.12.11"

// https://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-core
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.10.2"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "0.17"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.18"
libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-google-cloud-pub-sub" % "0.18"
//libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % "2.6.10"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.32"