name := "akka-playground"

version := "0.1"

scalaVersion := "2.13.0"

val akkaVersion        = "2.5.26"

libraryDependencies += "com.typesafe.akka" %% "akka-stream"         % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor"          % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-testkit"        % akkaVersion  % Test
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion  % Test
libraryDependencies += "org.scalatest"     %% "scalatest"           % "3.0.8"      % Test