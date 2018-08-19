name := "akka-playground"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies += "com.typesafe.akka" %% "akka-stream"         % "2.5.14"
libraryDependencies += "com.typesafe.akka" %% "akka-actor"          % "2.5.14"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit"        % "2.5.14"  % Test
libraryDependencies += "com.typesafe.akka" %% "akka-stream-testkit" % "2.5.14"  % Test
libraryDependencies += "org.scalatest"     %% "scalatest"           % "3.0.5"   % Test

