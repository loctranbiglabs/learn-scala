name := "Simple Project"
version := "1.0"
scalaVersion := "2.10.4"
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.5.2",
	"org.apache.spark" %% "spark-sql" % "1.5.2",
	"org.postgresql" % "postgresql" % "9.4-1201-jdbc41",
	"net.liftweb" %% "lift-json" % "2.5.1"
)
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
