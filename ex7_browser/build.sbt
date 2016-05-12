lazy val root = (project in file(".")).
  settings(
    name := "Browser Usage",

    version := "0.1",
    scalaVersion := "2.10.4",

    jarName in assembly := "browser-usage.jar",
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),

    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
	  
      "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
      "com.amazonaws" % "aws-java-sdk-s3" % "1.9.37",
      "mysql" % "mysql-connector-java" % "5.1.33",
      
      "org.apache.hadoop" % "hadoop-aws" % "2.6.0"
        exclude("com.amazonaws", "aws-java-sdk") 
        exclude ("javax.servlet", "servlet-api") 
        exclude("commons-beanutils", "commons-beanutils")
        exclude("commons-beanutils", "commons-beanutils-core"),
      
      "eu.bitwalker" % "UserAgentUtils" % "1.19"
    )
  )
