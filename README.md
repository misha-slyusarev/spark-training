### ex3_topten.py

This script reads a folder of files and count top ten countries with most often occurrences in the files.

### ex5_meanSalary.R

Main goal of the task is to try Spark SQL in action.

In this file there is a R program that reads JSON with vacancies and calculates mean salaries for programmers, designers, and system administrators with per city breakdown.

I chose SparkR as implementation language because the task reads as a perfect match of what the language is designed for. However I found that SparkR is incomplete and inefficient for complex tasks and hence choosing it was a bad idea.

### ex7_browser

This example reads data from S3, then count number of visits per Browser per month, and save it into MySQL. This is a Scala project managed by SBT. Compile it with Assembly plugin like this:
 
    cd ex7_browser   
    sbt -verbose assembly

Then you can submit assembled jar into Spark:

    path/to/spark-submit ./target/scala-2.10/browser-usage.jar 

Lastly, I used following Spark command to submit it into a cluster:

    path/to/spark-submit --deploy-mode cluster --master spark://big-data:6066  ./target/scala-2.10/browser-usage.jar
