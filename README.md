# spk-airplane

Examining the Airplane Dataset using Apache Spark.

## Instructions
* Copy src/resources/application.conf.template to src/resources/application.conf
* Change the value of `root` to be the `/path/to/the/data/folder/`.
* Run `sbt assembly`.
* From the Spark installation directory, run `bin/spark-submit --master local[*]
/path/to/repo/spk-airplane/target/scala-2.10/spk-airplane-assembly-1.0.jar`

## References
*
*[Data Source](http://stat-computing.org/dataexpo/2009/the-data.html)