#!/bin/sh
rsync -r mantogni@iccluster051.iccluster.epfl.ch:~/logs/ logs
/Library/Java/JavaVirtualMachines/jdk1.8.0_60.jdk/Contents/Home/bin/java -cp /usr/local/Cellar/apache-spark/1.6.0/libexec/conf/:/usr/local/Cellar/apache-spark/1.6.0/libexec/lib/spark-assembly-1.6.0-hadoop2.6.0.jar:/usr/local/Cellar/apache-spark/1.6.0/libexec/lib/datanucleus-api-jdo-3.2.6.jar:/usr/local/Cellar/apache-spark/1.6.0/libexec/lib/datanucleus-core-3.2.10.jar:/usr/local/Cellar/apache-spark/1.6.0/libexec/lib/datanucleus-rdbms-3.2.9.jar -Xms1g -Xmx1g org.apache.spark.deploy.history.HistoryServer logs
