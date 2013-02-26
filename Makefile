SPARK_HOME = ../spark-0.6.1

run:
	$(SPARK_HOME)/run -Dspark.serializer=spark.KryoSerializer edu.berkeley.cs.amplab.pipedream.Pipeline local[8] 
pkg:
	sbt/sbt package

test:
	sbt/sbt test

