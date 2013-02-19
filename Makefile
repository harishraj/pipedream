SPARK_HOME = ../spark-0.6.1

run:
	$(SPARK_HOME)/run edu.berkeley.cs.amplab.pipedream.Pipeline local[8]

pkg:
	$(SPARK_HOME)/sbt/sbt package

test:
	$(SPARK_HOME)/sbt/sbt test

