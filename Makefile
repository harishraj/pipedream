SPARK_HOME = ../spark-0.6.1

run:
	$(SPARK_HOME)/run edu.berkeley.cs.amplab.scatk.Pipeline local[4]

pkg:
	$(SPARK_HOME)/sbt/sbt package
