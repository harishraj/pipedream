# User parameters
BAM_FILE = /scratch/terhorst/merged.chr20.bam
REFERENCE_FASTA = /scratch/hg19/hg19.fa
THREADS = 16

BASEDIR := $(PWD)
SBT := $(BASEDIR)/sbt/sbt

export SCALA_HOME := $(BASEDIR)/vendor/scala
export SPARK_HOME := $(BASEDIR)/vendor/spark
export SPARK_CLASSPATH := $(BASEDIR)/target/scala-2.9.2/classes:$(BASEDIR)/lib/*
export SPARK_MEM := 96g

compile-deps: compile-gatk compile-scala-kryo compile-scala compile-spark
	
compile-gatk:
	ant -f $(BASEDIR)/vendor/gatk/build.xml dist
compile-scala-kryo:
	cd $(BASEDIR)/vendor/scala-kryo-serialization
	$(SBT) compile publish-local
compile-scala:
	ant -f $(BASEDIR)/vendor/scala/build.xml
compile-spark:
	cd $(BASEDIR)/vendor/spark
	sbt/sbt package

run:
	$(SPARK_HOME)/run edu.berkeley.cs.amplab.pipedream.Pipeline local[$(THREADS)] $(BAM_FILE) $(REFERENCE_FASTA)
compile:
	sbt/sbt compile
package:
	sbt/sbt package
test:
	sbt/sbt test
