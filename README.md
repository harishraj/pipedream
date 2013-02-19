Installation
============
1. Acquire the latest version of the GATK and then:
        $ ln -s $GATK_HOME/dist/*.jar lib/
2. Edit Makefile to point to Spark.
3. make pkg && make run
