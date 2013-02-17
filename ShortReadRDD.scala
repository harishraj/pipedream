package edu.berkeley.cs.amplab.pipedream

import spark.{SparkContext, RDD}

import org.apache.hadoop.io.LongWritable

import fi.tkk.ics.hadoop.bam.BAMInputFormat
import fi.tkk.ics.hadoop.bam.BAMRecordReader
import fi.tkk.ics.hadoop.bam.SAMRecordWritable
import fi.tkk.ics.hadoop.bam.FileVirtualSplit

object ShortReadRDD {
    def fromBam(bamFile: String, sc: SparkContext) : RDD[(LongWritable, SAMRecordWritable)] = {
      return sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](bamFile);
    }
}


