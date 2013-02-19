package edu.berkeley.cs.amplab.pipedream

import spark.{SparkContext, RDD}

import org.apache.hadoop.io.LongWritable

import net.sf.samtools.{SAMRecord, BAMRecord}

import fi.tkk.ics.hadoop.bam.BAMInputFormat
import fi.tkk.ics.hadoop.bam.BAMRecordReader
import fi.tkk.ics.hadoop.bam.SAMRecordWritable
import fi.tkk.ics.hadoop.bam.FileVirtualSplit

package object ShortReadRDDUtils {
    type ShortReadRDD = RDD[SAMRecord];
    def shortReadRDDfromBam(bamFile: String, sc: SparkContext) : ShortReadRDD = {
      return sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](bamFile).map(r => r._2.get)
    }
}

