/*** SimpleJob.scala ***/
import spark.{SparkContext, RDD}
import spark.api.java.JavaRDD
import SparkContext._

import org.apache.hadoop.io.LongWritable

import fi.tkk.ics.hadoop.bam.BAMInputFormat
import fi.tkk.ics.hadoop.bam.BAMRecordReader
import fi.tkk.ics.hadoop.bam.SAMRecordWritable
import fi.tkk.ics.hadoop.bam.FileVirtualSplit

import net.sf.samtools.SAMRecord
import net.sf.samtools.SAMTextHeaderCodec
import net.sf.samtools.SAMReadGroupRecord
import net.sf.samtools.SAMProgramRecord
import net.sf.samtools.SAMFileReader.ValidationStringency

package edu.berkeley.cs.amplab.scatk {
  object ShortReadRDD {
    def fromBam(bamFile: String, sc: SparkContext) : RDD[(LongWritable, SAMRecordWritable)] = {
      return sc.newAPIHadoopFile[LongWritable, SAMRecordWritable, BAMInputFormat](bamFile);
    }
  }
}


