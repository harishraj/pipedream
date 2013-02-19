package edu.berkeley.cs.amplab.pipedream

import scala.collection.JavaConversions._
import scala.io.Source

import java.io.File

import net.sf.samtools.{SAMFileReader, SAMRecord}

import spark.{RDD, SparkContext}

import ShortReadRDDUtils._

import org.scalatest.BeforeAndAfter

class BaseQualityRecalibratorSuite extends SparkTestUtils with BeforeAndAfter {
  sparkTest("covariate generation") {
    // Remove prepended 'file:'
    val filePath = getClass.getResource("/test.sam").getPath
    val ref = new CachedReference(new File("/data/gatk_bundle/hg19/ucsc.hg19.fasta"))
    val bqsr = new BaseQualityRecalibrator(sc, sc.broadcast(ref))
    val reads = new SAMFileReader(new File(filePath)).toList
    for (read <- reads) {
        println(read.getSAMString)
        val covs = bqsr.computeCovariates(read)
        for (cov <- covs.sortBy(_.cycle)) {
            println(cov)
            val n = read.getReadLength
            if (read.getMateNegativeStrandFlag) {
                assert(cov.base == read.getReadBases()(n - cov.cycle).toChar)
                assert(cov.dinuc == read.getReadBases()(n - cov.cycle - 1).toChar)
            } else {
                assert(cov.base == read.getReadBases()(cov.cycle - 1).toChar)
                assert(cov.dinuc == read.getReadBases()(cov.cycle - 2).toChar)
            }
        }
    }
  }
}
