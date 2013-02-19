package edu.berkeley.cs.amplab.pipedream

import org.scalatest.FunSuite
import java.io.File
import net.sf.samtools.{SAMFileReader, SAMRecord}
import spark.RDD

import ShortReadRDDUtils._


class BaseQualityRecalibratorSuite extends FunSuite with LocalSparkContext {    
  val sc: SparkContext = _
  val readsRDD: ShortReadRDD = _
  var ref: CachedReference

  before {
    sc = new SparkContext("local", "test")
    readsRDD = shortReadRDDfromBam('data/test.sam', sc)
    ref = new CachedReference(new File('/data/gatk_bundle/hg19/ucsc.hg19.fasta'))
  }

  test("covariate generation") {
    val bqsr = new BaseQualityRecalibrator(sc, readsRDD, sc.broadcast(ref))
    val sr = readsRDD.take(1)
    val covs = bqsr.computeCovariates(sr) 
    for (cov <- covs) {
        val n = sr.getReadLength
        if (sr.getMateNegativeStrandFlag) {
            assert(cov.base == sr.getReadBases()(n - cov.cycle).toChar)
            assert(cov.dinuc == sr.getReadBases()(n - cov.cycle - 1).toChar)
        } else {
            assert(cov.base == sr.getReadBases()(cov.cycle - 1).toChar)
            assert(cov.dinuc == sr.getReadBases()(cov.cycle - 2).toChar)
        }
    }
  }
}
