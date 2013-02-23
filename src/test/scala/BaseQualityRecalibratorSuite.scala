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
    val filePath = getClass.getResource("/test.sam").getPath
    val ref = new CachedReference(new File("/data/gatk_bundle/hg19/ucsc.hg19.fasta"))
    val bqsr = new BaseQualityRecalibrator(sc, sc.broadcast(ref))
    val reads = new SAMFileReader(new File(filePath)).toList
    for (read <- reads) {
        val covs = bqsr.computeCovariates(read)
        val bases = read.getReadBases
        val n = read.getReadLength
        for (cov <- covs.sortBy(_.cycle)) {
            if (read.getReadNegativeStrandFlag) {
                assert(cov.base == bases(n - 1 - cov.cycle))
                assert(cov.dinuc == bases(n - cov.cycle))
                val baseRef = ref.getBase(read.getReferenceName, read.getReferencePositionAtReadPosition(n - cov.cycle))
                assert(cov.matchesRef == (cov.base == baseRef))
            } else {
                assert(cov.base == bases(cov.cycle))
                assert(cov.dinuc == bases(cov.cycle - 1))
                val baseRef = ref.getBase(read.getReferenceName, read.getReferencePositionAtReadPosition(cov.cycle + 1))
                assert(cov.matchesRef == (cov.base == baseRef))
            }
        }
    }
  }
}
