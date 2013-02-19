package edu.berkeley.cs.amplab.pipedream

import net.sf.samtools.SAMRecord

import scala.collection.JavaConversions._
import scala.collection.immutable.HashSet

import spark.{SparkContext, RDD}
import spark.broadcast.Broadcast

import ShortReadRDDUtils._

class BQSRCovariate(val matchesRef: Boolean, 
    val qual: Char, val cycle: Int, 
    val base: Char, val dinuc: Char) {

    override def toString: String = {
        "%b\t%c\t%d\t%c\t%c".format(matchesRef, qual, cycle, base, dinuc)
    }
}

class BaseQualityRecalibrator(val sc: SparkContext, 
    val readsRDD: ShortReadRDD, val ref: Broadcast[CachedReference]) {
    var dbSNPSites: Set[(String, Long)] = null;

    def execute() {
        dbSNPSites = getSegregatingSites
        val covariates = readsRDD.map(computeCovariates).reduce(_ ++ _)
    }

    def computeCovariates(read: SAMRecord): List[BQSRCovariate] = {
        val contig = read.getReferenceName
        read.getAlignmentBlocks.toList.flatMap{block => 
            val blen = block.getLength
            // This is 1-based
            val readStart = block.getReadStart - 1
            val refBases = ref.value.getBases(contig, block.getReferenceStart, blen)
            val readBases = read.getReadBases.map(_.toChar).slice(readStart, readStart + blen)
            val quals = read.getBaseQualities.map(_.toChar).slice(readStart, readStart + blen)
            var cycle = (readStart to readStart + blen - 1).toList
            // X means: ignore
            var dinuc = List('X') ++ read.getReadBases.map(_.toChar).slice(readStart - 1, readStart + blen)
            if (read.getReadNegativeStrandFlag) {
                cycle = cycle.reverse
                dinuc = dinuc.reverse
            }
            val refRead = refBases.zip(readBases)
            val covs = (quals, cycle, dinuc).zipped.toList
            refRead.zip(covs).withFilter {
                case ((refBase, readBase), (qual, cycle, dinuc)) => dinuc != 'X'
            }.map{
                case ((refBase, readBase), (qual, cycle, dinuc)) => 
                    new BQSRCovariate(refBase != readBase, qual, cycle, readBase, dinuc)
            }
        }
    }

    // Return set of all segregating sites in dbSNP
    def getSegregatingSites(): Set[(String, Long)] = {
        val dbSNP = sc.textFile("/data/gatk_bundle/hg19/dbsnp_137.hg19.vcf")
        new HashSet[(String, Long)] ++ dbSNP.filter(! _.startsWith("#")).map(_.split("\t").slice(0,2)).map(
            x => (x(0), x(1).toLong)
        ).collect()
    }
}

