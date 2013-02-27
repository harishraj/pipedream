package edu.berkeley.cs.amplab.pipedream

import net.sf.samtools.SAMRecord

import scala.collection.JavaConversions._

import spark.{SparkContext, RDD}
import spark.broadcast.Broadcast
import spark.util.Vector

import ShortReadRDDUtils._

class BQSRCovariate(val matchesRef: Boolean, val qual: Byte, val cycle: Int, 
    val dinuc: Array[Byte]) {
    override def toString: String = {
        "matches:%b\tqual:%c\tcycle:%d\tdinuc:%s".format(matchesRef, qual, cycle, dinuc)
    }

    private def dinucToIndicator: IndexedSeq[Int] = {
        implicit def bool2int(b:Boolean) = if (b) 1 else 0
        val bases = "ACTG"
        for (b1 <- bases; b2 <- bases) yield (dinuc == Array(b1,b2)):Int
    }    

    def toData: (Int, Vector) = {
        val y = if (matchesRef) 1 else 0
        val x = List(qual.toInt, cycle) ++ dinucToIndicator
        (2*y - 1, new Vector(x.map(_.toDouble).toArray))
    }
}

package object BaseQualityScoreRecalibrator {
    // (Observed quality, Cycle, Base, Prev. Base)
    type BQSRCovariate = (Byte, Int, Byte, Byte)
    type BQSRObservation = (Boolean, BQSRCovariate)
    type BQSRObservations = Seq[BQSRObservation]
    type ErrorTable = Map[BQSRCovariate, Double]

    private def getWithDefault[K,V](m: Map[K,V], key: K, default: V): V = {
        if (m.contains(key)) m(key) else default
    }

    def computeProbErr(counts: Map[BQSRObservation, Long]): ErrorTable = {
        counts.keys.map(_._2).toList.distinct.map { k =>
                def gdc(b: Boolean): Long = { getWithDefault(counts, (b, k), 0) }
                (k -> (gdc(false) / (gdc(true) + gdc(false))).toDouble)
        }.toMap
    }

    def computeCovariates(read: SAMRecord, ref: Broadcast[CachedReference]): BQSRObservations = {
        val contig = read.getReferenceName
        val n = read.getReadLength
        val bases = read.getReadBases
        val quals = read.getBaseQualities
        read.getAlignmentBlocks.toList.flatMap{block => 
            val blen = block.getLength
            val readStart = block.getReadStart - 1
            val ind = readStart until (readStart + blen)
            val cycle = if (read.getReadNegativeStrandFlag)
                ind.map(n - 1 - _)
            else
                ind
            val a = if (read.getReadNegativeStrandFlag)
                1
            else
                -1
            val refBases = ref.value.getBases(contig, block.getReferenceStart, blen)
            cycle.zip(ind).filter(c => c._1 >= 1).map(
                c => (bases(c._2) == refBases(c._2 - readStart),
                        (quals(c._2), c._1, bases(c._2), bases(c._2 + a)))
            )
        }
    }
    def getSegregatingSites(sc: SparkContext): Set[(String, Long)] = {
        val dbSNP = sc.textFile("/data/gatk_bundle/hg19/dbsnp_137.hg19.vcf")
        dbSNP.filter(! _.startsWith("#")).map(_.split("\t").slice(0,2)).map(
            x => (x(0), x(1).toLong)
        ).collect().toSet
    }
    // "Destructively" recalibrate the reads
    def recalibrateRead(read: SAMRecord, errorTable: ErrorTable) { 

    }
}

