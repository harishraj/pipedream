package edu.berkeley.cs.amplab.pipedream

import net.sf.samtools.SAMRecord

import scala.collection.JavaConversions._
import scala.collection.immutable.HashSet
import scala.util.Random
import scala.math.{exp, sqrt}

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

    def toIndexedSeq: (Int, Vector) = {
        val y = if (matchesRef) 1 else 0
        val x = List(qual.toInt, cycle) ++ dinucToIndicator
        (2*y - 1, new Vector(x.map(_.toDouble).toArray))
    }
}

trait BQSRUtil {
    def min(a: Int, b: Int) = List(a, b).min
    def max(a: Int, b: Int) = List(a, b).max
}

package object BQSRFunctions {
    def computeCovariates(read: SAMRecord, ref: Broadcast[CachedReference]): List[BQSRCovariate] = {
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
                cycle.zip(ind).filter(c => c._1 >= 1).map(c =>
                    new BQSRCovariate(bases(c._2) == refBases(c._2 - readStart),
                                quals(c._2), c._1, Array(bases(c._2),bases(c._2 + a)))
                )
        }
    }
}

class BaseQualityRecalibrator(val sc: SparkContext, val ref: Broadcast[CachedReference]) extends BQSRUtil {
    val ITERATIONS = 10;

    def execute(rdd: ShortReadRDD) {
        // val dbSNPSites: BroadCast[Set[(String, Long)]] = sc.broadcast(getSegregatingSites)
        val covariates = rdd.map(BQSRFunctions.computeCovariates(_, ref)).reduce(_ ++ _).map(_.toIndexedSeq)
        var w = Vector(18, _ => Random.nextDouble)
        for (i <- 1 to ITERATIONS) {
            println("On iteration " + i)
                val gradient = covariates.map { p =>
                    (1 / (1 + exp(-p._1 * (w dot p._2))) - 1) * p._1 * p._2
            }.reduce(_ + _)
            w -= gradient
            println("|grad| = ", sqrt(gradient dot gradient))
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

