package edu.berkeley.cs.amplab.pipedream

import spark.{SparkContext, RDD}

class BaseQualityRecalibrator(val sc: SparkContext, val readsRDD: RDD, val ref: CachedReference) {
    var dbSNPSites: Set[(String, Log)] = null;
    
    private class Covariate(val contig: String, val pos: Long, 
        val match: Bool, val cycle: Int, val dinuc: Char);

    def initializeRecalibrator() {
        dbSNPSites = new Set(getSegregatingSites);
        val covariates = readsRDD.map(computeCovariates).filterNot(
    }

    def computeCovariates(read: SAMRecord) {
        val readString = read.getReadString.toArray
        val qString = read.getBaseQualityString.toArray
        val contig = read.getReferenceName
        val pos = read.getPosition
        val refSeqPos = readString.zipWithIndex(
            (r,i) => read.getReferencePositionAtReadPosition(i)
        )
        val readRef = ref.getBasesForRead(read)
        val n = read.getReadLength
        if (read.getReadNegativeStrandFlag) {
            val cycle = readString.zipWithIndex((r,i) => n - i)
            val dinuc = readString.reverse
        } else {
            val cycle = readString.zipWithIndex((r,i) => i)
            val dinuc = readString
        }
        dinuc = Array("N") ++ dinuc.takeRight(n - 1)
        zip(readString, refSeqPos, cycle, dinuc).takeRight(n - 1).filterNot
            (base, pos, cycle, dinuc) => (pos == 0) || dbSNPSites.contains (contig, pos)
        ).map(
            (base, pos, cycle, dinuc) => 
                new Covariate(contig, pos, base != ref.getBase(contig, p), cycle, dinuc)
        )
    }

    // Return set of all segregating sites in dbSNP
    def getSegregatingSites() {
        val dbSNP = sc.textFile("/data/gatk_bundle/hg19/dbsnp_137.hg19.vcf")
        dbSNP.filter(! _.startsWith("#")).map(_.split("\t").slice(1,3).map(
            (contig, pos) => (contig, pos.toLong)
        )
    }

}
