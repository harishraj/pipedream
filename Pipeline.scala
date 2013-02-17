package edu.berkeley.cs.amplab.scatk

import scala.collection.JavaConversions._
import scala.collection.mutable

import spark._
import SparkContext._
import spark.broadcast.Broadcast

import java.io.File
import java.util.ArrayList

import net.sf.samtools.{SAMRecord, BAMRecord}
import org.apache.hadoop.io.LongWritable
import fi.tkk.ics.hadoop.bam.SAMRecordWritable

import org.broadinstitute.sting.utils.GenomeLocParser
import org.broadinstitute.sting.utils.SampleUtils
import org.broadinstitute.sting.utils.sam.GATKSAMRecord;
import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import org.broadinstitute.sting.gatk.iterators.LocusIteratorByState
import org.broadinstitute.sting.gatk.ReadProperties
import org.broadinstitute.sting.gatk.datasources.reference.ReferenceDataSource;
import org.broadinstitute.sting.gatk.filters.{UnmappedReadFilter, BadCigarFilter};
import org.broadinstitute.sting.gatk.examples.GATKPaperGenotyper;

object Pipeline {
    var bglp:Broadcast[GenomeLocParser] = null;
    def main(args: Array[String]) {
        val sc = new SparkContext(args(0), "SparkContext");
        // Create a shared reference and copy to nodes
        val hg19 = new File("/data/gatk_bundle/hg19/ucsc.hg19.fasta");
        val referenceDataSource = new ReferenceDataSource(hg19);
        val genomeLocParser = new GenomeLocParser(referenceDataSource.getReference());
        bglp = sc.broadcast(genomeLocParser)

        val rdd = ShortReadRDD.fromBam("data/chrM.bam", sc);
        val pileups = rdd.mapPartitions((p) => readsToPileup(p, bglp))
        val litePileups = pileups.map(ctx => LightweightPileupFactory.create(ctx)).cache()
        val totalBases = litePileups.map(lpu => lpu.bases.length).sum
        println(totalBases)
        
        // Call us some SNPs!
        // val calls = litePileups.map(simpleSNPCaller).collect()
    }
        
    def simpleSNPCaller(pileup: LightweightPileup): String = {
        val bases = pileup.bases
        val quals = pileup.quals;
        val baseMap = mutable.Map('A' -> 0, 'C' -> 0, 'G' -> 0, 'T' -> 0);
        for (b <- bases.filter(baseMap.contains(_)))
            baseMap(b) += 1;
        val topTwo = baseMap.toList.sortWith((a,b) => a._2 > b._2);
        val k = topTwo(0)._2 + topTwo(1)._2
        if (topTwo(0)._2 / k >= .85)
            topTwo(0)._1.toString
        else if (topTwo(1)._2 / k >= .85)
            topTwo(1)._1.toString
        else
            topTwo(0)._1.toString + "/" + topTwo(1)._1.toString
    }

    // def samToGATK = (read: SAMRecord) => new GATKSAMRecord(read);
    // def samToGATK(read: SAMRecord): GATKSAMRecord = {
    //    val sr = new GATKSAMRecord(read);
    //    sr.setCigar(read.getCigar());
    //    sr.setReadBases(read.getReadBases()); 
    //    return sr;
    // }

    def readsToPileup(reads: Iterator[(LongWritable, SAMRecordWritable)],
                      genomeLocParser: Broadcast[GenomeLocParser]): LocusIteratorByState = {
        // Minimal ReadProperties object needed to satisfy the locus iterator
        val readInfo = new ReadProperties(null, null, null, true, null, null, null, null, null, true, 1);
        val unmappedFilter = new UnmappedReadFilter;
        val badCigarFilter = new BadCigarFilter;
        val mappedReads = reads.map(r => r._2.get()).filterNot(unmappedFilter.filterOut).filterNot(badCigarFilter.filterOut);
        new LocusIteratorByState(mappedReads, readInfo, genomeLocParser.value, Set("NA12878", "NA12877", "NA12882"));
    }
}
