package edu.berkeley.cs.amplab.pipedream

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.tools.nsc.interpreter.{ILoop, SimpleReader}
import scala.tools.nsc.Settings

import spark._
import SparkContext._
import spark.broadcast.Broadcast
import spark.util.Vector

import java.io.File
import java.util.ArrayList

import net.sf.samtools.{SAMRecord, BAMRecord}

import net.sf.picard.reference.IndexedFastaSequenceFile

import fi.tkk.ics.hadoop.bam.SAMRecordWritable

import org.apache.hadoop.io.LongWritable

import org.broadinstitute.sting.utils.GenomeLocParser
import org.broadinstitute.sting.utils.SampleUtils
import org.broadinstitute.sting.utils.sam.GATKSAMRecord;
import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import org.broadinstitute.sting.gatk.iterators.LocusIteratorByState
import org.broadinstitute.sting.gatk.ReadProperties
import org.broadinstitute.sting.gatk.datasources.reference.ReferenceDataSource
import org.broadinstitute.sting.gatk.filters.{UnmappedReadFilter, BadCigarFilter}
import org.broadinstitute.sting.gatk.examples.GATKPaperGenotyper

import ShortReadRDDUtils._

object Pipeline {
    var bglp:Broadcast[GenomeLocParser] = null;
    var bhg19:Broadcast[CachedReference] = null;

    def main(args: Array[String]) {
        System.setProperty("spark.serializer", "spark.KryoSerializer")
        System.setProperty("spark.kryo.registrator", "edu.berkeley.cs.amplab.pipedream.Registrator")
        val sc = new SparkContext(args(0), "SparkContext");

        // Create a shared reference and copy to nodes
        val hg19 = new File("/data/gatk_bundle/hg19/ucsc.hg19.fasta");
        val rds = new ReferenceDataSource(hg19);
        val genomeLocParser = new GenomeLocParser(rds.getReference());
        bhg19 = sc.broadcast(new CachedReference(hg19))
        bglp = sc.broadcast(genomeLocParser)

        val rdd = shortReadRDDfromBam("/hadoop/3/platinum_genomes/merged.bam", sc)
        // val pileups = rdd.mapPartitions(p => readsToPileup(p, bglp, bhg19)).cache()
        // val totalBases = pileups.map(lpu => lpu.bases.length).sum
        // println(totalBases)

        val weightedCovs = rdd.flatMap(BaseQualityRecalibrator.computeCovariates(_, bhg19)).countByValue()
        val pErr = BaseQualityRecalibrator.computeProbErr(weightedCov)
        println(pErr)

        // Step 1
        // Recalibrate base scores
        // val bqsr = new BaseQualityRecalibrator(sc, bhg19)
        // bqsr.execute(rdd)
        
        // Call us some SNPs!
        // val calls = litePileups.map(simpleSNPCaller).collect()
    }
        
    def readsToPileup(reads: Iterator[SAMRecord],
                      genomeLocParser: Broadcast[GenomeLocParser],
                      reference: Broadcast[CachedReference]): Iterator[LightweightPileup] = {
        // Minimal ReadProperties object needed to satisfy the locus iterator
        val readInfo = new ReadProperties(null, null, null, true, null, null, null, null, null, true, 1);
        val unmappedFilter = new UnmappedReadFilter;
        val badCigarFilter = new BadCigarFilter;
        val mappedReads = reads.withFilter(
            r => unmappedFilter.filterOut(r) || badCigarFilter.filterOut(r)
        )
        val locusIter = new LocusIteratorByState(mappedReads, readInfo, 
            genomeLocParser.value, Set("NA12878", "NA12877", "NA12882"));
        for (ctx <- locusIter.iterator) yield LightweightPileupFactory.create(ctx, reference.value)
    }
}
