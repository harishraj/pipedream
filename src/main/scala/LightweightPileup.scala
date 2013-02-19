package edu.berkeley.cs.amplab.pipedream

import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import net.sf.picard.reference.{IndexedFastaSequenceFile, ReferenceSequence}

class LightweightPileup(val contig:String, val position:Long,
    val refBase:Char, val refMatch:Int, val bases:Array[Char], 
    val quals:Array[Char]) {

    def possibleGenotype(): String = {
        val baseMap = collection.mutable.Map('A' -> 0, 'C' -> 0, 'G' -> 0, 'T' -> 0);
        baseMap(refBase) = refMatch
        for (b <- bases.filter(baseMap.contains(_)))
            baseMap(b) += 1;
        val topTwo = baseMap.toList.sortWith((a,b) => a._2 > b._2);
        val k = topTwo(0)._2 + topTwo(1)._2
        // Naive threshold-based SNP caller.
        if (topTwo(0)._2 / k >= .85)
            topTwo(0)._1.toString
        else if (topTwo(1)._2 / k >= .85)
            topTwo(1)._1.toString
        else
            topTwo(0)._1.toString + "/" + topTwo(1)._1.toString
    }
    var guessedGenotype:String = possibleGenotype();
}

object LightweightPileupFactory {
    def create(context: AlignmentContext, ref: CachedReference): LightweightPileup = {
        val pileup = context.getBasePileup.getPileupWithoutMappingQualityZeroReads;
        val bases = pileup.getBases.map(_.toChar)
        val quals = pileup.getQuals.map(_.toChar)
        val n = bases.length
        val contig = context.getContig
        val pos = context.getPosition
        val refBase = ref.getBase(contig, pos.toInt)
        val bq = bases.zip(quals).filter(x => x._1 != refBase).unzip
        new LightweightPileup(context.getContig, context.getPosition, refBase,
            n - bq._1.length, bq._1.toArray, bq._2.toArray);
    }
}
