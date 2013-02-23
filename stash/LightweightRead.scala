package edu.berkeley.cs.amplab.pipedream

import org.broadinstitute.sting.gatk.contexts.AlignmentContext;
import net.sf.picard.reference.{IndexedFastaSequenceFile, ReferenceSequence}
import net.sf.samtools.SAMRecord

trait ShortRead {
    def sequence: Array[Char];
    def qualities: Array[Char];
    def cigar: String;
    def position: Integer;
    def contig: String;
}

// A lightweight read class which stores diffs against the reference.
class LightweightRead(
    val contig:String, 
    val position:Long,
    val readLength:Int, 
    val cigar:String, 
    private val _sequence: Map[Int, Char], 
    val qualities: Array[Char]) extends ShortRead {

    def sequence(ref: CachedReference): Array[Char] = {
        val refSeq = ref.getBases(contig, position, readLength)
        for ((pos,base) <- _sequence)
            refSeq.update(pos, base)
        refSeq
    }
}

package object LightweightReadFactory {
    def create(read: SAMRecord, ref: CachedReference): LightweightRead = {
        val contig = read.getReferenceName
        val pos = read.getPosition
        val rlen = read.getReadLength
        val refSeq = ref.getBases(contig, pos, rlen)
        val diffBases = read.getReadBases.zip(refseq).filter(_ != _).zipWithIndex.map((_, _._1))
        new LightweightRead(
            contig,
            pos,
            rlen,
            read.getCigar,
            diffBases,
            read.getBaseQualities
        )
    }
}
