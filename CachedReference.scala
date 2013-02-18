package edu.berkeley.cs.amplab.pipedream

import java.io.File

import net.sf.picard.reference.{IndexedFastaSequenceFile, ReferenceSequence}
import net.sf.samtools.SAMRecord

class CachedReference(val f: File) extends IndexedFastaSequenceFile(f) {
    val contigs = collection.mutable.Map[String, ReferenceSequence]()
    override def getSequence(contig: String): ReferenceSequence = {
        if (!contigs.contains(contig)) {
            contigs(contig) = super.getSequence(contig)
        }
        contigs(contig)
    }
    def getBase(contig: String, pos: Long): Char = {
        getSequence(contig).getBases()(pos.toInt).toChar
    }
    def getBasesForRead(read: SAMRecord): Array[Char] = {
        val seq = getSequence(read.getReferenceName)
        // This is 1-based in Picard
        val start = read.getAlignmentStart - 1
        val end = read.getReadLength
        seq.toArray.slice(start, start + end).map(_.toChar)
    }
}
