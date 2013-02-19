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
    // Accepts 1-based coordinates
    def getBase(contig: String, pos: Int): Char = {
        getBases(contig, pos, 1)(0)
    }
    // Accepts 1-based coordinates
    def getBases(contig: String, start: Int, length: Int): Array[Char] = {
        getSequence(contig).getBases().slice(start - 1, start + length - 1).map(_.toChar)
    }
    def getBasesForRead(read: SAMRecord) {
        getBases(read.getReferenceName, read.getUnclippedStart - 1, read.getReadLength)
    }
}

