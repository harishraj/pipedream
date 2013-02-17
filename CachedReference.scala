package edu.berkeley.cs.amplab.pipedream

import java.io.File

import net.sf.picard.reference.{IndexedFastaSequenceFile, ReferenceSequence}

class CachedReference(val f: File) extends IndexedFastaSequenceFile(f) {
    val contigs = collection.mutable.Map[String, ReferenceSequence]()
    def getSequence(contig: String): ReferenceSequence = {
        if (!contigs.contains(contig)) {
            contigs(contig) = super.getSequence(contig)
        }
        contigs(contig)
    }
    def getBase(contig: String, pos: Int) {
        getSequence(contig).getBases()(pos)
    }
}
