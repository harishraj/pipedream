package edu.berkeley.cs.amplab.scatk

import org.broadinstitute.sting.gatk.contexts.AlignmentContext;

class LightweightPileup(val bases:List[Char], val quals:List[Char]);

object LightweightPileupFactory {
    def create(context: AlignmentContext): LightweightPileup = {
        val pileup = context.getBasePileup().getPileupWithoutMappingQualityZeroReads();
        val bases = pileup.getBases().toList.map(_.toChar)
        val quals = pileup.getQuals().toList.map(_.toChar)
        new LightweightPileup(bases, quals);
    }
}
