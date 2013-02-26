package edu.berkeley.cs.amplab.pipedream

import spark.KryoRegistrator
import com.esotericsoftware.kryo.Kryo
import com.romix.scala.serialization.kryo._

import net.sf.samtools.SAMRecord
import org.broadinstitute.sting.utils.sam.GATKSAMRecord;

class Registrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[LightweightPileup])
    kryo.register(classOf[BQSRCovariate])
    kryo.register(classOf[GATKSAMRecord])
    kryo.register(classOf[SAMRecord])

    // Serialization of all Traversable Scala collections like Lists, Vectors, etc
    kryo.register(classOf[scala.collection.Traversable[_]], new ScalaCollectionSerializer(kryo))
    kryo.register(classOf[scala.Product], new ScalaProductSerializer(kryo))
  }
}
