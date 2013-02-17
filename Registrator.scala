package edu.berkeley.cs.amplab.pipedream

import spark.KryoRegistrator
import com.esotericsoftware.kryo._

class Registrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[LightweightPileup])
  }
}
