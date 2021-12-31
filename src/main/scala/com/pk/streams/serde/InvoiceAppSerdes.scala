package com.pk.streams.serde

import com.pk.config.KafkaConfig
import com.pk.model.{HadoopRecord, Invoice, Notification}
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.{Serde, Serdes}

object InvoiceAppSerdes extends Serdes {

  def Invoice(): Serde[Invoice] = {
    configure(new SpecificAvroSerde[Invoice]())
  }

  def Notification(): Serde[Notification] = {
    configure(new SpecificAvroSerde[Notification]())
  }

  def HadoopRecord(): Serde[HadoopRecord] = {
    configure(new SpecificAvroSerde[HadoopRecord]())
  }

  private def configure[T](serde: Serde[T]): Serde[T] = {
    serde.configure(
      java.util.Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConfig.schemaRegistryUrl),
      false
    )
    serde
  }
}
