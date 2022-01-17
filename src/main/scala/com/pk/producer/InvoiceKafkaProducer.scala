package com.pk.producer

import com.pk.config.KafkaConfig
import com.pk.model.Invoice
import com.pk.producer.data.InvoiceSupplier
import io.confluent.kafka.serializers.{AbstractKafkaSchemaSerDeConfig, KafkaAvroSerializer}

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.logging.log4j.{LogManager, Logger}

object InvoiceKafkaProducer extends App {
  val props = new Properties()
  val logger: Logger = LogManager.getLogger("invoiceKafkaProducer")
  props.put(
    ProducerConfig.CLIENT_ID_CONFIG,
    KafkaConfig.producerApplicationID
  )
  props.put(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
    KafkaConfig.bootstrapServers
  )
  props.put(
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    classOf[StringSerializer]
  )
  props.put(
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    classOf[KafkaAvroSerializer]
  )
  props.put(
    AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
    KafkaConfig.schemaRegistryUrl
  )

  val kafkaProducer = new KafkaProducer[String, Invoice](props)

  while (true) {
    val invoice = InvoiceSupplier.nextInvoice()
    logger.info("Invoice: {}", invoice)
    kafkaProducer.send(new ProducerRecord[String, Invoice](KafkaConfig.invoiceTopicName, invoice.getStoreId, invoice))
    Thread.sleep(500)
  }

  Runtime.getRuntime.addShutdownHook {
    new Thread(() => {
      logger.info("Closing producer")
      kafkaProducer.close()
    })
  }
}