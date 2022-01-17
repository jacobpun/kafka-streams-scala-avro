package com.pk.streams

import com.pk.config.{AppConfig, KafkaConfig}
import com.pk.model.{Invoice, Notification}
import com.pk.streams.data.{HadoopRecordsTransformer, NotificationPartitioner, NotificationTransformer}
import com.pk.streams.serde.InvoiceAppSerdes
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced, Repartitioned, ValueTransformerSupplier}
import org.apache.kafka.streams.state.{KeyValueStore, StoreBuilder, Stores}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig}
import org.apache.logging.log4j.{LogManager, Logger}

import scala.jdk.CollectionConverters._
import java.util.Properties

object InvoiceStreamProcessor extends App {
  val logger: Logger = LogManager.getLogger("invoiceKafkaStream")

  val props = new Properties()
  props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.streamsApplicationId)
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers)

  val streamsBuilder: StreamsBuilder = new StreamsBuilder

  val invoiceStream = streamsBuilder.stream(
    KafkaConfig.invoiceTopicName,
    Consumed `with`(Serdes.String(), InvoiceAppSerdes.Invoice())
  )

  invoiceStream
    .filter((k, v) => v.getDeliveryType.equalsIgnoreCase(AppConfig.homeDelivery))
    .peek((k, v) => logger.info("Sending to shipment topic: {}", v))
    .to(KafkaConfig.shipmentTopicName, Produced `with`(Serdes.String(), InvoiceAppSerdes.Invoice()))

  val kvStoreBuilder: StoreBuilder[KeyValueStore[String, java.lang.Double]] = Stores.keyValueStoreBuilder(
    Stores.inMemoryKeyValueStore(KafkaConfig.storeName),
    Serdes.String(),
    Serdes.Double()
  )

  streamsBuilder.addStateStore(kvStoreBuilder)

  val transformerSupplier: ValueTransformerSupplier[Invoice, Notification] = () => NotificationTransformer

  invoiceStream
    .repartition(Repartitioned.streamPartitioner(NotificationPartitioner))
    .transformValues(transformerSupplier, KafkaConfig.storeName)
    .peek((_, v) => logger.info("Sending to notification topic: {}", v))
    .to(KafkaConfig.notificationTopicName, Produced `with`(Serdes.String(), InvoiceAppSerdes.Notification()))

  invoiceStream
    .flatMapValues(HadoopRecordsTransformer.transform(_).asJava)
    .peek((_, v) => logger.info("Sending to hadoop topic: {}", v))
    .to(KafkaConfig.hadoopTopicName, Produced `with`(Serdes.String(), InvoiceAppSerdes.HadoopRecord()))

  val streams: KafkaStreams = new KafkaStreams(streamsBuilder.build(), props)
  streams.start()

  Runtime.getRuntime.addShutdownHook {
    new Thread(() => {
      logger.info("Closing stream")
      streams.close()
    })
  }
}

