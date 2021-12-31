package com.pk.config

object KafkaConfig {
  val producerApplicationID = "InvoiceProducer"
  val streamsApplicationId = "InvoiceConsumer"
  val bootstrapServers = "localhost:9092,localhost:9093"
  val invoiceTopicName = "invoice-topic"
  val shipmentTopicName = "shipment-topic"
  val notificationTopicName = "notification-topic"
  val hadoopTopicName = "hadoop-record-topic"
  val schemaRegistryUrl = "http://localhost:8081"
}

object AppConfig {
  val homeDelivery = "HOME"
  val onlineDelivery = "ONLINE"
}