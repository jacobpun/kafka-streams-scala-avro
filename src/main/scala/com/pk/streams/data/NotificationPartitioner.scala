package com.pk.streams.data

import com.pk.model.Invoice
import org.apache.kafka.streams.processor.StreamPartitioner

object NotificationPartitioner extends StreamPartitioner[String, Invoice] {
  override def partition(topic: String, key: String, value: Invoice, numPartitions: Int): Integer = value.getCustomerNumber.hashCode % numPartitions
}
