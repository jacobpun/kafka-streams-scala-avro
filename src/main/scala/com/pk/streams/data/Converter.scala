package com.pk.streams.data

import com.pk.config.{KafkaConfig}
import com.pk.model.{HadoopRecord, Invoice, Notification}
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

import scala.jdk.CollectionConverters._

object HadoopRecordsTransformer {
  def transform(invoice: Invoice): List[HadoopRecord] =
    invoice.getLineItems.asScala.map(
      item => {
        new HadoopRecord(
          invoice.getInvoiceNumber,
          invoice.getCustomerNumber,
          item.getItem,
          item.getUnitPrice,
          item.getQuantity
        )
      }
    ).toList
}

object NotificationTransformer extends ValueTransformer[Invoice, Notification] {
  var kvStore: KeyValueStore[String, java.lang.Double] = null

  override def init(context: ProcessorContext): Unit = {
    kvStore = context.getStateStore(KafkaConfig.storeName)
  }

  override def transform(invoice: Invoice): Notification = {
    val thisInvoiceTotal = invoice.getLineItems.asScala.map(item => item.getQuantity * item.getUnitPrice).sum
    val totalAmountSoFar = kvStore.get(invoice.getCustomerNumber)
    val newTotal = {
      if (totalAmountSoFar == null) {
        thisInvoiceTotal
      } else {
        totalAmountSoFar + thisInvoiceTotal
      }
    }
    kvStore.put(invoice.getCustomerNumber, newTotal)
    new Notification(
      invoice.getInvoiceNumber,
      invoice.getCustomerNumber,
      thisInvoiceTotal,
      newTotal
    )
  }

  override def close(): Unit = {}
}
