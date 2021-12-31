package com.pk.streams.data

import com.pk.model.{HadoopRecord, Invoice, Notification}

import scala.jdk.CollectionConverters._

object Converter {

  def convertToNotification(invoice: Invoice): Notification =
    new Notification(
      invoice.getInvoiceNumber,
      invoice.getCustomerNumber,
      invoice.getLineItems.asScala.map(item => item.getQuantity * item.getUnitPrice).sum
    )

  def convertToHadoopRecords(invoice: Invoice): List[HadoopRecord] =
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
