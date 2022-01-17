package com.pk.producer.data

import com.pk.config.AppConfig
import com.pk.model.{Address, Invoice, Item}

import scala.util.Random
import scala.jdk.CollectionConverters._

object InvoiceSupplier {
  private val random = new Random()

  private val customerNumbers = List("C-1", "C-2", "C-3", "C-4", "C-5")
  private val storeIds = List("S-1", "S-2", "S-3", "S-4", "S-5")

  def nextInvoice(): Invoice = {
    val address = {
      if (random.nextInt(10) > 5)
        new Address(
          randomString(10),
          randomString(7),
          randomString(8),
          randomString(9),
          randomString(5))
      else
        null
    }

    val lineItems = (0 to random.nextInt(4)).map(_ => {
      new Item(
        randomString(8),
        random.nextDouble(),
        random.nextInt(100)
      )
    }).toList

    new Invoice(
      randomString(10),
      storeIds(random.nextInt(storeIds.length)),
      customerNumbers(random.nextInt(customerNumbers.length)),
      if (address == null) AppConfig.onlineDelivery else AppConfig.homeDelivery,
      address,
      lineItems.asJava
    )
  }

  def randomString(length: Int, characterSet: Array[Char] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray): String = {
    val result = new Array[Char](length)
    for (i <- result.indices) {
      val randomCharIndex = random.nextInt(characterSet.length)
      result(i) = characterSet(randomCharIndex)
    }
    new String(result)
  }
}
