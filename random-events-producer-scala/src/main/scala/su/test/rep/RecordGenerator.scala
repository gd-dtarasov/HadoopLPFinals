package su.test.rep

import java.time.{LocalDate, LocalDateTime, LocalTime}
import scala.util.Random.{nextGaussian, nextInt}
import java.time.format.DateTimeFormatter

import scala.util.Random

object RecordGenerator {
  val style = Array("Classic", "Striped", "Destroyed")
  val color = Array("Navy Blue", "Midnight Black", "Flame Red")
  val item = Array("Jeans", "Shorts", "T Shirt", "Hoodie")
  val age = Array("Adult", "Kid")
  val category_special = Array("New Arrivals", "Sale", "BOGO", "Regular")
  val category_gender = Array("Male", "Female", "Unisex")
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
  
  def generateProductName(): String =
    Array(style, color, age, item).map(a => a(nextInt(a.length))).mkString(" ")


  def generateProductCategory(): String =
    Array(category_gender, category_special).map(a => a(nextInt(a.length))).mkString(" ")

  def generateProductPrice(): Double = (Math.abs(nextGaussian()) * 10000).round / 100d

  def generatePurchaseDate(): String = {
    val now = LocalDate.now();
    val date = LocalDate.of(now.getYear, now.getMonth, nextInt(7) + 1)
    val time = LocalTime.ofSecondOfDay((Math.abs(nextGaussian()) * (24 * 60 * 60 - 1)).round % (24 * 60 * 60 - 1))
    formatter.format(LocalDateTime.of(date, time))
  }

  def generateClientIP(): String = nextInt(255) + "." + nextInt(255) + "." + nextInt(255) + "." + nextInt(255)

  def generatePurchaseRecord: String =
    Array(generateProductName(), generateProductPrice(), generatePurchaseDate(), generateProductCategory(), generateClientIP())
      .mkString(",") + "\n"
}
