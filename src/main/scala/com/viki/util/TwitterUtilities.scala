package com.viki.util

import org.apache.log4j.Level
import scala.io.Source

object TwitterUtilities {

  def setUpTwitter() = {
    val fileStream = getClass.getResourceAsStream("/twitter.txt")
    for (line <- Source.fromInputStream(fileStream).getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
  }
}