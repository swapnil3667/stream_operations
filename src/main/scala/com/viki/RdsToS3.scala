package com.viki

/**
  * Created by Swapnil on 22/6/17.
  *
  */
import java.sql.DriverManager

import com.typesafe.config.ConfigFactory

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.BasicAWSCredentials
import java.io.File
import scala.collection.mutable.HashMap
import java.io.FileOutputStream
import java.io.ObjectOutputStream
import java.io.FileInputStream
import java.io.ObjectInputStream
import com.amazonaws.services.s3.model.PutObjectRequest

object RdsToS3 {

  val config = ConfigFactory.load("jdbc.conf")

  def main(args: Array[String]) {

    val videoGenres = new HashMap[String,String]
    getVideoGenre(false, videoGenres)
    videoGenres foreach (x => println (x._1 + "-->" + x._2))


    val fos = new FileOutputStream("map.ser")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(videoGenres)


    val bucketName = "vkal-emr-resources"          // specifying bucket name

    //file to upload
    val fileToUpload = new File("map.ser")

    /* These Keys would be available to you in  "Security Credentials" of
        your Amazon S3 account */
    val AWS_ACCESS_KEY = "AKIAJHQ7YREE5DTWZB5A"
    val AWS_SECRET_KEY = "asNWKqKS/HZvsPZrnKLR3fhAgAw+Yb3NgB31PORB"

    val yourAWSCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    val amazonS3Client = new AmazonS3Client(yourAWSCredentials)
    // This will create a bucket for storage
   // amazonS3Client.createBucket(bucketName)
    amazonS3Client.putObject(new PutObjectRequest(bucketName,"swapnil/map.ser", fileToUpload))


    val fis = new FileInputStream("map.ser")
    val ois = new ObjectInputStream(fis)
    val anotherMap = ois.readObject.asInstanceOf[Nothing]

  }

  /* ------------------------------- START -----------------------------*/
  /* ------------------------ READ from POSTGRES -----------------------*/
  /* From RDS: get Video Category/Genre */
  def getVideoGenre(useProxy: Boolean, videoGenres: HashMap[String, String]) {


  val dbtag = "rds"
    val dbname = if (useProxy) config.getString(dbtag+".proxy.database") else config.getString(dbtag+".database")
    val jdbcUrl = if (useProxy) {
      "jdbc:postgresql://"+config.getString(dbtag+".proxy.host")+":"+config.getString(dbtag+".proxy.port")+"/"+dbname
    } else {
      "jdbc:postgresql://"+config.getString(dbtag+".host")+":"+config.getString(dbtag+".port")+"/"+dbname
    }
    val username = if (useProxy) config.getString(dbtag+".proxy.username") else config.getString(dbtag+".username")

    val password = if (useProxy) config.getString(dbtag+".proxy.password") else config.getString(dbtag+".password")
    println("swapnil in jdbc")
    println(jdbcUrl)

    val sqlQuery = """
    SELECT v.id, v.genres
    FROM reporting.videos v WHERE genres IS NOT NULL"""
    try {
      //Class.forName("org.postgresql.Driver")
      val connection = DriverManager.getConnection(jdbcUrl, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery(sqlQuery)
      while ( resultSet.next() ) {
        val id = resultSet.getString("id")
        val genres = resultSet.getString("genres")
        videoGenres += (id -> genres)
      }

    } catch {
      case e: Exception => e.printStackTrace
    }
  }
}
