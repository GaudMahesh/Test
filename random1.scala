package com.tcs.nwc

import scala.annotation.tailrec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, _}
import scala.util.Random
import java.time.LocalDate
import java.time.LocalTime
import java.time._
import scala.util.Random
import java.io.File
import java.time.LocalDateTime
import java.time.ZoneOffset

object RandomDataGenerator {
  
	Logger.getLogger("org").setLevel(Level.WARN)
	val logger = Logger.getLogger(this.getClass.getName)
                                      
  def main(args: Array[String]) {
    
    val starttime = System.nanoTime()

    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    require(args.length <= 2,
      s"""
         | USAGE : <Num of Records to be generated>  <number of Files to be generated
         | Current values are : ${args.foreach(println)}
   """.stripMargin)
    val numrecords = if (args.length > 0) args(0).toInt else 1000
    val numfiles = if (args.length > 0) args(1).toInt else 4
    
    val num = numrecords * numfiles
    
    val sqlcontext:SQLContext = spark.sqlContext
    
    val source = scala.io.Source.fromFile("C:\\Users\\hp\\Desktop\\NWC_POC\\time.txt")
    val lines = try source.mkString finally source.close()
//    println("Last Run Time ",lines)

    val ddtime = LocalDateTime.parse(lines)
//    println("converted " + ddtime)
      
    val (year,month,date,hour,min,sec) = (ddtime.getYear,ddtime.getMonthValue,ddtime.getDayOfMonth,
                                       ddtime.getHour,ddtime.getMinute,ddtime.getSecond)
//    println("Split Date Time ", year,month,date,hour,min,sec)
    
    def randomdatetime(year:Int,month:Int,date:Int,hour:Int,min:Int,sec:Int): java.sql.Timestamp  = {
   	val timenow = java.time.LocalDateTime.now()
   	val (year1,month1,date1,hour1,min1,sec1) = (timenow.getYear,timenow.getMonthValue,timenow.getDayOfMonth,
                                       timenow.getHour,timenow.getMinute,timenow.getSecond)
    val minTime = LocalDateTime.of(year, month, date, hour, min, sec).toEpochSecond(ZoneOffset.UTC).toInt
    val maxTime = LocalDateTime.of(year1, month1, date1, hour1, min1, sec1).toEpochSecond(ZoneOffset.UTC).toInt
    val randomTime = minTime + Random.nextInt(maxTime - minTime).toLong
    val rdatetime = LocalDateTime.ofEpochSecond(randomTime,0,ZoneOffset.UTC)
    val randomdatetime = java.sql.Timestamp.valueOf(rdatetime)
    return randomdatetime
    }
                                                        
//    println("Files")
    val files = getListOfFiles("C:\\Users\\hp\\Desktop\\NWC_POC\\Meter_Data\\")
//    println("List of Files : ",files)
    
    val li = files(0)
//    print(li)
    
    val meterdata = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
//    .load("C:\\Users\\hp\\Desktop\\NWC_POC\\Meter.csv")
    .load(li)
    
    meterdata.show(5)
    
    val meterlist = meterdata.select("Meter_Id_Nbr").map(r => r.getString(0)).collect.toArray
    
    val Geotype = meterdata.select("Geo_Type").map(r => r.getString(0)).collect.toArray
    
    val Geovalue = meterdata.select("Geo_Value").map(r => r.getString(0)).collect.toArray
//    println(meterlist.toList)
    
    
    //random meter function
    def meter(): String = {
    val randmeter = lit(meterlist(Random.nextInt(meterlist.size))).toString()
    return randmeter
  }

    def geotype(): String = {
    val randmeter = lit(Geotype(Random.nextInt(Geotype.size))).toString()
    return randmeter
  }
    
    def geovalue(): String = {
    val randmeter = lit(Geovalue(Random.nextInt(Geovalue.size))).toString()
    return randmeter
  }    

/*    println("Enter the numbers of records : ")
    val num = scala.io.StdIn.readInt()
    
    println("Enter the numbers of files : ")
    val numfiles = scala.io.StdIn.readInt()*/
    
    val randomdata = 1 to num map(x =>  (randomdatetime(year,month,date,hour,min,sec)
                                   ,randomString(10)
                                   ,meter(),1,randomFloat()
                                   ,randomString(2),randomString(4),randomString(4)))

    val randomdata1 = sqlcontext.createDataFrame(randomdata).toDF("READ_DTTM"
                                                      ,"MTR_CONFIG_ID"
                                                      ,"MTR_ID_NBR","READ_SEQ","REG_READING"
                                                      ,"READ_TYPE_FLG","UOM_CD","READER_REM_CD")    
  

    val randomdata2 = randomdata1.join(meterdata,randomdata1("MTR_ID_NBR") ===  meterdata("Meter_Id_Nbr"),"inner")
                      .drop("Meter_ID","Loc_ID","MTR_ID_NBR").sort(randomdata1("READ_DTTM")).toDF()
    randomdata2.show(5)
//    randomdata2.printSchema()
                      
    val final_df = randomdata2.coalesce(numfiles)                                                                           
                    .write.mode(SaveMode.Overwrite)
                    .option("header",true)
                    .csv("C:\\Users\\hp\\Desktop\\NWC_POC\\Transaction_Data")
    
    println("Number of Records :"+ numrecords)   
    println("Number of Files :"+ numfiles)
    println("Total Number of Records Generated :"+ num)
    
    val currenttime = java.time.LocalDateTime.now()
    val curtime = currenttime.toString()

    scala.tools.nsc.io.File("C:\\Users\\hp\\Desktop\\NWC_POC\\time.txt").writeAll(curtime)
    val endtime = System.nanoTime()
    val timelapse = ((endtime-starttime)/1000/1000/1000).toInt
    println("Program End Time :" +currenttime)
    println("Execution Time : "+ timelapse+" sec")
    spark.stop()               
    sc.stop()
  }
    
    def getListOfFiles(dir: String): List[String] = {
      val file = new File(dir)
      file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.getPath).toList
    }    
  
//  random int function
    def randomInt(length: Int) = {
//  logger.info("length is " + length)
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val value = r.nextInt(length)
//      println("Value : ",value)
      if (value > 0)
        sb.append(value)
        else
        sb.append(value) 
    }
//        logger.info(sb)
    sb.toString.substring(0, length)
    }
    
    def randomFloat():String={
    val max = 111111111111111.11f
    val min = 999999999999999.99f
    val randfloat=(Math.random())*(max-min)+min
    val rloat = f"$randfloat%1.6f"
    return rloat
    }
    
    //random string function    
    def randomString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }
    
  
  
  // random alphanumeric
  def randomAlphaNumericString(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
    randomStringFromCharList(length, chars)
  }
  
  // random alpha
  def randomAlpha(length: Int): String = {
    val chars = ('a' to 'z') ++ ('A' to 'Z')
    randomStringFromCharList(length, chars)
  }

  // ranndom char generator
  def randomStringFromCharList(length: Int, chars: Seq[Char]): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      val randomNum = Random.nextInt(chars.length)
      sb.append(chars(randomNum))
    }
    sb.toString
  }

}
