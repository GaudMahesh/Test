package com.tcs.nwc

import scala.annotation.tailrec
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, _}
import scala.util.Random
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.expressions.Window

object MeterDataGenerator {
    Logger.getLogger("org").setLevel(Level.WARN)
    private val logger = Logger.getLogger(this.getClass.getName)

    val mapdivdist:Map[String,List[String]] = Map("Asir" -> List("Asir"),  
                        "Al-Bahah" -> List("Al-Bahah"), 
                        "Al-Qassim" -> List("Al-Qassim"),
                        "Sudair" -> List("Sudair"), 
                        "Riyadh" -> List("Riyadh"), 
                        "Mecca" -> List("Mecca"), 
                        "Medina" -> List("Medina"))
                                    
    val mapdistcity:Map[String,List[String]] = Map("Asir" -> List("Abha","Bareq","Bisha"),  
                        "Al-Bahah" -> List("Al-Bahah","Baljurashi"), 
                        "Al-Qassim" -> List("Qatif","Unaizah"),
                        "Sudair" -> List("Hautat-Sudair","Jalajil"),
                        "Mecca" -> List("Jeddah","Makkah","Shafa"),
                        "Medina" -> List("Medina"),
                        "Riyadh" -> List("Layla","Rimah","Shagra"))
                                                
    val mapcitypost:Map[String,List[String]] = Map("Abha" -> List("45215"),"Bareq" -> List("45218"),  
                "Bisha" -> List("45285"),"Al-Bahah" -> List("46524"),"Baljurashi" -> List("42542"),
                "Qatif" -> List("46523"),"Unaizah" -> List("42515"),"Hautat-Sudair" -> List("45236"),
                "Jalajil" -> List("45218"),"Jeddah" -> List("45216"),"Makkah" -> List("48755"),
                "Shafa" -> List("41254"),"Medina" -> List("42157"),"Layla" -> List("43652"),
                "Rimah" -> List("45521"),"Shagra" -> List("48541"))
                        
    val mappostpremise:Map[String,List[String]] = Map("45215" -> List("XXXX1"), "45218" -> List("XXXX2"), 
               "45285" -> List("XXXX3"), "46524" -> List("XXXX4"),"42542" -> List("XXXX5"),
               "46523" -> List("XXXX6"), "42515" -> List("XXXX7"),"45236" -> List("XXXX8"),
               "45218" -> List("XXXX9"), "45216" -> List("XXXX10"),"48755" -> List("XXXX11"),
               "41254" -> List("XXXX12"), "42157" -> List("XXXX13"),"43652" -> List("XXXX14"),
               "45521" -> List("XXXX15"), "48541" -> List("XXXX16","XXXX17"))
                        
    val mappremlocid:Map[String,List[String]] = Map("XXXX1" -> List("YYYY1","YYYY2","YYYY3"), "XXXX2" -> List("YYYY4","YYYY5","YYYY6"), 
               "XXXX3" -> List("YYYY7","YYYY8","YYYY9"), "XXXX4" -> List("YYYY10","YYYY11","YYYY12"),"XXXX5" -> List("YYYY13","YYYY14","YYYY15"),
               "XXXX6" -> List("YYYY16","YYYY17","YYYY18"), "XXXX7" -> List("YYYY19","YYYY20","YYYY21"),"XXXX8" -> List("YYYY22","YYYY23","YYYY24"),
               "XXXX9" -> List("YYYY25","YYYY26","YYYY27"), "XXXX10" -> List("YYYY28","YYYY29","YYYY30"),"XXXX11" -> List("YYYY31","YYYY32","YYYY33"),
               "XXXX12" -> List("YYYY34","YYYY35","YYYY36"), "XXXX13" -> List("YYYY37","YYYY38","YYYY39"),"XXXX14" -> List("YYYY40","YYYY41","YYYY42"),
               "XXXX15" -> List("YYYY43","YYYY44","YYYY45"), "XXXX16" -> List("YYYY46","YYYY47","YYYY48"),"XXXX17" -> List("YYYY49","YYYY50","YYYY51"))                        
                                    
  def main(args: Array[String]) {

    
    val spark: SparkSession = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val sqlcontext:SQLContext = spark.sqlContext
//    val sqlcontext = new org.apache.spark.sql.SQLContext(sc)
    
    println("Enter the numbers of records to be generated ")
    val num = scala.io.StdIn.readInt()


    val mt_data = 1 to num map(x =>  (randomString(30)
                                   ,randomString(12),randomString(30)))
    
    val lc_data = 1 to num map(x =>  (randomString(8),randomString(50),randomInt(4)))

    val mtr_data = sqlcontext.createDataFrame(mt_data).toDF("Meter_Id_Nbr"
                                                      ,"Meter_Sr_Cd","Badge_Nbr")  
     
                                                      
    val loc_data = sqlcontext.createDataFrame(lc_data).toDF("Geo_Type"
                                                      ,"Geo_Value","Sp_Id")  

                                                    
    def rowgen() : Row = {               
//    println("generating key value")
    val a = keyvalue()
//    println("Key Value")    
//    println(List(a))
    val r = a._1
    val d = a._2
    val c = a._3
    val p = a._4
    val i = a._5
    val l = a._6
    val rowdata = Row(r,d,c,p,i,l) 
    return (rowdata)
    }
    
    val kdata = 1 to num map(x =>  rowgen())  
    val lrows = kdata.toList
    
       
    val schema = StructType(List(
    StructField("Region", StringType, true),
    StructField("District", StringType, true),
    StructField("City", StringType, true) ,
    StructField("PostCode", StringType, true),
    StructField("Premise_ID", StringType, true),
    StructField("Loc_ID", StringType, true)
    
    ))

    /* Convert list to RDD */
    val rdd = spark.sparkContext.parallelize(lrows)

    /* Create data frame */
    val df = spark.createDataFrame(rdd, schema)
//    print(df.schema)
//    df.show(5) 
                                                      
    /**
		* Add Column Index to dataframe 
		*/
    def addColumnIndex(df: DataFrame) = sqlcontext.createDataFrame(
    // Add Column index
    df.rdd.zipWithIndex.map{case (row, columnindex) => Row.fromSeq(row.toSeq :+ columnindex)},
    // Create schema
    StructType(df.schema.fields :+ StructField("columnindex", LongType, false))
    )
    
    def combine(df1 : DataFrame,df2 : DataFrame) : DataFrame = {
    // Add index now...
    val df1WithIndex = addColumnIndex(df1)
    val df2WithIndex = addColumnIndex(df2)

    // Now time to join ...
    val df = df1WithIndex
                 .join(df2WithIndex , Seq("columnindex"))
                 .drop("columnindex")
    return df                 
    }                                                   
        
      //Distinct all columns
    val locdata1 = df.distinct()
    val unique = locdata1.count().toInt
    println("Distinct count: "+locdata1.count())
     //    locdata1.show()
    
    val locdata = combine(loc_data,locdata1)
    locdata.show(5)
    
    val w = Window.orderBy("Meter_Id_Nbr")
    val mtrindex1 = mtr_data.toDF().withColumn("Meter_ID",row_number().over(w))
    val mtrindex2 = moveColumnToFirstPos(mtrindex1,"Meter_ID")
    mtrindex2.show(5)
    
    val mrtdata = combine(mtrindex2,locdata).sort("Meter_ID").toDF()
    mrtdata.show(5)
   
    
    val final_loc_data = locdata.coalesce(1)
                    .write.mode(SaveMode.Overwrite)
                    .option("header",true)
                    .csv("C:\\Users\\hp\\Desktop\\NWC_POC\\Location_Data")    
    
    val final_mtr_data = mrtdata.coalesce(1)
                        .write.mode(SaveMode.Overwrite)
                        .option("header",true)
                        .csv("C:\\Users\\hp\\Desktop\\NWC_POC\\Meter_Data")    
    
    println("Unique Number of Records generated :"+ unique)                        
    spark.stop()
    
  }
    
    def moveColumnToFirstPos(dataframecolumnOrderChanged: DataFrame, colname: String):DataFrame =  {
    val xs = Array(colname) ++ dataframecolumnOrderChanged.columns.dropRight(1)
    val fistposColDF = dataframecolumnOrderChanged.selectExpr(xs: _*)
    return fistposColDF
  }

    
    //random keyvalue function
    def keyvalue() : (String,String,String,String,String,String) = {
        
    val div = mapdivdist.keys.toList
//    println(div)
    
    val region = lit(div(Random.nextInt(div.size))).toString()
//    println(region)
                        
    val dist = mapdivdist(region.toString())
//    println(dist)
    
    val district = lit(dist(Random.nextInt(dist.size))).toString()
//    println(region,"  ",district)
    
    val city = mapdistcity(district.toString())
//    println(city)
    
    val City = lit(city(Random.nextInt(city.size))).toString()
//    println(City)
    
    val post = mapcitypost(City.toString())
//    println(post)
    
    val Post = lit(post(Random.nextInt(post.size))).toString()
//    println(Post)
    
    val premise = mappostpremise(Post.toString())
//    println(post)
    
    val PremiseId = lit(premise(Random.nextInt(premise.size))).toString()
//    println(PremiseId)  
    
    val loc = mappremlocid(PremiseId.toString())
//    println(post)
    
    val LocId = lit(loc(Random.nextInt(loc.size))).toString()
//    println(LocId)    
    
//    val (reg,dist,city,post,premise,loc) = (region,district,City,Post,PremiseId,LocId)
    return (region,district,City,Post,PremiseId,LocId)
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
