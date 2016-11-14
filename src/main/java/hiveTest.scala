import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by soumyaka on 11/14/2016.
  */
object hiveTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("hive-test")
      .setMaster("yarn-client")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hc = new HiveContext(sc)

    import sqlContext.implicits._
    Logger.getRootLogger().setLevel(Level.ERROR)
//    val query = "insert into daimler_test_db.DW_PRODUCTION_DOSR_MALFUCTION values(\"line number\", 1, 2, \"str1\", \"str2\", \"str3\", \"str4\")"


    val testRDD = sc.parallelize(Seq(Row("line", 1, 2, "str1", "str2", "str3", "str4")))

    val data = hc.sql("select * from daimler_test_db.DW_PRODUCTION_DOSR_MALFUCTION where 1=2")

//    data.schema
    val testDF = hc.createDataFrame(testRDD, data.schema)

   /// testDF.write.mode(SaveMode.Append).saveAsTable("`daimler_test_db.dw_production_dosr_malfuction`")
    //testDF.write.mode(SaveMode.Append).insertInto("`daimler_test_db.dw_production_dosr_malfuction`")
    testDF.write.mode(SaveMode.Append).format("orc").save("/apps/hive/warehouse/daimler_test_db.db/dw_production_dosr_malfuction/")


    //hc.sql("select * from daimler_test_db.DW_PRODUCTION_DOSR_MALFUCTION")

//    hc.sql(query)

    println("done")
  }
}
