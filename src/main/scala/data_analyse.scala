import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object data_analyse {

  //  统计各类专辑的数量
  def genre(sc:SparkContext,spark:SparkSession,df:DataFrame):Unit={
    val df2=df.groupBy("genre").count().orderBy(desc("count"))
    println(df2.show(10))
  }

  //  统计各类专辑的销量
  def genreSales(sc:SparkContext,spark:SparkSession,df:DataFrame):Unit={
    val df3=df.groupBy("genre")
      .sum("num_of_sales")
      .withColumnRenamed("sum(num_of_sales)","sales_num")
    println(df3.show(10))
  }

  //  统计每年的专辑数量和单曲数量
  def yearTracksAndSales(spark:SparkSession):Unit={
    val df4=spark.sql("select " +
      "year_of_pub," +
      "sum(num_of_tracks) as sum_tracks," +
      "count(*) as count_album " +
      "from music_data " +
      "group by year_of_pub " +
      "order by sum_tracks,count_album")
    println(df4.show(10))
  }

  // 分析总销量前五的专辑类型的各年份销量
  def GenreYearSales(spark:SparkSession):Unit={
    val df5=spark.sql("select " +
      "genre," +
      "count(num_of_sales) as count_sales " +
      "from music_data " +
      "group by genre " +
      "order by count_sales desc " +
      "limit 5")
    df5.createOrReplaceTempView("temp1")
    val df6=spark.sql("select " +
      "temp1.genre," +
      "temp2.year_of_pub," +
      "temp2.num_of_sales " +
      "from music_data temp2 " +
      "inner join temp1 " +
      "on temp2.genre=temp1.genre")
    df6.createOrReplaceTempView("temp3")
    val df7=spark.sql("select " +
      "genre," +
      "year_of_pub," +
      "sum(num_of_sales) as sum_sales " +
      "from temp3 " +
      "group by genre,year_of_pub " +
      "order by genre")
    println(df5.show())
    println(df6.show())
    println(df7.show())
  }

  //分析总销量前五的专辑类型，在不同评分体系中的平均评分
  def GenreCritic(spark:SparkSession):Unit={
    val df8=spark.sql("select " +
      "genre," +
      "sum(num_of_sales) as sum_sales " +
      "from music_data " +
      "group by genre " +
      "order by sum_sales desc " +
      "limit 5")
    df8.createOrReplaceTempView("df8")
    val df9=spark.sql("select " +
      "* " +
      "from music_data " +
      "where music_data.genre in (select genre from df8)")
    df9.createOrReplaceTempView("df9")
    val df10=spark.sql("select " +
      "genre," +
      "avg(rolling_stone_critic) as avg_rolling_stone_critic," +
      "avg(mtv_critic) as avg_mtv_critic," +
      "avg(music_maniac_critic) as avg_music_maniac_critic " +
      "from df9 " +
      "group by genre")
    println(df10.show())
  }

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local[*]").setAppName("music_data_analyse")
    val sc=new SparkContext(conf)
    val spark=SparkSession.builder().config(conf).getOrCreate()
//    val struct=
    val df=spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .load("D:\\music_data_analyes\\src\\main\\data\\albums.csv")
    df.createOrReplaceTempView("music_data")
//    println(df.schema)
//    println(df.show(10))
//    genre(sc,spark,df)
//    genreSales(sc,spark,df)
//    yearTracksAndSales(spark)
//    GenreYearSales(spark)
//    GenreCritic(spark)
  }
}
