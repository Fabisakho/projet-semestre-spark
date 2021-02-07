import org.apache.spark.sql.SparkSession

object ParadiseIntermediary extends App{


  /**
   * Creation de la session spark
   */
  val spark = SparkSession.builder().appName(name = "ParadisIntermediary Fiscal").master(master= "local[*]").getOrCreate()


  /**
   *
   */
  val intermediary = spark.read.option("inferSchema", "true").option("header", "true").csv(path= "data/paradise_papers.nodes.intermediary.csv")
  intermediary.show(20)

}
