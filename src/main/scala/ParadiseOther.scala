import org.apache.spark.sql.SparkSession

object ParadiseOther extends App{

  /**
   * Creation de la session spark
   */
  val spark = SparkSession.builder().appName(name = "ParadisOther Fiscal").master(master= "local[*]").getOrCreate()

  /**
   *
   */
  val other = spark.read.option("inferSchema", "true").option("header", "true").csv(path= "data/paradise_papers.nodes.other.csv")
  other.show(20)

}
