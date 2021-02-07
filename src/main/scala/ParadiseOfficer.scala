import org.apache.spark.sql.SparkSession

object ParadiseOfficer extends App{

  /**
   * Creation de la session spark
   */
  val spark = SparkSession.builder().appName(name = "ParadisOfficer Fiscal").master(master= "local[*]").getOrCreate()

  /**
   *
   */
  val officer = spark.read.option("inferSchema", "true").option("header", "true").csv(path= "data/paradise_papers.nodes.officer.csv")
  officer.show(20)


}

