
import org.apache.spark.sql.SparkSession



object ParadisePapers extends App{

  /**
   * Creation de la session spark
   */
 val spark = SparkSession.builder().appName(name = "Paradis Fiscal").master(master= "local[*]").getOrCreate()
  /**
   * Creation du val
   */

  /**
   *
    */
  val paradisf = spark.read.option("inferSchema", "true").option("header", "true").csv(path= "data/paradise_papers.nodes.address.csv")
  paradisf.show(20)

  //val sqlCsv = spark.sql("SELECT * FROM paradise_papers.nodes.address.csv")
  //paradisf.count()
  //paradisf.select("n.countries").show(10)
//paradisf.select("n.countries STRING").show(10)
  //val aff =  spark.sql("SELECT * from "data/paradise_papers.nodes.address.csv" ")
/*
* Afficher le contenue du csv
 */
 // val dfReader= spark.read
 // val dfcsv= dfReader.csv("data/paradise_papers.nodes.address.csv")
  //dfcsv.show(5)
/**
* Creer une vue temporaire
 */
  //val sqlDfCsv: Unit = dfcsv.createOrReplaceTempView("paradise_papers.nodes.address")
 // val sqlDf = spark.sql("SELECT * FROM paradise_papers.nodes.address where n.countries = Mauritius")
 // sqlDf.show(5)
}
