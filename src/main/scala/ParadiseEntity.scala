import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache._




object ParadiseEntity extends App{

  /**
   * Creation de la session spark
   */
  val spark = SparkSession.builder().appName(name = "ParadisEntity Fiscal").master(master= "local[*]").getOrCreate()

  /**
   *
   */
  val entity = spark.read.format("csv").option("inferSchema", "true").option("mode", "DROPMALFORMED").option("header", "true").csv(path= "data/paradise_papers.nodes.entity.csv")
  entity.show(10)
  entity.printSchema()

  //val filterData = entity.select("*").where("n.country_codes=CYM")
  //filterData.show(10)


}
