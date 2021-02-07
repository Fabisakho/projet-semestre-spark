import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object ParadiseEdges extends App{

  /**
   * Creation de la session spark
   */
  //val spark = SparkSession.builder().appName(name = "ParadisEdges Fiscal").master(master= "local[*]").getOrCreate()

  /**
   *
   */



  val sc= SparkContext
  val spark = SparkSession.builder
    .master("local")
    .appName("paradisEdges")
    .getOrCreate()

  val users = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/paradise_papers.edges.csv")

  users.printSchema()

  users.show(5)



  val sq = users.select("node_2").distinct
  sq.show()

  //val  test = users.select("rel_type").distinct()
 //test.show(5)

  //val  tes = users.select("r.sourceID" ).distinct()
  //tes.show(5)


   val selecti = users.select("rel_type").where("rel_type!= 'registered_address'")
      selecti.show()

  // selecti.count()
  val usersFiltered = users.select("node_1", "node_2", "rel_type").where("rel_type!= 'registered_address'")
 usersFiltered.show(10)

  //val te = usersFiltered.count()




}
