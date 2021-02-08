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

  //Ce Dataframe contiient des informations sur les comptes leurs ID date d'ouverture et date de fin, on pourrait affiché l'année qui a eu le plus d'ouverture de comptes
  var users = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/paradise_papers.edges.csv")
  println("affiche le schema des donnees Edges")
  users.printSchema()


  //On va modifier le nom des colonnes commençant par "r."
  for (x <- users.columns){
    if(x.contains("r.")){
      users = users.withColumnRenamed(x,x.drop(2))
    }
  }
  println("affiche les nom des colonnes et le dataframe Edges")
  users.columns
  users.show(5)


  users.createOrReplaceTempView("users")
  var date = spark.sql("SELECT start_date FROM users")
  date = date.filter("start_date is not NULL")

  def years(row: Array[org.apache.spark.sql.Row]): Array[String] ={
    var liste_annee = new Array[String](0)
    var annee : String = ""
    for (x <- row){
      if(x != null) {
        annee = (x.toString().split("-"))(0).drop(1)
        liste_annee ++= Array(annee)
      } // Récupère uniquement les années
    }
    liste_annee
  }
  println("affiche la listes des années d'ouverture de comptes contenu dans nos données")
  var liste_annees = years(date.collect)
  println(liste_annees.distinct)
  """
     //RDD.toDF ne fonctionne pas sur IntelliJ
  val yearsDF = spark.sparkContext.parallelize(liste_annees).toDF("annees")
  yearsDF.show()

  yearsDF= yearsDF.groupBy("annees").agg(count("annees"))
  yearsDF.show()

  yearsDF.registerTempTable("nbCompteParAnnes")
  display(sqlContext.sql("select * from nbCompteParAnnes"))

  """

}
