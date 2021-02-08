import org.apache.spark.sql.SparkSession

object ParadiseOther extends App{

  /**
   * Creation de la session spark
   */
  val spark = SparkSession.builder().appName(name = "ParadisOther Fiscal").master(master= "local[*]").getOrCreate()

  /**
   *
   */
  println("Affiche les données contenu dans le dossier Other et le schema")
  var other = spark.read.option("inferSchema", "true").option("header", "true").csv(path= "data/paradise_papers.nodes.other.csv")
  other.show(20)
  other.printSchema()

  for (x <- other.columns) {
    if (x.startsWith("n.")) {
      other = other.withColumnRenamed(x, x.drop(2))
    }
  }
  println("Affiche le noms des colones après modification")
  other.columns
  //On souhaite récupérer et afficher le nom des compagnies impliquées dans l'investigation
  other.createOrReplaceTempView("other")
  var compagnies = spark.sql("SELECT name FROM other")
  println("affiche le nom des compagnies impliquées")
  for (x <- compagnies.collect()){
    println(x)
  }

}
