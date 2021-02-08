import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object
ParadiseOfficer extends App {

  /**
   * Creation de la session spark
   */
  val spark = SparkSession.builder().appName(name = "ParadisOfficer Fiscal").master(master = "local[*]").getOrCreate()

  /**
   *
   */
  println("affiche le schema des donnees officer")
  var officer = spark.read.option("inferSchema", "true").option("header", "true").csv(path = "data/paradise_papers.nodes.officer.csv")
  officer.printSchema()

  //Ayant des diffucultés à traiter les données avec leur nom de collones actuels,
  //On va modifier le nom des colonnes commençant par "n."

  for (x <- officer.columns) {
    if (x.startsWith("n.")) {
      officer = officer.withColumnRenamed(x, x.drop(2))
    }
  }
  println("affiche le dataframe officer et le nom des colonnes après modification")
  officer.columns
  officer.show(20)
  officer.createOrReplaceTempView("officer")

  //On souhaiterais afficher un graphe réprésentant les pays ayant le plus de comptes offshore dans nos données
  //Pour cela on crée un DF de code de pays et procède au décompte

  println("affiche la colonne codes_pays ")
  var code_pays = spark.sql("SELECT country_codes FROM officer")
  code_pays.show()

  //On remarque que sur une ligne on peut avoir plusieurs pays séparé par un ;
  //et aussi certaines lignes ne sont pas renseignés = null
  // On supprime les lignes NULL
  val nb = (code_pays.count) // le nombres de lignes initial
  code_pays = code_pays.filter("country_codes is not NULL")
  println("le nombre de lignes sans codes pays =",nb - code_pays.count)


  def compte_pays(row: Array[org.apache.spark.sql.Row]): Array[String] = {
    var liste_pays = new Array[String](324) //324 étant le nombre maximal de pays dans le monde

    for (x <- row) {
      if (x(0) != null)
        liste_pays ++= ((x(0).toString).split(";")) //Concatenate Arrays
    }
    liste_pays.filter(_ != null)
  }

  //compte_pays(sqlDF.collect)
  val liste_pays = compte_pays(code_pays.collect).distinct
  val nb_pays = liste_pays.size
  println("il y a ", nb_pays, " pays impliqué")

  //On veut à présent compter le nombre de comptes par pays et afficher un bar plot de classification des pays ayant le plus de comptes ouvert
  var df = code_pays.groupBy("country_codes").agg(count("country_codes"))
 println("affiche les pays et leurs occurences")
  df.show()

  //On compte le nombre d'occurence par pays
  def liste_occurences(row: Array[org.apache.spark.sql.Row],liste_pays: Array[String]): Array[Int] = {
    var occurence:Int = 0
    var list_occ = new Array[Int](liste_pays.length)

    for (i <- (0 to liste_pays.length-1)){
      occurence = 0
      for(y <- row){
        if((y(0).toString).contains(liste_pays(i))){
          occurence =occurence + y(1).toString.toInt   //La méthode toInt ne fonctionnant que sur des String il a fallu convertir en String d'abord
        }
      }
      list_occ(i) = occurence
    }
    list_occ
  }

  //val liste_occurence = liste_occurences(df.collect, liste_pays)
  //println(liste_occurence)



"""
//Les graphes s'affichent sur databricks et pas sur InteelliJ
  import sqlContext.implicits._
  //On peut à présent associer chaque pays à son nombre d'occurence dans le dataset donc on peut récupérer les 10 pays les plus impliqués(ceux qui ont le plus de compte)

  val CountryOccDF = spark.sparkContext.parallelize(liste_pays zip liste_occurence).toDF("pays", "nombre de comptes")
  CountryOccDF.registerTempTable("country_codes")
  display(SQLContext.sql("select * from country_codes"))
  """



}

