import org.apache.spark.sql.SparkSession




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

  //On va modifier le nom des colonnes commençant par "n."
"""
  for (x <- entity.columns) {
    if (x.startsWith("n.")) {
      entity = entity.withColumnRenamed(x, x.drop(2))
    }
  }
  entity.columns"""
  //val filterData = entity.select("*").where("n.country_codes=CYM")
  //filterData.show(10)


}
