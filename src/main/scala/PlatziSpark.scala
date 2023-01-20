import org.apache.spark.sql.SparkSession

object PlatziSpark extends App {

  /*Este es un programa ejemplo, en el cual trato de reforzar lo aprendido en el curso de DATIO*/

  /*Crear la sesion a traves de SparkSession*/
  val spark = SparkSession.builder().master("local[*]").appName("PlatziSpark").getOrCreate()
  //Crear la variable con sc que es un SparkContext
  val sc = spark.sparkContext

  // This import is needed to use the $-notation

  import spark.implicits._


  /*Las formas de cargar los archivos la encontre en la siguiente pagina */
  /*https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html*/
  /* Crear una variable con el PATH de nuestros archivos en formato csv*/
  val chargefileCSV = spark.read.format("csv")
    .option("sep", ",") //Separacion por comas
    .option("inferSchema", "true")
    .option("header", "true") //Indicar que la tabla tiene encabezados
    .load("C:/Users/joni_/Downloads/paises.csv") //Indicar la ruta del archivo que queremos abrir

  /*Muestra la tabla*/
  chargefileCSV.show()
  /*
  +---+--------------------+-----+
  | id|              equipo|sigla|
  +---+--------------------+-----+
  |  1|         30. Februar|  AUT|
  |  2|A North American ...|  MEX|
  |  3|           Acipactli|  MEX|
  |  4|             Acturus|  ARG|
  |  5|         Afghanistan|  AFG|
  |  6|            Akatonbo|  IRL|
  |  7|            Alain IV|  SUI|
  |  8|             Albania|  ALB|
  |  9|              Alcaid|  POR|
  | 10|            Alcyon-6|  FRA|
  | 11|            Alcyon-7|  FRA|
  | 12|           Aldebaran|  ITA|
  | 13|        Aldebaran II|  ITA|
  | 14|              Aletta|  IRL|
  | 15|             Algeria|  ALG|
  | 16|         Ali-Baba II|  SWE|
  | 17|         Ali-Baba IV|  SUI|
  | 18|         Ali-Baba IX|  SUI|
  | 19|         Ali-Baba VI|  SUI|
  | 20|             Allegro|  FRA|
  +---+--------------------+-----+
  only showing top 20 rows*/

  /*Print the schema in a tree format*/
  chargefileCSV.printSchema()
  // root
  // |-- id: integer (nullable = true)
  // |-- equipo: string (nullable = true)
  // |-- sigla: string (nullable = true)

  /*---------------------------------------------------------------------------------*/

  /*Guarde el nombre de la tabla cuando se cargo por algo mas comprensible y corto*/
  val tablecountry = chargefileCSV

  /*Seleccionar una columna con dos renglones*/
  tablecountry.select("equipo").limit(2).show()
  /*
  +--------------------+
  |              equipo|
  +--------------------+
  |         30. Februar|
  |A North American ...|
  +--------------------+
  */

  /*Usar un filtro solo para traer los datos con ciertas caracteristicas*/
  tablecountry.filter($"id" < 5).show
  /*
  +---+--------------------+-----+
  | id|              equipo|sigla|
  +---+--------------------+-----+
  |  1|         30. Februar|  AUT|
  |  2|A North American ...|  MEX|
  |  3|           Acipactli|  MEX|
  |  4|             Acturus|  ARG|
  +---+--------------------+-----+*/


  /*Running SQL Queries Programmatically
  /The sql function on a SparkSession enables applications to run SQL queries programmatically
  /and returns the result as a DataFrame.
  /Register the DataFrame as a SQL temporary view
  /Con crear una vista temporal podemos hacer un QUERY con las sentencias comunes de SQL */
  tablecountry.createOrReplaceTempView("paises")
  val sqlDF = spark.sql("SELECT * FROM paises WHERE sigla ='MEX'")
  sqlDF.show()

  /*
  +----+--------------------+-----+
  |  id|              equipo|sigla|
  +----+--------------------+-----+
  |   2|A North American ...|  MEX|
  |   3|           Acipactli|  MEX|
  | 193|           Chamukina|  MEX|
  | 656|              Mexico|  MEX|
  | 657|            Mexico-1|  MEX|
  | 658|            Mexico-2|  MEX|
  | 700|          Nausikaa 4|  MEX|
  |1053|              Tlaloc|  MEX|
  |1168|              Xolotl|  MEX|
  +----+--------------------+-----+
*/

  sc.stop() // Se cierra la sesión del SparkContext

}
