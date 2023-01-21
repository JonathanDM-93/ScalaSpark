/* La finalidad de este ejercicio sera practicar los fundamentos de trabajar con SCALASPARK
para esto usaremos datos de sismos descargados de la pagina oficial del SSM (Servicio Sismológico Mexicano)
Trataremos de hacer unos consultas SQL más complejas. Nos apoyaremos de la pagina oficial de APACHE SPARK
 */
import org.apache.spark.sql.SparkSession

object SismoSpark extends App{

  /*Crear la sesion a traves de SparkSession*/
  val spark = SparkSession.builder().master("local[*]").appName("SismoSpark").getOrCreate()
  //Crear la variable con sc que es un SparkContext
  val sc = spark.sparkContext

  /*Las formas de cargar los archivos la encontre en la siguiente pagina */
  /*https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html*/
  /* Crear una variable con el PATH de nuestros archivos en formato csv*/
  val chargefileCSV = spark.read.format("csv")
    .option("sep", ",") //Separacion por comas
    .option("inferSchema", "true")
    .option("header", "true") //Indicar que la tabla tiene encabezados
    .load("C:/Users/joni_/Downloads/SSNMX_catalogo.csv") //Indicar la ruta del archivo que queremos abrir

  /* Algo interesante es que este archivo trae una introducción de que es el archivo veamos como
  lo maneja. Bueno al final si tuve que quitar esos encabezados porque generaban problemas al cargar la tabla.
  */

  /*Muestra las tablas*/
  chargefileCSV.show()

  /*Imprimir el esquema en formato de arbol*/
  chargefileCSV.printSchema()


}
