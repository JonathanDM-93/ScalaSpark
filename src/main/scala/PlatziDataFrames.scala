//En este ejercicio se crea un DF desde cero.
//Lo primero que tenemos que hacer es cargar los modulos necesarios
//Estos ejemplos de codigo son en base a lo aprendido en las clases de Platzi modificandolos a Scala

import org.apache.spark.sql.SparkSession
//All data types of Spark SQL are located in the package org.apache.spark.sql.types. You can access them by doing
import org.apache.spark.sql.types._

object PlatziDataFrames extends App {

  /*Crear la sesion a traves de SparkSession*/
  val spark = SparkSession.builder().master("local[*]").appName("PlatziDataFrames").getOrCreate()
  //Crear la variable con sc que es un SparkContext
  val sc = spark.sparkContext


  /*Las formas de cargar los archivos la encontre en la siguiente pagina */
  /*https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html*/
  /* Crear una variable con el PATH de nuestros archivos en formato csv*/
  val chargefileCSV = spark.read.format("csv")
    .option("sep", ",") //Separacion por comas
    .option("inferSchema", "true")
    .option("header", "true") //Indicar que la tabla Liene encabezados
    .load("C:/Users/joni_/Downloads/juegos.csv") //Indicar la ruta del archivo que queremos abrir

  /*Ya se cargo la ruta del archivo.
  Ahora se podemos crear la estructura que querramos que tenga el archivo...
  cuando lo leamos independientemente como venga del archivo .csv*/

  /*En la siguiente página se puede ver lo relacionado a Struct type...
  * https://spark.apache.org/docs/3.3.1/sql-ref-datatypes.html#supported-data-types
  * StructField(name, dataType, nullable): Represents a field in a StructType.
  * The name of a field is indicated by name.
  * The data type of a field is indicated by dataType. nullable is used to indicate if values of these fields can have null values.*/

  /*CREACION DEL ESQUEMA*/
  val nameSchema = StructType(
    Array(
      StructField("juego_id",IntegerType,nullable = false),
      StructField("anio",StringType,nullable = false),
      StructField("temporada",StringType,nullable = false),
      StructField("ciudad",StringType,nullable = false)))

  val uploadShema = spark.read.schema(nameSchema).format("csv")
    .option("sep", ",") //Separacion por comas
    .option("inferSchema", "true")
    .option("header", "true") //Indicar que la tabla Liene encabezados
    .load("C:/Users/joni_/Downloads/juegos.csv")

  //df.show()


  //Crear una consulta a traves de comando comunes de SQL
  uploadShema.createOrReplaceTempView("viewSQL")
  val sqlDF = spark.sql("SELECT anio FROM viewSQL")
  sqlDF.show()

  sc.stop()

  // En resumen sobre este ejercicio es crear un esquema a traves de los types de SQL con StructType
  //Despues cargamos un archivo .csv al esquema para poder visualizarlo.
  //Por ultimo se genero un query para ver los años

}

