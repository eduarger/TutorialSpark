# Demos en Apache Spark 2.X de DataFrame y DataSets

Los ejemplos y demos se correran en el shell de Spark. El siguente comando indica la ejecucción con numero restringido de ejecutores (4), cada ejecutor con 2 cores y 9GB de memoria ram. Adicional para restringir los ejecutores creados, se se realiza la configuración de los parámetros `spark.dynamicAllocation.minExecutors` y `spark.dynamicAllocation.maxExecutors=4`.
```sh
/opt/spark2/bin/spark-shell --num-executors 4 \ 
--executor-cores 2 --executor-memory 9G \
--conf spark.dynamicAllocation.minExecutors=4 \
--conf spark.dynamicAllocation.maxExecutors=4
```

## Datasets Vs DataFrames

Los ejemplos se basarán en un DataFrame y Dataset cargados directamente de una tabla Hive. La idea es mostrar la diferencia entre los dos API, donde se observara que el DataSet es un tipo de datos con el tipo definido.

```scala
import org.apache.spark.sql._
val testDf = spark.table("testdf")
testDf.show(5) //Se muestran los 5 primeros elementos
```
Adicional muestro el esquema de la tabla que se acabo de crear:
```scala
testDf.printSchema()
root
 |-- idn: integer (nullable = true)
 |-- date: long (nullable = true)
 |-- cuotas: double (nullable = true)
 |-- monto: double (nullable = true)
 |-- documentoclientec: string (nullable = true)
```

### DataFrame:
 Quiero multiplicar cada elemento de la columna por 2.0. Por lo tanto Accedo al row y al el shell obtengo un ```long``` de la columna 3 (monto) y lo multiplico por 2.
```scala
val dfX2 = testDf.map(row=>row.getLong(3)*2)
```
Al definir la anterior acción, el compilador no verificara que efectivamente se puede obtener un ```long``` de la columna 3 (y como se mostro en el esquema la columna 3 monto es un double), por lo tanto al realizar la siguente acción la operacion fallará:
```scala
dfX2.show(5)
```scala
Se tiene que saber el tipo de dato que voy extraer del ROW y el Compilador:

```scala
val dfX2 = testDf.map(row=>row.getDouble(3)*2)
dfX2.show(5)
```

#### DataSet
Para el dataset se debe definir un  ```case class``` con la estructura: 

```scala
case class TX(idn: Int, date: Long, cuotas: Double, monto: Double, documentoclientec: String)
```
Luego a un dataframe se le puede hacer cast con el case class para tener definido el tipo de cada columna
```scala
val testDf = spark.table("testdf").as[TX]
```
Ya con el cast definido me puede referir directamente a la columna y el tipo estaria asegurado por el ```case class```.
```scala
val dfX2 = testDf.map(row=>row.monto*2)
dfX2.show(5)
```


## Operaciones Horizontales: UDF

# UDF and WithColumn
```scala
val dfX2=testDf.withColumn("montoX2", testDf("monto")*2)
dfX2.show(5)
``` 

creación de una UDF mas compleja

```scala
val cocCal = (monto: Double,  cuotas: Double, valDiv0: Double) => {
      var cociente=0.0
      try {
       cociente ={
       if(cuotas==0.0)
          valDiv0
       else
          monto/cuotas
      }

    } catch {
      case _: Throwable => valDiv0

             }
        // return
        cociente
             }
 ```
 
Se debe registrar la udf
```scala
val sqlCocCal = udf(cocCal)
```

Y con el mismo metodo with Column se aplica la UDF
```scala
val dfX2=testDf.withColumn("cuotaMensual", sqlCocCal(col("monto"), col("cuotas"), lit(0.0)))
// verificar cuotas>2
dfX2.filter("cuotas>2").show(10)
// verificar cuotas 0
dfX2.filter("cuotas==0").show(10)
```
## Windows: operaciones Horizontales
Para trabajar con ventanas se importan las siguentes librerías: 
```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
```

### ventana tipo row
si se quiere solo tener ventanas definidas por algun tipo en concreto de indice:

```scala
val v1= Window.orderBy(col("date").asc).partitionBy("documentoclientec").rowsBetween(Long.MinValue, -1)
val dfV1=testDf.withColumn("avg", avg(col("monto")).over(v1))
dfV1.show(20)
dfV1.filter(col("documentoclientec")==="45547784").show(10)
```
### ventana tipo range

Si se quiere tener definida un vetana con base a un valor actual

```scala
val vm = 60*24*3600
val v2 = Window.orderBy(col("date").asc).partitionBy(col("documentoclientec")).rangeBetween(-vm,-1)
val dfV2=testDf.withColumn("cuenta60dias", count(col("idn")).over(v2))
dfV2.withColumn("date", from_unixtime(col("date"))).filter(col("documentoclientec")==="45547784").show(10)
```
## Operaciones sobre grupos
no se pueden obtener grupos sin ser agregados, la siguente instrucción es un error: 
```scala
testDf.groupBy("documentoClientec").show
```
Se debe definir una agregacion
```scala
testDf.groupBy("documentoClientec").count.orderBy(col("count").desc).show(10)
testDf.groupBy("documentoClientec", "cuotas").count.orderBy(col("count").desc).show(10)
testDf.filter(col("documentoClientec")=!="NULL").
groupBy("documentoClientec", "cuotas").count.orderBy(col("count").desc).show(30)
```
### UDAF
Se pueden definir funciones de agregación de usuario construyendolas creando un clase heredada de ```UserDefinedAggregateFunction``` y definiendo 4 etapas: ```initialize```, ```update```, ```merge``` y ```evaluate```.

```scala
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class CustomMean() extends org.apache.spark.sql.expressions.UserDefinedAggregateFunction {

  // Input Data Type Schema
  def inputSchema: StructType = StructType(Array(StructField("item", DoubleType)))

  // Intermediate Schema
  def bufferSchema = org.apache.spark.sql.types.StructType(Array(
    StructField("sum", DoubleType),
    StructField("cnt", LongType)
  ))

  // Returned Data Type .
  def dataType: DataType = DoubleType

  // Self-explaining
  def deterministic = true

  // This function is called whenever key changes
  def initialize(buffer: MutableAggregationBuffer) = {
    buffer(0) = 0.toDouble // set sum to zero
    buffer(1) = 0L // set number of items to 0
  }

  // Iterate over each entry of a group
  def update(buffer: MutableAggregationBuffer, input: Row) = {
    buffer(0) = buffer.getDouble(0) + input.getDouble(0)
    buffer(1) = buffer.getLong(1) + 1
  }

  // Merge two partial aggregates
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row) = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  // Called after all the entries are exhausted.
  def evaluate(buffer: Row) = {
    buffer.getDouble(0)/buffer.getLong(1).toDouble
  }

}
```
Luego se aplica con la ```UDAF``` instanciando la clase creada y aplicandola con el metodo ```agg```
```scala
val testDf = spark.table("testdf")
val custom_mean = new CustomMean()
testDf.
filter(col("documentoClientec")=!="NULL").
groupBy("documentoClientec").agg(
custom_mean(testDf("monto")).as("custom_mean"),
avg("monto").as("avg")).
show()
testDf.
.filter(col("documentoClientec")=!="NULL").
.groupBy(col("monto"))
.agg(custom_mean(instances.col(colCOL_LABEL))
.as("custom_mean"))
```
