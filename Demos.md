# Demos en Apache Spark 2.X de DataFrame y DataSets

Los ejemplos y demos se correran en el shell de Spark. El siguente comando indica la ejecucción con numero restringido de ejecutores (4), cada ejecutor con 2 cores y 9GB de memoria ram. Adicional para restringir los ejecutores creados, se se realiza la configuración de los parámetros `spark.dynamicAllocation.minExecutors` y `spark.dynamicAllocation.maxExecutors=4`.
```sh
/opt/spark2/bin/spark-shell --num-executors 4 \ 
--executor-cores 2 --executor-memory 9G \
--conf spark.dynamicAllocation.minExecutors=4 \
--conf spark.dynamicAllocation.maxExecutors=4
```

## Datasets Vs DataFrames

Los ejemplos se basarán en un DataFrame y Dataset cargados directamente de Hive.

```scala
import org.apache.spark.sql._
val testDf = spark.table("testdf")
test.show(5) //Se muestran los 5 primeros elementos
```
### DataFrame:
Accedo al row y el shell (Compilador admite el codigo)
```scala

val dfX2 = testDf.map(row=>row.getLong(3)*2)
// Al Ejecutar: Error!!!!
dfX2.show(5)
// Se tiene que saber el tipo de dato que voy extraer del ROW y el Compilador
// detecta el error
val dfX2 = testDf.map(row=>row.getDouble(3)*2)
dfX2.show(5)

// #### crear un case class
case class TX(idn: Int, date: Long, cuotas: Double, monto: Double, documentoclientec: String)
val testDf = spark.table("testdf").as[TX]
val dfX2 = testDf.map(row=>row.monto*2)
dfX2.show(5)


/*********
DEMO 2
**********/

// # UDF and WithColumn
val dfX2=testDf.withColumn("montoX2", testDf("monto")*2)
dfX2.show(5)

// # crear la UDF
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
// # registrar la udf
val sqlCocCal = udf(cocCal)
// aplicar la UDF with Column
val dfX2=testDf.withColumn("cuotaMensual", sqlCocCal(col("monto"), col("cuotas"), lit(0.0)))
// check the
dfX2.filter("cuotas>2").show(10)
// verificar cuotas 0
dfX2.filter("cuotas==0").show(10)


/*********
DEMO 3
**********/

// # Windows
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._

// historica
val v1= Window.orderBy(col("date").asc).partitionBy("documentoclientec").rowsBetween(Long.MinValue, -1)
val dfV1=testDf.withColumn("avg", avg(col("monto")).over(v1))
dfV1.show(20)
dfV1.filter(col("documentoclientec")==="45547784").show(10)



// rango
val vm = 60*24*3600
val v2 = Window.orderBy(col("date").asc).partitionBy(col("documentoclientec")).rangeBetween(-vm,-1)
val dfV2=testDf.withColumn("cuenta60dias", count(col("idn")).over(v2))
dfV2.withColumn("date", from_unixtime(col("date"))).filter(col("documentoclientec")==="45547784").show(10)


/*********
DEMO 4
**********/

// groupBy
testDf.groupBy("documentoClientec").show
// se tiene que definir una agregacion
testDf.groupBy("documentoClientec").count.orderBy(col("count").desc).show(10)

testDf.groupBy("documentoClientec", "cuotas").count.orderBy(col("count").desc).show(10)

testDf.filter(col("documentoClientec")=!="NULL").
groupBy("documentoClientec", "cuotas").count.orderBy(col("count").desc).show(30)
```
