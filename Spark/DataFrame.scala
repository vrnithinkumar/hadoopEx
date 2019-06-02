import org.apache.spark.sql.types._

println("Start!");
val start_time = System.nanoTime
// --------Start--------

val schemaCus = StructType(Array(
    StructField("c_cusKey", IntegerType, true),
    StructField("cName", StringType, true),
    StructField("adress", StringType, true),
    StructField("nationKey", IntegerType, true),
    StructField("phone", StringType, true),
    StructField("acctbal", DoubleType, true),
    StructField("mktsegment", StringType, true),
    StructField("comment", StringType, true)))

val schemaOrd = StructType(Array(
    StructField("orderKey", IntegerType, true),
    StructField("o_cusKey", IntegerType, true),
    StructField("orderstatus", StringType, true),
    StructField("totalprice", DoubleType, true),
    StructField("orderdate", StringType, true),
    StructField("orderpriority", StringType, true),
    StructField("c_clerk", StringType, true),
    StructField("shippriority", IntegerType, true),
    StructField("comment", StringType, true)))

val dfCus = spark.read.format("com.databricks.spark.csv").schema(schemaCus).option("delimiter", "|").load("/home/sparkuser/SparkData/cAo/customer.tbl")
val dfOrd = spark.read.format("com.databricks.spark.csv").schema(schemaOrd).option("delimiter", "|").load("/home/sparkuser/SparkData/cAo/orders.tbl")

val result = dfCus.where($"nationKey" === "4").where($"mktsegment" === "AUTOMOBILE").join(dfOrd.where($"orderstatus" ==="F"), $"c_cusKey" === $"o_cusKey").select($"c_cusKey", $"c_clerk")

println("TotalCount: "+ result.count())
result.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "|").save("/home/sparkuser/SparkData/KeyNClerk/output.tbl")

// -------End---------

println("End time: "+(System.nanoTime-start_time)/1e6+"ms")
System.exit(0)
