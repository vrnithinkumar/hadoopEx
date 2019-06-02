println("Start!");

val start_time = System.nanoTime

val cus = sc.textFile("/home/sparkuser/SparkData/cAo/customer.tbl")
val ord = sc.textFile("/home/sparkuser/SparkData/cAo/orders.tbl")
val ordS = sc.textFile("/home/sparkuser/SparkData/cAo/ordersSmall")
val fOrders = ord.map(line => line.split('|')).filter(row => row(2)=="F")
val autoMobCus = cus.map(line => line.split('|')).filter(row => row(6) == "AUTOMOBILE")
val nation4Cus = autoMobCus.filter(row => row(3) == "4")

val cusPair = nation4Cus.map(c => (c(0), c))

val ordPair = fOrders.map(o => (o(1), o))

val joinRes = cusPair.join(ordPair)
val resWithKeyNClerk = joinRes.map(r => (r._1, r._2._2(6)))
  
resWithKeyNClerk.saveAsTextFile("/home/sparkuser/SparkData/KeyNClerk")	

println("End time: "+(System.nanoTime-start_time)/1e6+"ms")

System.exit(0)
