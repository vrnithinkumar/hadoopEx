// browser to check the jobs and master/slave status
http://10.0.2.15:4040


// Spark home directory: 
/home/sparkuser/spark-2.4.1-bin-hadoop2.7

// to start master and slave servers in standalone mode
/home/sparkuser/spark-2.4.1-bin-hadoop2.7/sbin/./start-all.sh or ./stop-all.sh

// to connect the application to the cluster 
/home/sparkuser/spark-2.4.1-bin-hadoop2.7/bin/./spark-shell --master spark://dbislab-VirtualBox:7077


/home/sparkuser/SparkData/data

## Schema

customer(c custkey, c name, c address, c nationkey, c phone, c acctbal, c mktsegment, c comment)
orders(o orderkey, o custkey → customer.c custkey, o orderstatus, o totalprice, o orderdate,
o orderpriority, o clerk, o shippriority, o comment)

## Commands to execute
:load /home/sparkuser/KeyClerk.scala
/home/sparkuser/spark-2.4.1-bin-hadoop2.7/bin/./spark-shell --master spark://dbislab-VirtualBox:7077 -i /home/sparkuser/KeyClerk.scala


1) 20288.595242ms 
2) 21605.142744ms 
3) 19070.692043ms
4) 19567.848661ms
5) 21678.6404ms  

Median : 20288.595242ms 
