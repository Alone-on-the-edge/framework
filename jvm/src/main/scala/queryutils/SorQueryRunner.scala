package com.adp.ssot.queryutils

import org.apache.spark.sql.SparkSession

import java.math.{BigDecimal => JBigDecimal}
import java.util.{Map => JMap}
import scala.collection.JavaConverters._
import scala.collection.parallel.ForkJoinTaskSupport

object SorQueryRunner{

    //responsible for querying tables in the aor and returning the results.
    def querySor(table: JMap[String, Any], dbConnectionConfig: JMap[String, String]) : JMap[String, Any] = {
        val spark = SparkSession.builder().getOrCreate()

        val driver = dbConnectionConfig.get("driver")
        val username = dbConnectionConfig.get("username")
        val password = dbConnectionConfig.get("password")
        val maxTimeoutSeconds = dbConnectionConfig.get("max_timeout_seconds").toInt
        val jdbcUrl = dbConnectionConfig.get("jdbc_url")
        val attemptNum = dbConnectionConfig.get("attempt_num").toInt

        val query = table.get("query").toString().replace("<degree_of_parallelism>",s"${attemptNum * 2}")

        try{
            val queryStartTime = System.currentTimeMillis()

            val result = spark.read.format("jdbc")
                .option("url", jdbcUrl)
                .option("driver", driver)
                .option("oracle.jdbc.timezoneAsRegion", "false")
                .option("user", username)
                .option("password", password)
                .option("query", query)
                .option("numPartitions", "1")
                .option("queryTimeout", maxTimeoutSeconds)
                .load()
                .collect()(0)(0)

            val queryEndTime = System.currentTimeMillis()
            val queryDurationSeconds: Float = ((queryEndTime - queryStartTime).toFloat/1000)

            table.put("is_error_src", 0)
            table.put("result", new JBigDecimal(result.toString))
            table.put("error_msg_src", null)
            table.put("attempt_num", dbConnectionConfig.get("attempt_num))
            table.put("query_duration_seconds", queryDurationSeconds)
            table.put("query", query)

        }catch{
            case e: Throwable => {
                if(attemptNum < 3 ){
                    dbConnectionConfig.put("attempt_num", (attemptNum + 1).toString)
                    dbConnectionConfig.put("max_timeout_seconds", (maxTimeoutSeconds + 900).toString)

                    Thread.sleep(30) //30 seconds sleep before retry

                    val retry_table = querySor(table, dbConnectionConfig)
                    return retry_table

                }
                else{
                    table.put("is_error_src", 1)
                    table.put("error_msg_src", e.toString())
                    table.put("result", null)
                    table.put("attempt_num", dbConnectionConfig.get("attempt_num))
                    table.put("query_duration_seconds", null)
                    table.put("query", query)
                }
            }
        }
        return(table)
    }

    //responsible for parallelising queries on sor databases
    def paralleliseOnDatabase(tableList: java.util.ArrayList[java.util.ArrayList[Any]], numParallelConnections: Integer) = {
        /* 
        args
        tableList: array of tuple (the function is invoked from a python script using py4j, so tuple becomes ArrayList)
        -- the tuple has 2 maps
        -- the details of table to be queried 
        -- the connection details of the database 
        numParallelConnections: number of parallel connections to establish on the db 
        */
        val parallelList = tableList.asScala.toList.par
        parallelList.tasksupport = new ForkJoinTaskSupport(
            new java.util.concurrent.ForkJoinTaskSupport(numParallelConnections))

        parallelList.map(x => querySor(x.asScala(0).asInstanceOf[JMap[String, Any]], x.asScala(1).asInstanceOf[JMap[String, String]])).toList.asJava
    }
}