package com.adp.ssot.schema

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.Schema 
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.types._ 

import java.util.{List => JList, Map => JMap}
import scala.collection.JavaConverters._

object AvroSchemaUtils {

    private val MAX_DECIMAL_PRECISION = 38
    //private val MAX_DECIMAL_PRECISION = 6 //read from app_spec

    private val avroToStruct = (schemaAsJson: String) => {
        val avroSchema = new Schema.Parser().parse(schemaAsJson)
        SchemaConverters.toSqlType(avroSchema).dataType match {
            case st: StructType => st.json 
            case _ => throw new IllegalStateException("Avro schema must result in a struct!")
        }
    }

    // responsible for limiting the decimal precision to 38 as spark cant handle decimals greater than 38 digits
    // also responsible for converting column names to lower case 
    def preprocessSchema(schemaAsJson: String, preserveColumnCase: Boolean, maxDecmalScale: String): String = {
        def preprocessSchemaRec(avroSchemaMap: Any) : Any = avroSchemaMap match {
            case map: JMap[String, Any] =>
            val scalaMap = map.asScala
            if (!preserveColumnCase) {
                val fieldName = scalaMap.get("name").asInstanceOf[Option[String]]
                if (fieldName.isDefined) {
                    scalaMap.put("name", fieldName.get.toLowerCase)
                }
            }
            //handle large decimals
            val isBytesType = scalaMap..get("type").asInstanceOf[Option[String]].contains("bytes")
            val isDecimalLogicalType = scalaMap.get("logicalType").asInstanceOf[Option[String]].contains("decimal")
            if (isBytesType && isDecimalLogicalType) {
                val precision = scalaMap.get("precision").asInstanceOf[Option[Int]]
                if (precision.get > MAX_DECIMAL_PRECISION)
                    (scalaMap +
                    ("precision" -> MAX_DECIMAL_PRECISION) +
                    ("scale" -> maxDecmalScale.toInt)).asJava
                else
                    scalaMap.asJava
            } else {
                scalaMap.mapValues(value => preprocessSchemaRec(value)).asJava
            }
            case list: JList[_] => list.asScala.map(i => preprocessSchemaRec(i)).asJava
            case other => other
        }

        val schemaAsMap = JsonUtils.toMap(schemaAsJson)
        val preprocSchema = preprocessSchemaRec(schemaAsMap).asInstanceOf[JMap[String, Any]]
        JsonUtils.fromMap(preprocSchema)
    }

    def parseAsDDL(schemaAsJson: String) : String = {
        val sparkSQLSchema = avroToStruct(schemaAsJson)
        DataType.fromJson(sparkSQLSchema) match {
            //before and after have the same schema here - so lets select before schema for generting DDL.
            case struct: StructType => struct.fields.find(_.name == "before").get.dataType match {
                case beforeStruct: StructType => beforeStruct.toDDL
            }
        }
    }

    object JsonUtils {

        private lazy val mapper = new ObjectMapper()

        def toMap(jsonStr: String) : JMap[String, Any] = 
            mapper.readValue(jsonStr, classOf[JMap[String, Any]])

        def fromMap(map: JMap[String, Any]) : String = 
            mapper.writeValueAsString(map)
    }
}