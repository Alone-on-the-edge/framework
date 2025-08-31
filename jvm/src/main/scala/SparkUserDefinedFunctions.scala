package com.adp.ssot

import com.adp.ssot.schema.AvroSchemaUtils.{preprocessSchema => preprocessAvroSchema}
import com.adp.ssot.schema.LobUtils.writeToKafka
import com.adp.ssot.schema.{MysqlCsnDecoder, PostgresCsnDecoder, SqlServerCsnDecoder}
import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.spark.sql.SparkSession 

import java.math.BigInteger 


object SparkUserDefinedFunctions {

    private val decodeCsn = (csn: String, dbType: String) => {
        val dbTypeLower = dbType.toLowerCase.trim
        if (dbType == "oracle"){
            csn
        } else {
            val decodedCsn = if (dbTypeLower == "postgres") {
                PostgresCsnDecoder.decode(csn)
            } else if (dbTypeLower == "mysql") {
                MysqlCsnDecoder.decode(csn)
            } else if (dbTypeLower == "sqlserver") {
                SqlServerCsnDecoder.decode(csn)
            } else {
                throw new Exception (s"Unsupported database type $dbType")
            }
            if (decodeCsn == BigInteger.ZERO) {
                throw new Exception(s"Invalid csn value '$csn' received from golden gate")
            } else {
                String.valueOf(decodedCsn)
            }
        }
    }

    private val getSchemaFingerprint = (schema: String) => 
        SchemaNormalization.parsingFingerprint64(new Schema.Parser().parse(schema))

        private val preprocessSchema = (schema: String, preserveColumnCase: Boolean, maxDecimalScale: String) =>
            preprocessAvroSchema(schema, preserveColumnCase, maxDecimalScale)

        def register() : Unit = {
            SparkSession.getActiveSession.foreach {spark =>
                spark.udf.register("generate_schema_fingerprint", getSchemaFingerprint)
                spark.udf.register("preprocess_schema", preprocessSchema)
                spark.udf.register("write_to_kafka", writeToKafka)
                spark.udf.register("decode_csn", decodeCsn)
            }
        }
}