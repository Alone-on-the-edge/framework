package com.adp.ssot.schema

import kafkashaded.org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.Row

import java.util.Properties 


object LobUtils extends java.io.Serializable {

    private var producer: KafkaProducer[Array[Byte], Array[Byte]] = _

    private def getProducer(kafkaPros: Map[String, String]) : KafkaProducer[Array[Byte], Array[Byte]] = {
        val props = new Properties()
        props.put("value.serializer", "kafkashaded.org.apache.kafka.common.serialization.ByteArraySerializer")
        props.put("key.serializer", "kafkashaded.org.apache.kafka.common.serialization.ByteArraySerializer")
        props.put("compression.type", "gzip")
        props.put("reconnect.backoff.ms", "60000")
        props.put("enable.idempotence", "true")
        props.put("batch.size", "16384")
        kafkaProps.foreach(x => props.put(x._1, x._2))
        producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
        producer
    }

    private[ssot] = val writeToKafka = (row: Row) => {
        if (producer == null) {
            val kafkaProps = row.getAs[Map[String, String]]("kafka_conf")
            producer = getProducer(kafkaProps)
        }
        
        var numLobRetries = 0;
        if (row.getAs[Array[Byte]]("header_value") != null) {
            numLobRetries = BigInt(row.getAs[Array[Byte]]("header_value)).toInt + 1
        }
        val topic = row.getAs[String]("__topic)
        val optype = row.getAs[String]("op_type)
        val lobCols = row.getAs[String]("lob_cols)

        var isNullLob = false
        if (opType == "U") {
            for (col <- lobCols) {
                if (row.getAs[String](col) == null) {
                    isNullLob = true
                }
            }
        }

        if (isNullLob && numLobRetries < 1) {
            val key = row.getAs[Array[Byte]]("original_key")
            val value = row.getAs[Array[Byte]]("original_value")
            val partition = row.getAs[Int]("partition")
            val ProducerRecord = new ProducerRecord(topic, partition, key, value)
            ProducerRecord.headers().add("X-LOB-RETRIES", BigInt(numLobRetries).toByteArray)
            producer.send(ProducerRecord).get()
            producer.flush()
            true
        }
        else {
            false
        }
    }
}