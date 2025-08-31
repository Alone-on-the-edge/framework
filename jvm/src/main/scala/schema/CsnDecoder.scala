package com.adp.ssot.schema

import java.lang.{Long => JLong}
import java.math.BigInteger
import java.nio.ByteBuffer
import java.util.UUID
import scala.util.{Failure, Try}

sealed trait CsnDecoder {
    protected final def splitCsn(csn: String, splitAt: Either[Char, String], expectedSplitLengths: seq[Int]): Array[String] = {
        val csnSplits = splitAt match {
            case Left(l) = csn.trim.split(l)
            case Right(r) = csn.trim.split(r)
        }
        val csnSplitLengths = csnSplits.map(_.length).toSeq
        if (csnSplitLengths != expectedSplitLengths) {
            throw new Exception(
                f"""CSN split sizes (value = $csnSplitLengths)
                    do not match expected split sizes (value = $expectedSplitLengths.)"""
            )
        }
        csnSplits
    }

    def decode(csn: String) : BigInteger
}

object MysqlCsnDecoder extends CsnDecoder {

    def decode(csn: String): BigInteger = {
        val csnSplits = csn.split(':')
        csnSplits.length match {
            case 2 => decode2PartCsn(csn)
            case 3 => decode3PartCsn(csn)
            case _ => throw new Exception("Mysql CSN can only be 2 or 3 parts")
        }
    }

    /* 
        Mysql csn has this format "000000000000000005083:0000000021806404" for 2 part CSN.
        The LSN in Mysql is 64-bit/8-bytes.
        The first half represents redo log file number and the second half is the offset within that file.
    */
    private def decode2PartCsn(csn: String) = {
        val csnSplits = splitCsn(csn,
            splitAt = Left(':),
            expectedSplitLengths = Seq(21, 15))
            .map(s => JLong.parseLong(s))

        val buffer = ByteBuffer.allocate(8)
        buffer 
            .putInt(csnSplits(0).asInstanceOf[Int])
            .putInt(csnSplits(1).asInstanceOf[Int])
        buffer.position(0)
        new BigInteger(buffer.array())
    }

    /* 
        Mysql csn has this format "0000000000000000000001:f77024f9-f4e3-11eb-a052-0021f6e03f10:0000000000000000010654" for 3 part CSN.
        The LSN in Mysql is 64-bit/8-bytes.
        The first half represents redo log file number and the third part is the offset within that file.
        The middle part or 2nd part is the server UUID - we want to validate thats its a UUID and ignore.
        https://docs.oracle.com/en/middlewate/goldengate/core/21.3gghdb/using-group-replication.html#GUID-some numbers
    */
    private def decode3PartCsn(csn: String) = {
        val csnSplits = splitCsn(csn,
            splitAt = Left(':'),
            expectedSplitLengths = Seq(20,36, 19))
        
        Try(UUID.fromString(csnSplits(1))) match {
            case Failure(_) => throw new Exception("The middle part of CSN is not a UUID.")
            case _ =>
        }
        val numericCsn = new BigInteger(csnSplits(2))
        if (numericCsn.toString.length > 38) {
            throw new Exception("Invalid CSN, number of digits for decoded CSN should be <= 38")
        } else {
            numericCsn
        }
    }
}


object SqlServerCsnDecoder extends CsnDecoder {
    /* 
        sql server csn has this format "0000009d:00001c90:000d-ffffffff-0000009d:00001c90:000d",
        when split on -ffffffff- first and second halves are assumed to be the same.
        also. the value is encoded in hexadecimal, so we'll convert this to bigint for sequencing purposes.
    */
    def decode(csn: String): BigInteger = {
        val Array(csnSplit1, csnSplit2) = splitCsn(csn,
        splitAt = Right("-ffffffff-"),
        expectedSplitLengths = Seq(22, 22))
        if (csnSplit1 != csnSplit2) {
            throw new Exception (
                f"""SQl server CSN format error: Expectation failed - CSN first part (value = $csnSplit1)
                    is not equal to second part (value = $csnSplit2)""")
        }
        val csnHexSplits = splitCsn(csnSplit1,
            splitAt = Left(':'),
            expectedSplitLengths = Seq(8,8,4))
        val csnSplits = csnHexSplits.map(s => JLong.parseLong(s, 16))
        val buffer = ByteBuffer.allocate(10)
        buffer 
            .putInt(csnSplits(0).asInstanceOf[Int])
            .putInt(csnSplits(1).asInstanceOf[Int])
            .putShort(csnSplits(2).asInstanceOf[Short])
        buffer.position(0)
        new BigInteger(buffer.array())
    }
}

object PostgresCsnDecoder extends CsnDecoder {
    /* 
        postgres csn has this format "0000000001/314752B0".
        It has 2 parts which are both encoded in hexadecimal.
        The hexadecimal here is converted to bigint for sequencing purposes.
    */
    def decode(csn: String) : BigInteger = {
        val csnHexSplits = splitCsn(csn, splitAt = Left('/'), expectedSplitLengths = Seq(8,8))
        val csnSplits = csnHexSplits.map(s => JLong.parseLong(s, 16))
        val buffer ByteBuffer.allocate(8)
        buffer 
            .putInt(csnSplits(0).asInstanceOf[Int])
            .putInt(csnSplits(1).asInstanceOf[Int])
        buffer.position(0)
        new BigInteger(buffer.array())
    }
}