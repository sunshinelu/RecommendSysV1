package com.ecloud.Inglory.hbaseOperation


import java.util

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{TableMapper, TableMapReduceUtil, MultiTableInputFormat}
import org.apache.hadoop.io.{Text, IntWritable}

/**
 * Created by sunlu on 17/3/27.
 */
object hbaseMultiTableInput {
  def main(args: Array[String]) {
/*
    val scan1 = new Scan()
//    scan1.setStartRow(start1)
//    scan1.setStopRow(end1)
    val scan2 = new Scan()
//    scan2.setStartRow(start2)
//    scan2.setStopRow(end2)
    val mtic: MultiTableInputCollection = new MultiTableInputCollection()
    mtic.Add(tableName1, scan1)
    mtic.Add(tableName2, scan2)
    TableMapReduceUtil.initTableMapperJob(mtic, TestTableMapper.class, Text.class, IntWritable.class, job1)

    */
    /*
    val scans = new util.ArrayList()

    val scan1 = new Scan()
//    scan1.setStartRow(firstRow1)
//    scan1.setStopRow(lastRow1)
    scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "table1")
    scans.add(scan1)

    val scan2 = new Scan()
    scan2.setStartRow(firstRow2)
    scan2.setStopRow(lastRow2)
    scan1.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, "table2")
    scans.add(scan2)

    TableMapReduceUtil.initTableMapperJob(scans, TableMapper.class, Text.class, IntWritable.class, job)

*/



  }
}
