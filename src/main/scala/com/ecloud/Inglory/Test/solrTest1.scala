package com.ecloud.Inglory.Test


import java.net.URL

import com.github.seratch.scalikesolr._
import com.github.seratch.scalikesolr.request.query.Query
import org.apache.solr.client.solrj.SolrRequest.METHOD
//import org.apache.solr.client.solrj.impl.{BinaryResponseParser, CloudSolrServer}

/**
 * Created by sunlu on 17/3/1.
 */
object solrTest1 {
  def main(args: Array[String]) {
/*
    val client = Solr.httpServer(new URL("http://localhost:8983/solr")).newClient
    val request = new QueryRequest(writerType = WriterType.JavaBinary, query = Query("author:Rick")) // faster when using WriterType.JavaBinary
    val response = client.doQuery(request)
    println(response.responseHeader)
    println(response.response)
    response.response.documents foreach {
      case doc => {
        println(doc.get("id").toString()) // "978-1423103349"
        println(doc.get("cat").toListOrElse(Nil).toString) // List(book, hardcover)
        println(doc.get("title").toString()) // "The Sea of Monsters"
        println(doc.get("pages_i").toIntOrElse(0).toString) // 304
        println(doc.get("price").toDoubleOrElse(0.0).toString) // 6.49
      }
    }
*/
    /*
    val cloudServer = new CloudSolrServer("http://localhost:8983")
    cloudServer.setDefaultCollection("")
    cloudServer.setParser(new BinaryResponseParser()
    val response = new QueryRequest(solrQuery, METHOD.POST).process(cloudServer)
    val result = response.getResults()
*/



  }
}


