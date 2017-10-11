package com.ecloud.Inglory.Solr

import java.io.IOException
import java.util.{ArrayList, Collection, Iterator}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan, HTable, ResultScanner}
import org.apache.solr.client.solrj.SolrClient
import org.apache.solr.client.solrj.impl.HttpSolrClient
import org.apache.solr.common.SolrInputDocument

/**
 * Created by sunlu on 17/10/11.
 */
object AddIndex {
  private var conf: Configuration = null

  def getHbaseCon {
    conf = HBaseConfiguration.create
    conf.addResource("mapred-site.xml")
    conf.addResource("yarn-site.xml")
    conf.addResource("hbase-site.xml")
  }

  /**
   * 获取HBASE表中的所有结果集
   */
  @throws(classOf[IOException])
  def getAllRows(tableName: String): ResultScanner = {
    getHbaseCon
    val table: HTable = new HTable(conf, tableName)
    val scan: Scan = new Scan
    val results: ResultScanner = table.getScanner(scan)
    return results
  }

  /**
   * 删除所有的索引
   */
   def purgAllIndex(solrUrl: String) {
    var solrClient: SolrClient = null
    try {
      solrClient = new HttpSolrClient(solrUrl)
      solrClient.deleteByQuery("*:*")
      solrClient.commit
    }
    catch {
      case e: Any => {
        try {
          if (null != solrClient) {
            solrClient.rollback
          }
        }
        catch {
          case e1: Any => {
            throw new RuntimeException(e1.getMessage, e1)
          }
        }
        throw new RuntimeException(e.getMessage, e)
      }
    } finally {
      if (null != solrClient) {
        try {
          solrClient.close
        }
        catch {
          case e: IOException => {
          }
        }
      }
    }
  }

  /**
   * 对相似文章结果表（ylzx_xgwz）建立索引
   */
  def addIndex_SimilarArticle(results: ResultScanner, solrUrl_similarArticleRec: String) {
    var solrClient: SolrClient = null
//    purgAllIndex(solrUrl_similarArticleRec)
    solrClient = new HttpSolrClient(solrUrl_similarArticleRec)
    try {
      val it: Iterator[Result] = results.iterator
      var i: Int = 0
      var docs: Collection[SolrInputDocument] = new ArrayList[SolrInputDocument](3000)
      while (it.hasNext) {
        val result: Result = it.next
        val document: SolrInputDocument = new SolrInputDocument
        val rowKey: String = new String(result.getRow, "utf-8")
        val articleId: String = new String(result.getValue("info".getBytes, "id".getBytes), "utf-8")
        val websitename: String = new String(result.getValue("info".getBytes, "websitename".getBytes), "utf-8")
        val manuallabel: String = new String(result.getValue("info".getBytes, "manuallabel".getBytes), "utf-8")
        val simsScore: String = new String(result.getValue("info".getBytes, "simsScore".getBytes), "utf-8")
        val t: String = new String(result.getValue("info".getBytes, "t".getBytes), "utf-8")
        val mod: String = new String(result.getValue("info".getBytes, "mod".getBytes), "utf-8")
        val simsID: String = new String(result.getValue("info".getBytes, "simsID".getBytes), "utf-8")
        val level: String = new String(result.getValue("info".getBytes, "level".getBytes), "utf-8")
        if (t != null && t.length > 0) {
          i += 1
          document.addField("id", rowKey)
          document.addField("articleId", articleId)
          document.addField("websitename", websitename)
          document.addField("manuallabel", manuallabel)
          document.addField("simsScore", simsScore)
          document.addField("t", t)
          document.addField("mod", mod)
          document.addField("simsID", simsID)
          document.addField("level", level)
          docs.add(document)
        }
        if (i % 3000 == 0 || !it.hasNext) {
          if (docs.size > 0) {
            solrClient.add(docs)
            solrClient.commit
            i = 0
            docs = new ArrayList[SolrInputDocument](3000)
          }
        }
      }
    }
    catch {
      case e: Any => {
        try {
          if (null != solrClient) {
            solrClient.rollback
          }
        }
        catch {
          case e1: Any => {
            throw new RuntimeException(e1.getMessage, e1)
          }
        }
        throw new RuntimeException(e.getMessage, e)
      }
    } finally {
      try {
        if (null != solrClient) {
          solrClient.close
        }
      }
      catch {
        case e: IOException => {
        }
      }
    }
  }

  /**
   * 对猜你喜欢推荐结果表（ylzx_cnxh）建立索引
   */
  def addIndex_GuessYouLike(results: ResultScanner, solrUrl_guessYouLikeRec: String) {
    var solrClient: SolrClient = null
//    purgAllIndex(solrUrl_guessYouLikeRec)
    solrClient = new HttpSolrClient(solrUrl_guessYouLikeRec)
    try {
      val it: Iterator[Result] = results.iterator
      var i: Int = 0
      var docs: Collection[SolrInputDocument] = new ArrayList[SolrInputDocument](3000)
      while (it.hasNext) {
        val result: Result = it.next
        val document: SolrInputDocument = new SolrInputDocument
        val rowKey: String = new String(result.getRow, "utf-8")
        val articleId: String = new String(result.getValue("info".getBytes, "id".getBytes), "utf-8")
        val manuallabel: String = new String(result.getValue("info".getBytes, "manuallabel".getBytes), "utf-8")
        val mod: String = new String(result.getValue("info".getBytes, "mod".getBytes), "utf-8")
        val rating: String = new String(result.getValue("info".getBytes, "rating".getBytes), "utf-8")
        val title: String = new String(result.getValue("info".getBytes, "title".getBytes), "utf-8")
        val userID: String = new String(result.getValue("info".getBytes, "userID".getBytes), "utf-8")
        val rn: String = new String(result.getValue("info".getBytes, "rn".getBytes), "utf-8")
        if (title != null && title.length > 0) {
          i += 1
          document.addField("id", rowKey)
          document.addField("articleId", articleId)
          document.addField("manuallabel", manuallabel)
          document.addField("rating", rating)
          document.addField("title", title)
          document.addField("mod", mod)
          document.addField("userID", userID)
          document.addField("rn", rn)
          docs.add(document)
        }
        if (i % 3000 == 0 || !it.hasNext) {
          if (docs.size > 0) {
            solrClient.add(docs)
            solrClient.commit
            i = 0
            docs = new ArrayList[SolrInputDocument](3000)
          }
        }
      }
    }
    catch {
      case e: Any => {
        try {
          if (null != solrClient) {
            solrClient.rollback
          }
        }
        catch {
          case e1: Any => {
            throw new RuntimeException(e1.getMessage, e1)
          }
        }
        throw new RuntimeException(e.getMessage, e)
      }
    } finally {
      try {
        if (null != solrClient) {
          solrClient.close
        }
      }
      catch {
        case e: IOException => {
        }
      }
    }
  }

}
