package com.atguigu.gmall0921.realtime.utils

import java.util
import java.util.Properties

import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
//import org.elasticsearch.search.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder


/**
 * @Project_name gmall0921realtime
 * @Package_name com.atguigu.gmall0921.realtime.utils
 * @author zhuguang
 * @date 2021-01-26-11:04
 */
object MyEsUtil {
  private var factory: JestClientFactory = null
  val DEFAULT_TYPE = "_doc"

  def saveBulk(indexName: String, docList: List[(String, Any)]) = {
    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder()
    for ((id, doc) <- docList) {
      val index: Index = new Index.Builder(doc).id(id).build()
      bulkBuilder.addAction(index)
    }
    bulkBuilder.defaultIndex(indexName).defaultType(DEFAULT_TYPE)
    val bulk = bulkBuilder.build()

    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
    println("已保存：" + items.size() + "条")
    jest.close()


  }


  def getClient = {
    if (factory == null) {
      build()
    }
    factory.getObject

  }

  def build() = {
    factory = new JestClientFactory
    val properties: Properties = PropertiesUtil.load("config.properties")
    val serverUrl: String = properties.getProperty("elasticsearch.server")
    factory.setHttpClientConfig(new HttpClientConfig.Builder(serverUrl)
      .multiThreaded(true).maxTotalConnection(10).connTimeout(1000).readTimeout(1000).build())

  }


  def save(): Unit = {
    val jest: JestClient = getClient
    val index = new Index.Builder(MovieTest("0104", "电影123")).index("movie_test0921_20210126")
      .`type`("_doc").build()
    jest.execute(index)


    jest.close()
  }


  def search() = {
    val jest: JestClient = getClient
    val query = "{\n  \"query\": {\n    \"match\": {\n      \"name\": \"red\"\n    }\n  }\n}"

    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.query(new MatchQueryBuilder("name", "red sea"))
    searchSourceBuilder.sort("doubanSore", SortOrder.DESC)
    searchSourceBuilder.size(2)
    searchSourceBuilder.from(0)
    searchSourceBuilder.fetchSource(Array("name", "doubanScore"), null)
    //    searchSourceBuilder.highlighter(new HighlightBuilder().
    //      field("name").preTags("<span").postTags("</span"))

    val searchRs: Search = new Search.Builder(searchSourceBuilder.toString).addIndex("movie_index").addType("_doc").build()
    val result: SearchResult = jest.execute(searchRs)
    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])

    import collection.JavaConverters._
    for (rs <- rsList.asScala) {
      println(rs.score)
    }
    jest.close()
  }

  def main(args: Array[String]): Unit = {
    search()
  }


  case class MovieTest(id: String, movie_name: String)

}
