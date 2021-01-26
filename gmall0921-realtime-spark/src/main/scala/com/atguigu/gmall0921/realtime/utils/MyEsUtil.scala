package com.atguigu.gmall0921.realtime.utils

import java.util
import java.util.Properties

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index, Search, SearchResult}
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

object MyEsUtil {
  private    var  factory:  JestClientFactory=null;


  def getClient:JestClient={
    if(factory==null) build()
    factory.getObject

  }

  def build(): Unit ={
    factory=new JestClientFactory
    val properties: Properties = PropertiesUtil.load("config.properties")
    val serverUri: String = properties.getProperty("elasticsearch.server")
    factory.setHttpClientConfig(new HttpClientConfig.Builder(serverUri)
      .multiThreaded(true).maxTotalConnection(10)
      .connTimeout(10000).readTimeout(10000).build())
  }


  def main(args: Array[String]): Unit = {
    search()
  }

  def search(): Unit ={
    val jest: JestClient = getClient
   //组织参数
    val query="{\n  \"query\": {\n    \"match\": {\n      \"name\": \"operation red sea\"\n    }\n  },\n  \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"asc\"\n      }\n    }\n  ],\n  \"size\": 2\n  , \"from\": 0\n  ,\"_source\": [\"name\",\"doubanScore\"]\n  ,\"highlight\": { \"fields\": {\"name\":{ \"pre_tags\": \"<span color='red'>\", \"post_tags\": \"</span>\" }}}\n \n}"

    val searchSourceBuilder = new SearchSourceBuilder()
    searchSourceBuilder.query(new MatchQueryBuilder("name","operation red sea"))
    searchSourceBuilder.sort("doubanScore",SortOrder.DESC)
    searchSourceBuilder.size(2)
    searchSourceBuilder.from(0)
    searchSourceBuilder.fetchSource(Array("name","doubanScore"),null)
    searchSourceBuilder.highlighter(new HighlightBuilder().
      field("name").preTags("<span>").postTags("</span>"))

    val search: Search = new Search.Builder(searchSourceBuilder.toString).addIndex("movie_index0921").addType("movie").build()
    val result: SearchResult = jest.execute(search)
    val rsList: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
       //1 专用容器 case class  2 通用容器  map
    import  collection.JavaConverters._
    for (rs <- rsList.asScala ) {
      println(rs.source)
    }

    //接收结果

    jest.close()
  }

  val DEFAULT_TYPE="_doc"
  //batch  bulk   构建批量保存
  def saveBulk(indexName:String, docList:List[(String,Any)]): Unit ={

    val jest: JestClient = getClient
    val bulkBuilder = new Bulk.Builder()
    //加入很多个单行操作
    for ((id,doc) <- docList ) {
      val index = new Index.Builder(doc ).id(id).build()
      bulkBuilder.addAction(index)
    }
    //加入统一保存的索引
    bulkBuilder.defaultIndex(indexName).defaultType(DEFAULT_TYPE)
    val bulk = bulkBuilder.build()
    val items: util.List[BulkResult#BulkResultItem] = jest.execute(bulk).getItems
    println("已保存："+items.size()+"条")
    jest.close()
  }

  def save(): Unit ={
    val jest: JestClient = getClient
    // source 可以放2种 1 class case  2 通用可转json的对象  比如map
    val index = new Index.Builder(MovieTest("0104","电影123")).index("movie_test0921_20210126").`type`("_doc")  .build()
    jest.execute(index)
    jest.close()
  }


  case class MovieTest(id:String,movie_name:String)


}
