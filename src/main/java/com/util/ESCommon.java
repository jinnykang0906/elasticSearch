package com.util;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import com.entity.Message;
import com.fasterxml.jackson.core.JsonProcessingException;

public class ESCommon {
	private static final String INDEX = "twitter";
	Client  client = null;
	{
	/*	Node node = NodeBuilder.nodeBuilder().clusterName("es19").node();
		client = node.client();*/
	/*	Node node = NodeBuilder.nodeBuilder().node();	
		client = node.client();*/
		
		Settings settings = Settings.settingsBuilder().put("cluster.name", "eslocal105").build();
		try {
			 client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("127.0.0.1"),9300));
		} catch (UnknownHostException e) {
			client = null;
		}
		/*client =  new TransportClient(ImmutableSettings.builder().put("cluster.name", "es19").build())
		 .addTransportAddress(new InetSocketTransportAddress("192.168.1.19", 9300));*/

	}
	
	public static void main(String[] args) throws ElasticsearchException, JsonProcessingException{
		ESCommon esCommon = new ESCommon();
		//esCommon.createIndex();
		//esCommon.getIndexData();
		//esCommon.updateIndex();
		//esCommon.deleteIndex();
		//esCommon.multiGetIndex();
		//esCommon.bulkIndex();
		//esCommon.searchIndex();
		esCommon.queryDSL();
	}

	
	public void createIndex() throws ElasticsearchException, JsonProcessingException{
		
		for(int i = 1011; i < 1012; i++){
			Message message = new Message();
			message.setId(Long.valueOf(i));
			message.setContent("pppPPPPuuUUuuuUUUU");
			message.setName("消息"+i);
			//message.setCreateDate(new Date(System.currentTimeMillis()));
			IndexResponse response = client.prepareIndex(INDEX, Message.class.getSimpleName(), message.getId().toString()).setSource(JsonUtil.getJson(message)).get();
			
			System.out.println(response.getIndex());
			System.out.println(response.getType());
			System.out.println(response.getId());
		}
		

		
		
	}
	
	public void  getIndexData(){
		GetResponse response = client.prepareGet(INDEX, Message.class.getSimpleName(), "2").get();
		System.out.println(response.getSource());
	}
	
	public void updateIndex() throws ElasticsearchException, JsonProcessingException{
	/*	Message message = new Message();
		message.setId(3l);
		message.setContent("你好吗！我是开发专员！");
		message.setName("消息三");
		message.setCreateDate(new Date(System.currentTimeMillis()));*/
		
		client.prepareUpdate(INDEX, Message.class.getSimpleName(), "2").setScript(new Script("ctx._source.name = \"消息二\"" , ScriptService.ScriptType.INLINE, null, null)).get();
		System.out.println("更新成功");
	}
	
	public void deleteIndex(){
		client.prepareDelete(INDEX, Message.class.getSimpleName(), "3").get();
		System.out.print("删除成功");
	}
	
	public void multiGetIndex(){
		String ids [] = {"1","2"};
		MultiGetResponse multiGetItemResponses = client.prepareMultiGet().add(INDEX, Message.class.getSimpleName(), ids)
				.add("teamwork","User","477").get();
		for (MultiGetItemResponse itemResponse : multiGetItemResponses) { 
		    GetResponse response = itemResponse.getResponse();
		    if (response.isExists()) {                      
		        String json = response.getSourceAsString(); 
		        System.out.println(json);
		    }
		}
	}
	
	public void bulkIndex() throws JsonProcessingException{
		BulkRequestBuilder bulkRequest =  client.prepareBulk();
		
		Message message = new Message();
		message.setId(5l);
		message.setContent("你好吗！我是酱油专员！");
		message.setName("消息五");
		message.setCreateDate(new Date(System.currentTimeMillis()));
		
		IndexRequestBuilder indexRequest = client.prepareIndex(INDEX, Message.class.getSimpleName(), message.getId().toString()).setSource(JsonUtil.getJson(message));
		bulkRequest.add(indexRequest);
		UpdateRequestBuilder updateRequest = client.prepareUpdate(INDEX, Message.class.getSimpleName(), "4").setScript(new Script("ctx._source.name = \"消息4\"" , ScriptService.ScriptType.INLINE, null, null));	
		bulkRequest.add(updateRequest);
		BulkResponse  response = bulkRequest.get();
		if(response.hasFailures()){
			System.out.println("+++++失败+++++");
		}
	}
	
	public void searchIndex(){
	
		SearchRequestBuilder searchRequest = client.prepareSearch(INDEX).setTypes(Message.class.getSimpleName()).setSearchType(SearchType.DEFAULT);
		BoolQueryBuilder  boolBuilder = QueryBuilders.boolQuery();
		boolBuilder.mustNot(QueryBuilders.termQuery("id", "3"));
		//searchRequest.setScroll(new TimeValue(60000));
		//searchRequest.setTerminateAfter(2);
		//searchRequest.setPostFilter(FilterBuilders.rangeFilter("id").from("1").to("3"));
		//searchRequest.addAggregation(AggregationBuilders.terms("_id").field("createDate"));
		SearchResponse searchResponse = searchRequest.setQuery(boolBuilder).execute().actionGet();
		/*Terms  terms = searchResponse.getAggregations().get("_id");
		if(terms != null){
			for(Terms.Bucket entry : terms.getBuckets()){
				System.out.println(entry.getKey()+"+++++++++++++"+entry.getDocCount());
			}
		}
		if(searchResponse.isTimedOut()){
			for(SearchHit searchHit :searchResponse.getHits()){
				System.out.println(searchHit.getSourceAsString());
				
			}
		}*/
		
		for(SearchHit searchHit :searchResponse.getHits()){
			System.out.println(searchHit.getSourceAsString());
			
		}
		
		/*if(searchResponse.isTerminatedEarly()){
			
		}else{
			System.out.println("成功");
		}*/
		
		
	}
	
	public void queryDSL(){
		SearchRequestBuilder searchRequest = client.prepareSearch(INDEX).setTypes(Message.class.getSimpleName()).setSearchType(SearchType.DEFAULT);
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();//match ALL query
		/**
		 * full text query  全文查询
		 */
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.matchQuery("content","22专")).execute().actionGet();//match query  会自动分割查询字段   满足关键字一种即可
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.multiMatchQuery("22专","content","name")).execute().actionGet();//多字段全文匹配  执行全文查询，包括模糊匹配和短语或近似查询标准查询。
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.commonTermsQuery("content","22专")).execute().actionGet();//通用字段查询  满足所有关键字的才可以
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.queryStringQuery("22专").field("content")).execute().actionGet();//通用字段查询
		
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.queryStringQuery("22专").field("content")).execute().actionGet();//字段查询   
		
		/**
		 * 基础查询
		 */
	//	SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.termsQuery("content","专员","员")).execute().actionGet();//满足条件值中的一个即可      字段不会被切割成词
		
	//	SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.rangeQuery("id").from(5).to(10)).execute().actionGet();//范围查询
	//	SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.existsQuery("id1")).execute().actionGet();//存在查找 存在就继续 不存在就返回结果
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.missingQuery("createDate")).execute().actionGet();//其中指定的字段是否缺失或只包含空值。 缺失显示缺失的doc 
	//	SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.prefixQuery("content","10")).execute().actionGet();//前缀查询  作为一个词的前缀进行查询
	//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.wildcardQuery("content","*u*")).execute().actionGet();//通配符查询 针对切割后的字段  英文全部转小写处理   查询字段如果为字母 也应该小写
	//	SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.fuzzyQuery("content","15")).execute().actionGet();//模糊查询
		
		/**
		 * 复合查询   包装基础查询
		 */
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.constantScoreQuery(QueryBuilders.termQuery("content", "专员")).boost(2.0f)).execute().actionGet();//常量得分查询 
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("content", "员")).must(QueryBuilders.termQuery("name", "息"))).execute().actionGet();//bool查询    条件关系为and 
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.disMaxQuery().add(QueryBuilders.termQuery("content", "专员")).add(QueryBuilders.termQuery("name", "一"))).execute().actionGet();//组合查询  可以接受多个查询  or分开每个条件
		//SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.functionScoreQuery().add(QueryBuilders.termQuery("content", "我是专员"), ScoreFunctionBuilders.randomFunction("122"))).execute().actionGet();//函数降低分数查询
		
	/**
	 * 连接查询	嵌套查询   has_child and has_parent
	 */
		SearchResponse searchResponse = searchRequest.setQuery(QueryBuilders.hasChildQuery(Message.class.getSimpleName(),QueryBuilders.termQuery("content", "专"))).execute().actionGet();//加入查询  查询字表匹配
		for(SearchHit searchHit :searchResponse.getHits()){
			System.out.println(searchHit.getSourceAsString());
			
		}
	}
	
	
	public void adminIndeice(){
		client.admin().indices();
	}
}
