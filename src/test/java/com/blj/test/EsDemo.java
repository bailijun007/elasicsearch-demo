package com.blj.test;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.RangeFieldMapper;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filters;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.search.aggregations.metrics.cardinality.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import javax.management.Query;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;

/**
 * es Demo
 *
 * @author BaiLiJun on 2019/11/3 0003
 */
public class EsDemo {

    /**
     * Get 查询
     */
    @Test
    public void testGetQuery() throws UnknownHostException {
        TransportClient client = getClient();
        //数据查询
        GetResponse response = client.prepareGet("test_index", "test_type", "25")
                .execute().actionGet();
        //得到查询的数据
        System.out.println("response.getSourceAsString() = " + response.getSourceAsString());

        client.close();
    }

    private TransportClient getClient() throws UnknownHostException {
        //指定es集群
        Settings settings = Settings.builder().put("cluster.name", "my-application").build();
        //创建es客户端
        return new PreBuiltTransportClient(settings).addTransportAddress(new TransportAddress(InetAddress.getByName("127.0.0.1"), 9300));
    }


    /*
    *添加文档
    *"{"+
    "\"id\":\"1\"," +
    "\ "title\":\ "Java设计模式之装饰模式\"," +
    "\ "content\":\ "在不必改变原类文件和使用继承的情况下，动态地扩展-一个对象的功能。\"," +
    "\ "postdate\":\"2019-11-03\","+
    "\"ur1\":\"csdn.net/ 79239072\"" +
    "}"
    */
    @Test
    public void testAddDoc() throws Exception {
        TransportClient client = getClient();
        //文档内容
        XContentBuilder doc = XContentFactory.jsonBuilder().startObject()
                .field("id", "1")
                .field("title", "Java设计模式之装饰模式")
                .field("content", "在不必改变原类文件和使用继承的情况下，动态地扩展-一个对象的功能")
                .field("postdate", "2019-11-03")
                .field("url", "csdn.net/79239072")
                .endObject();
        //添加文档
        IndexResponse response = client.prepareIndex("index1", "blog", "10")
                .setSource(doc).get();
        System.out.println("response.status() = " + response.status());
    }


    /**
     * 删除文档
     */
    @Test
    public void testDelete() throws Exception {
        TransportClient client = getClient();
        DeleteResponse response = client.prepareDelete("index1", "blog", "10").get();
        System.out.println("response.status() = " + response.status());

    }


    /**
     * 更新文档(局部更新)
     */
    @Test
    public void testUpdate() throws Exception {
        TransportClient client = getClient();
        UpdateRequest request = new UpdateRequest();
        request.index("index1").type("blog").id("10").doc(
                XContentFactory.jsonBuilder().startObject()
                        .field("title", "单例模式")
                        .field("postdate", "2019-11-05")
                        .endObject()
        );
        UpdateResponse response = client.update(request).get();
        System.out.println("response.status() = " + response.status());
    }

    /**
     * upset 更新操作:不存在就创建，存在就更新
     */
    @Test
    public void testUpset() throws Exception {
        TransportClient client = getClient();
        IndexRequest request1 = new IndexRequest("index1", "blog", "8")
                .source(
                        XContentFactory.jsonBuilder().startObject()
                                .field("id", "2")
                                .field("title", "工厂模式")
                                .field("content", "静态工厂，实例工厂")
                                .field("postdate", "2019-11-05")
                                .field("url", "csdn.net/79239072")
                                .endObject()
                );

        UpdateRequest request2 = new UpdateRequest("index1", "blog", "8").doc(
                XContentFactory.jsonBuilder().startObject()
                        .field("title", "设计模式")
                        .endObject()
        ).upsert(request1);

        UpdateResponse response = client.update(request2).get();
        System.out.println("response.status() = " + response.status());
    }

    /**
     * mget用法:批量查询
     */
    @Test
    public void testMultiGet() throws Exception {
        TransportClient client = getClient();
        MultiGetResponse response = client.prepareMultiGet()
                .add("index1", "blog", "8", "10")
                .add("test_index", "test_type", "22", "24")
                .get();
        for (MultiGetItemResponse itemResponse : response) {
            GetResponse response1 = itemResponse.getResponse();
            if (response1 != null && response1.isExists()) {
                System.out.println("response1.getSourceAsString() = " + response1.getSourceAsString());
            }
        }
    }

    /**
     * bulk 批量增删改操作
     */
    @Test
    public void testBulk() throws Exception {
        TransportClient client = getClient();
        BulkRequestBuilder requestBuilder = client.prepareBulk();
        requestBuilder.add(client.prepareIndex("index1", "blog", "9").setSource(
                XContentFactory.jsonBuilder().startObject()
                        .field("id", "3")
                        .field("title", "bulk 批量增demo1")
                        .field("content", "bulk 批量增demo1")
                        .field("postdate", "2019-11-05")
                        .field("url", "csdn.net/79239072")
                        .endObject()
        ));

        requestBuilder.add(client.prepareIndex("index1", "blog", "7").setSource(
                XContentFactory.jsonBuilder().startObject()
                        .field("id", "4")
                        .field("title", "bulk 批量增demo2")
                        .field("content", "bulk 批量增demo2")
                        .field("postdate", "2019-11-05")
                        .field("url", "csdn.net/79239072")
                        .endObject()
        ));

        BulkResponse response = requestBuilder.get();
        if (response.hasFailures()) {
            System.out.println("失败");
        } else {
            System.out.println(" response.status() = " + response.status());

        }


    }


    /**
     * 查询删除
     */
    @Test
    public void testDeleteByQuery() throws Exception {
        TransportClient client = this.getClient();
        BulkByScrollResponse response = DeleteByQueryAction.INSTANCE
                .newRequestBuilder(client)
                .filter(QueryBuilders.matchQuery("title", "模式"))
                .source("index1").get();
        long counts = response.getDeleted();
        System.out.println("counts = " + counts);
    }

    /**
     * 查询所有
     */
    @Test
    public void testMatchAllQuery() throws Exception {
        TransportClient client = getClient();
        MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();
        SearchResponse response = client.prepareSearch("index1", "test_index").setQuery(matchAllQuery)
                .setSize(3).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + " = " + map.get(key));
            }
        }
    }

    /**
     * match query
     */
    @Test
    public void testMatchQuery() throws Exception {
        TransportClient client = getClient();
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("test_field", "hello");
        SearchResponse response = client.prepareSearch("test_index", "index1").setQuery(matchQuery)
                .setSize(3).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "  = " + map.get(key));
            }
        }
    }

    /**
     * multiMatchQuery 一个查询文本在多个字段中匹配，其中一个字段中有则返回
     */
    @Test
    public void testMultiMatchQuery() throws Exception {
        TransportClient client = getClient();
        MultiMatchQueryBuilder multiMatchQuery = QueryBuilders.multiMatchQuery("批量", "title", "content");
        SearchResponse response = client.prepareSearch("index1").setQuery(multiMatchQuery).setSize(3).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "    =" + map.get(key));
            }
        }
    }


    /**
     * term query
     * term是代表完全匹配，即不进行分词器分析，文档中必须包含整个搜索的词汇
     */
    @Test
    public void testTermQuery() throws Exception {
        TransportClient client = getClient();
        TermQueryBuilder termQuery = QueryBuilders.termQuery("name", "jack");
        SearchResponse response = client.prepareSearch("test_index", "company")
                .setQuery(termQuery).setSize(3).get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "    =" + map.get(key));
            }
        }
    }

    /**
     * terms query 多个词查询
     */
    @Test
    public void testTermsQuery() throws Exception {
        TransportClient client = getClient();
        TermsQueryBuilder termsQuery = QueryBuilders.termsQuery("name", "jack", "tom");
        SearchResponse response = client.prepareSearch("test_index", "company")
                .setQuery(termsQuery).setSize(3).get();

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + "= " + map.get(key));
            }
        }
    }

    /**
     * 各种查询
     */
    @Test
    public void testQuerys() throws Exception {
        TransportClient client = getClient();
        //range查询
        // RangeQueryBuilder builder = QueryBuilders.rangeQuery("join_work").from("2019-09-01").to("2019-11-7").format("yyyy-MM-dd");

        //prefix 查询
        //QueryBuilder builder = QueryBuilders.prefixQuery("name","zhao");

        //ids 查询
        // QueryBuilder builder = QueryBuilders.idsQuery().addIds("1","2","3");

        //wildcard 查询:通配符查询
        //QueryBuilder builder = QueryBuilders.wildcardQuery("name","zhao*");

        //type 查询
        //QueryBuilder builder = QueryBuilders.typeQuery("blog");

        //fuzzy 查询: 模糊查询
        QueryBuilder builder = QueryBuilders.fuzzyQuery("name", "jaek");

        SearchResponse response = client.prepareSearch("test_index", "company", "index1")
                .setQuery(builder).setSize(3)
                .get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + " = " + map.get(key));
            }
        }

    }


    /**
     * 聚合查询
     */
    @Test
    public void testAgg() throws Exception {
        TransportClient client = getClient();

//       AggregationBuilder agg = AggregationBuilders.max("aggMax").field("age");
//        SearchResponse response = client.prepareSearch("test_index", "company", "index1")
//                .addAggregation(agg).get();
//        Max max = response.getAggregations().get("aggMax");
//        System.out.println("max = " + max.getValue());

//        AggregationBuilder agg = AggregationBuilders.min("aggMin").field("age");
//        SearchResponse response = client.prepareSearch("test_index", "company", "index1").addAggregation(agg).get();
//        Min min = response.getAggregations().get("aggMin");
//        System.out.println("min.getValue() = " + min.getValue());

//        AggregationBuilder agg = AggregationBuilders.avg("aggAvg").field("age");
//        SearchResponse response = client.prepareSearch("test_index", "company", "index1").addAggregation(agg).get();
//        Avg avg = response.getAggregations().get("aggAvg");
//        System.out.println("avg = " + avg.getValue());

//        AggregationBuilder agg = AggregationBuilders.sum("aggSum").field("age");
//        SearchResponse response = client.prepareSearch("test_index", "company", "index1").addAggregation(agg).get();
//        Sum sum = response.getAggregations().get("aggSum");
//        System.out.println("sum = " + sum.getValue());

        //基数：即，如果有4个值，3个值不重复，则返回3
        AggregationBuilder agg = AggregationBuilders.cardinality("aggCardinality").field("age");
        SearchResponse response = client.prepareSearch("index1", "company").addAggregation(agg).get();
        Cardinality cardinality = response.getAggregations().get("aggCardinality");
        System.out.println("cardinality = " + cardinality.getValueAsString());
    }


    /**
     * queryString
     */
    @Test
    public void testQueryString() throws Exception {
        TransportClient client = getClient();
        //CommonTermsQueryBuilder builder = QueryBuilders.commonTermsQuery("name", "jack");

        //  +:必须包含  -：必须不能包含 queryStringQuery查询必须完全满足才返回结果
        //  QueryBuilder builder =QueryBuilders.queryStringQuery("+jiangsu -wuxi");

        //simpleQueryStringQuery 只需要满足其中一个条件就可以返回
        QueryBuilder builder = QueryBuilders.simpleQueryStringQuery("+jiangsu -wuxi");
        SearchResponse response = client.prepareSearch("index1", "company").setQuery(builder).setSize(3).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
            Map<String, Object> map = hit.getSourceAsMap();
            for (String key : map.keySet()) {
                System.out.println(key + " = " + map.get(key));
            }
        }
    }


    /**
     * 组合查询
     */
    @Test
    public void testCombinationQuery() throws Exception {
        TransportClient client = getClient();
//        QueryBuilder builder = QueryBuilders.boolQuery()
//                .must(QueryBuilders.prefixQuery("name", "zhao"))
//                .mustNot(QueryBuilders.matchQuery("adress", "shanghai"))
//                .should(QueryBuilders.matchQuery("adress", "China"))
//                .filter(QueryBuilders.rangeQuery("join_work").gte("2019-09-01").format("yyyy-MM-dd"));


        //constantScoreQuery 不计算相关度分数
        QueryBuilder builder =QueryBuilders.constantScoreQuery(QueryBuilders.wildcardQuery("name","zhao*"));
        SearchResponse response = client.prepareSearch("index1", "company").setQuery(builder)
                .setSize(3).get();
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            System.out.println("hit.getSourceAsString() = " + hit.getSourceAsString());
        }
    }

    /**
     * 分组聚合
     */
    @Test
    public void testTermsAgg() throws UnknownHostException {
        TransportClient client = getClient();
       AggregationBuilder builder = AggregationBuilders.terms("terms").field("age");
        SearchResponse response = client.prepareSearch("company")
                .addAggregation(builder).execute().actionGet();
        Terms terms = response.getAggregations().get("terms");
        for (Terms.Bucket entry : terms.getBuckets()) {
            System.out.println(entry.getKey()+":"+entry.getDocCount());
        }
                
    }

    /**
     * Filter 聚合
     */
    @Test
    public void testFilterAgg() throws UnknownHostException {
        TransportClient client = getClient();
      QueryBuilder query = QueryBuilders.termQuery("age", 26);
       AggregationBuilder agg = AggregationBuilders.filter("filter", query);
        SearchResponse response = client.prepareSearch("company").addAggregation(agg).execute().actionGet();
        Filter filter = response.getAggregations().get("filter");
        System.out.println("filter.getDocCount() = "+filter.getDocCount());
    }

    /**
     * Filters 聚合
     */
    @Test
    public void testFiltersAgg() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.filters("filters",
                new FiltersAggregator.KeyedFilter("26",QueryBuilders.termsQuery("age","26")),
                new FiltersAggregator.KeyedFilter("27",QueryBuilders.termsQuery("age","27"))
                );
        SearchResponse response = client.prepareSearch("company").addAggregation(agg).execute().actionGet();
        Filters filters = response.getAggregations().get("filters");
        for (Filters.Bucket entry : filters.getBuckets()) {
            System.out.println(entry.getKey()+":"+entry.getDocCount());
        }
    }

    /**
     * Range 聚合
     */
    @Test
    public void testRangeAgg() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.range("range")
                .field("age")
                .addUnboundedTo(30)
                .addRange(25,30)
                .addUnboundedFrom(25);
        SearchResponse response = client.prepareSearch("company").addAggregation(agg).execute().actionGet();
        Range range = response.getAggregations().get("range");
        for (Range.Bucket entry : range.getBuckets()) {
            System.out.println(entry.getKey()+":"+entry.getDocCount());
        }
    }

    /**
     * missing 聚合:统计null值的字段有多少个
     */
    @Test
    public void testMissingAgg() throws UnknownHostException {
        TransportClient client = getClient();
        AggregationBuilder agg = AggregationBuilders.missing("missing").field("join_work");
        SearchResponse response = client.prepareSearch("company").addAggregation(agg).execute().actionGet();
        Aggregation missing = response.getAggregations().get("missing");
        System.out.println("missing.toString() = " + missing.toString());

    }


}

