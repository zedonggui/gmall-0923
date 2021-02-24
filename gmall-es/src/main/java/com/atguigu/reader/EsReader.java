package com.atguigu.reader;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

//查数据
public class EsReader {
    public static void main(String[] args) throws IOException {
        //1.创建工厂
        JestClientFactory clientFactory = new JestClientFactory();

        //2.设置连接地址
        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        clientFactory.setHttpClientConfig(clientConfig);

        //3.获取连接
        JestClient jestClient = clientFactory.getObject();

        //4.执行查询
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"name\": \"小\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        { \"match\": {\n" +
                "            \"favo\": \"球\"\n" +
                "           }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"groupByClass\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"class_id\",\n" +
                "        \"size\": 10\n" +
                "      },\n" +
                "     \"aggs\": {\n" +
                "        \"maxAge\": {\n" +
                "           \"max\": {\n" +
                "              \"field\": \"age\"\n" +
                "            }\n" +
                "         }\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "    \"from\": 0,\n" +
                "    \"size\": 2\n" +
                "  \n" +
                "}")
                .addIndex("student3")
                .addType("_doc")
                .build();

        SearchResult result = jestClient.execute(search);
        //获取命中条数
        System.out.println("命中条数："+result.getTotal());

        //获取数据详情
        List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            //遍历map集合
            for (Object o : source.keySet()) {
                System.out.println(o+":"+source.get(o));
            }
        }
        //获取聚合组数据
        MetricAggregation aggregations = result.getAggregations();
        TermsAggregation groupByClass = aggregations.getTermsAggregation("groupByClass");
        List<TermsAggregation.Entry> buckets = groupByClass.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key:"+bucket.getKey());
            System.out.println("doc_count:"+bucket.getCount());
            MaxAggregation maxAge = bucket.getMaxAggregation("maxAge");
            System.out.println("年龄最大值:"+maxAge.getMax());
        }

        //5.关闭连接
        jestClient.shutdownClient();
    }
}
