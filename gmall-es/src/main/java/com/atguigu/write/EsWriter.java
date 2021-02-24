package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

//单条数据写入
public class EsWriter {
    public static void main(String[] args) throws IOException {
        //1.创建连接工厂
        JestClientFactory jestClientFactory = new JestClientFactory();

        //2.指定连接地址
        HttpClientConfig clientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();
        jestClientFactory.setHttpClientConfig(clientConfig);
        //3.获取连接
        JestClient jestClient = jestClientFactory.getObject();

        //数据准备
        Movie movie = new Movie("1003", "金瓶梅");
        Index index = new Index.Builder(movie)
                .id("1002")
                .index("movie_test1")
                .type("_doc")
                .build();
//        Index index = new Index.Builder("{\n" +
//                "  \"id\":1002,\n" +
//                "  \"name\":\"侍神令2\"\n" +
//                "}")
//                .id("1002")
//                .index("movie_test1")
//                .type("_doc")
//                .build();
        //4.执行写入操作
        jestClient.execute(index);

        //5.关闭连接
        jestClient.shutdownClient();
    }
}
