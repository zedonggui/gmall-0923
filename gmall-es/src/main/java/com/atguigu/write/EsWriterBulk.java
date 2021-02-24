package com.atguigu.write;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;
//批量写入
public class EsWriterBulk {
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
        Movie movie2 = new Movie("1004", "西厢记");
        Movie movie3 = new Movie("1005", "西游记");
        Index index = new Index.Builder(movie).build();
        Index index2 = new Index.Builder(movie2).build();
        Index index3 = new Index.Builder(movie3).build();

        Bulk bulk = new Bulk.Builder()
                .defaultIndex("movie_test1")
                .defaultType("_doc")
                .addAction(index)
                .addAction(index2)
                .addAction(index3)
                .build();

        //4.执行写入操作
        jestClient.execute(bulk);

        //5.关闭连接
        jestClient.shutdownClient();
    }
}
