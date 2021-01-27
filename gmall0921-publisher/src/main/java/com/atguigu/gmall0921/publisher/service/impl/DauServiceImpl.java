package com.atguigu.gmall0921.publisher.service.impl;

import com.atguigu.gmall0921.publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class DauServiceImpl implements DauService {

    @Autowired
    JestClient jestClient;

    public static final String DAU_INDEX_PREFIX="gmall0921_dau_info_";
    public static final String DAU_INDEX_SUFFIX="-query";
    public static final String DEFAULT_TYPE="_doc";



    @Override
    public String getDate(String name) {
        //查询数据
        return "30岁,男,程序员";
    }

    @Override
    public Long getTotal(String date) {
        //查询es
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        String indexName=DAU_INDEX_PREFIX+date+DAU_INDEX_SUFFIX;

        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType(DEFAULT_TYPE).build();
        try {
            SearchResult result = jestClient.execute(search);
            return result.getTotal();

        } catch (IOException e) {
            e.printStackTrace();
            throw  new RuntimeException("es 查询异常");
        }

    }

    @Override
    public Map getHourCount(String date) {
        date = date.replace("-", "");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);

        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("groupby_hr").field("hr").size(24);
        searchSourceBuilder.aggregation(aggregationBuilder);
        String indexName=DAU_INDEX_PREFIX+date+DAU_INDEX_SUFFIX;
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(indexName).addType(DEFAULT_TYPE).build();
        try {
            SearchResult result = jestClient.execute(search);
            Map hourMap=new HashMap();
            TermsAggregation termsAggregation = result.getAggregations().getTermsAggregation("groupby_hr");
            //避免空指针
            if(termsAggregation!=null){
                List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hourMap.put( bucket.getKey(),bucket.getCount());
                }
            }
            return  hourMap;

        } catch (IOException e) {
            e.printStackTrace();
            throw  new RuntimeException("es 查询异常");
        }
    }
}
