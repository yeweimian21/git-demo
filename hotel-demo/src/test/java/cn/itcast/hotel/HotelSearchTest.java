package cn.itcast.hotel;

import cn.itcast.hotel.pojo.HotelDoc;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class HotelSearchTest {

    private RestHighLevelClient client;

    @BeforeEach
    void setUp() {
        this.client = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.116.128:9200")
        ));
    }

    @AfterEach
    void tearDown() {
        try {
            this.client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleResponse(SearchResponse searchResponse) {
        SearchHits searchHits = searchResponse.getHits();
        TotalHits totalHits = searchHits.getTotalHits();
        long total = totalHits.value;
        System.out.println("共查询到" + total + "条数据");
        SearchHit[] hits = searchHits.getHits();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSONObject.parseObject(json, HotelDoc.class);
            System.out.println(hotelDoc);
        }
    }

    @Test
    void testMatchAll() {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.matchAllQuery());
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println(searchResponse);

            handleResponse(searchResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testMatch() {
        SearchRequest searchRequest = new SearchRequest("hotel");
        searchRequest.source().query(QueryBuilders.matchQuery("all", "如家"));
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            System.out.println(searchResponse);

            handleResponse(searchResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testBoolQuery() {
        SearchRequest searchRequest = new SearchRequest("hotel");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("city", "上海"));
//        boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lt(300));

        searchRequest.source().query(boolQueryBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            handleResponse(searchResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testPageAndSort() {
        SearchRequest searchRequest = new SearchRequest("hotel");

        searchRequest.source().query(QueryBuilders.matchAllQuery());
        searchRequest.source().sort("price", SortOrder.ASC);
        searchRequest.source().from(5).size(5);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            handleResponse(searchResponse);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testHighlight() {
        SearchRequest searchRequest = new SearchRequest("hotel");

        searchRequest.source().query(QueryBuilders.matchQuery("all", "如家"));

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("name").requireFieldMatch(false);
        searchRequest.source().highlighter(highlightBuilder);

        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
//            handleResponse(searchResponse);

            SearchHits searchHits = searchResponse.getHits();
            TotalHits totalHits = searchHits.getTotalHits();
            long total = totalHits.value;
            System.out.println("共查询到" + total + "条数据");
            SearchHit[] hits = searchHits.getHits();
            for (SearchHit hit : hits) {
                String json = hit.getSourceAsString();
                HotelDoc hotelDoc = JSONObject.parseObject(json, HotelDoc.class);

                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                if (!CollectionUtils.isEmpty(highlightFields)) {
                    HighlightField highlightField = highlightFields.get("name");
                    if (highlightField != null) {
                        String name = highlightField.getFragments()[0].toString();
                        System.out.println(name);
                    }
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Test
    void testAggregation() {
        SearchRequest searchRequest = new SearchRequest("hotel");

        try {
            searchRequest.source().size(0);
            searchRequest.source().aggregation(AggregationBuilders
                    .terms("brandAgg")
                    .field("brand")
                    .size(10)
            );

            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            Aggregations aggregations = response.getAggregations();
            Terms brandTerms = aggregations.get("brandAgg");
            List<? extends Terms.Bucket> buckets = brandTerms.getBuckets();
            for (Terms.Bucket bucket : buckets) {
                System.out.println(bucket.getKey() + " : " + bucket.getDocCount());
            }


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    @Test
    void testSuggestion() {
        SearchRequest request = new SearchRequest("hotel");

        request.source().suggest(new SuggestBuilder().addSuggestion(
                "hotelSuggestion",
                SuggestBuilders
                        .completionSuggestion("suggestion")
                        .prefix("sh").skipDuplicates(true).size(10)
        ));

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            Suggest suggest = response.getSuggest();
            CompletionSuggestion hotelSuggestion = suggest.getSuggestion("hotelSuggestion");
            List<CompletionSuggestion.Entry.Option> options = hotelSuggestion.getOptions();
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                System.out.println(text);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
