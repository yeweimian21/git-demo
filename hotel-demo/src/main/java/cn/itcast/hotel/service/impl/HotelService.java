package cn.itcast.hotel.service.impl;

import cn.itcast.hotel.mapper.HotelMapper;
import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.pojo.PageResult;
import cn.itcast.hotel.pojo.RequestParams;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.apache.lucene.queryparser.flexible.standard.QueryParserUtil;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private RestHighLevelClient client;

    @Override
    public PageResult search(RequestParams requestParams) {
        PageResult pageResult = new PageResult();

        SearchRequest searchRequest = new SearchRequest("hotel");

        // 查询条件
        bulidBasicQuery(requestParams, searchRequest);

        // 排序
        String location = requestParams.getLocation();
        if (location != null && !"".equals(location)) {
            searchRequest.source().sort(SortBuilders
                    .geoDistanceSort("location", new GeoPoint())
                    .order(SortOrder.ASC)
                    .unit(DistanceUnit.KILOMETERS)
            );
        }

        // 分页
        int page = requestParams.getPage();
        int size = requestParams.getSize();
        int from = (page - 1) * size;
        searchRequest.source().from(from).size(size);

        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            pageResult = handleResponse(response);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return pageResult;
    }

    @Override
    public Map<String, List<String>> filters(RequestParams requestParams) {
        Map<String, List<String>> resultMap = new HashMap<>();

        SearchRequest searchRequest = new SearchRequest("hotel");

        // 查询条件
        bulidBasicQuery(requestParams, searchRequest);

        searchRequest.source().size(0);
        buildAggregation(searchRequest);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

            Aggregations aggregations = response.getAggregations();
            List<String> brandList = getAggByName(aggregations, "brandAgg");
            resultMap.put("品牌", brandList);
            List<String> cityList= getAggByName(aggregations, "cityAgg");
            resultMap.put("城市", cityList);
            List<String> starList = getAggByName(aggregations, "starAgg");
            resultMap.put("星级", starList);

        } catch (IOException e) {
            e.printStackTrace();
        }


        return resultMap;
    }

    @Override
    public List<String> getSuggestions(String prefix) {
        List<String> suggestions = new ArrayList<>();

        SearchRequest request = new SearchRequest("hotel");

        request.source().suggest(new SuggestBuilder().addSuggestion(
                "hotelSuggestion",
                SuggestBuilders
                        .completionSuggestion("suggestion")
                        .prefix(prefix).skipDuplicates(true).size(10)
        ));

        try {
            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            Suggest suggest = response.getSuggest();
            CompletionSuggestion hotelSuggestion = suggest.getSuggestion("hotelSuggestion");
            List<CompletionSuggestion.Entry.Option> options = hotelSuggestion.getOptions();
            for (CompletionSuggestion.Entry.Option option : options) {
                String text = option.getText().toString();
                suggestions.add(text);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return suggestions;
    }

    @Override
    public void insertById(Long id) {
        Hotel hotel = getById(id);
        HotelDoc hotelDoc = new HotelDoc(hotel);

        IndexRequest request = new IndexRequest("hotel").id(id.toString());
        request.source(JSONObject.toJSONString(hotelDoc), XContentType.JSON);
        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteById(Long id) {
        DeleteRequest request = new DeleteRequest("hotel", id.toString());
        try {
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // INSERT INTO `elastic_search_demo`.`tb_hotel`(`id`, `name`, `address`, `price`, `score`, `brand`, `city`, `star_name`, `business`, `latitude`, `longitude`, `pic`) VALUES (60223, '上海希尔顿酒店', '静安华山路250号', 2688, 37, '希尔顿', '上海', '五星级', '静安寺地区', '31.219306', '121.445427', 'https://m.tuniucdn.com/filebroker/cdn/res/92/10/9210e74442aceceaf6e196d61fc3b6b1_w200_h200_c1_t0.jpg');

    }

    private void buildAggregation(SearchRequest searchRequest) {
        searchRequest.source().aggregation(AggregationBuilders
                .terms("brandAgg")
                .field("brand")
                .size(100)
        );
        searchRequest.source().aggregation(AggregationBuilders
                .terms("cityAgg")
                .field("city")
                .size(100)
        );
        searchRequest.source().aggregation(AggregationBuilders
                .terms("starAgg")
                .field("starName")
                .size(100)
        );
    }

    private List<String> getAggByName(Aggregations aggregations, String aggName) {
        List<String> termList = new ArrayList<>();

        Terms terms = aggregations.get(aggName);
        List<? extends Terms.Bucket> buckets = terms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            String key = bucket.getKeyAsString();
            termList.add(key);
        }

        return termList;
    }

    private void bulidBasicQuery(RequestParams requestParams, SearchRequest searchRequest) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        String key = requestParams.getKey();
        if (key == null || "".equals(key)) {
            boolQueryBuilder.must(QueryBuilders.matchAllQuery());
        } else {
            boolQueryBuilder.must(QueryBuilders.matchQuery("all", key));
        }

        String city = requestParams.getCity();
        String brand = requestParams.getBrand();
        String starName = requestParams.getStarName();
        Integer minPrice = requestParams.getMinPrice();
        Integer maxPrice = requestParams.getMaxPrice();
        if (city != null && !"".equals(city)) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("city", city));
        }
        if (brand != null && !"".equals(brand)) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("brand", brand));
        }
        if (starName != null && !"".equals(starName)) {
            boolQueryBuilder.filter(QueryBuilders.termQuery("starName", starName));
        }
        if (minPrice != null) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").gte(minPrice));
        }
        if (maxPrice != null) {
            boolQueryBuilder.filter(QueryBuilders.rangeQuery("price").lte(maxPrice));
        }

        // 算分控制
        FunctionScoreQueryBuilder functionScoreQueryBuilder =
                QueryBuilders.functionScoreQuery(
                        //
                        boolQueryBuilder,
                        new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                                        QueryBuilders.termQuery("isAD", true),
                                        ScoreFunctionBuilders.weightFactorFunction(10)
                                )
                        });

//        searchRequest.source().query(boolQueryBuilder);
        searchRequest.source().query(functionScoreQueryBuilder);
    }

//    @Override
//    public PageResult search(RequestParams requestParams) {
//        PageResult pageResult = new PageResult();
//
//        SearchRequest searchRequest = new SearchRequest("hotel");
//        String key = requestParams.getKey();
//        if (key == null || "".equals(key)) {
//            searchRequest.source().query(QueryBuilders.matchAllQuery());
//        } else {
//            searchRequest.source().query(QueryBuilders.matchQuery("all", key));
//        }
//        int page = requestParams.getPage();
//        int size = requestParams.getSize();
//        int from = (page - 1) * size;
//        searchRequest.source().from(from).size(size);
//
//        try {
//            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
//            pageResult = handleResponse(response);
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        return pageResult;
//    }

    private PageResult handleResponse(SearchResponse searchResponse) {
        SearchHits searchHits = searchResponse.getHits();
        TotalHits totalHits = searchHits.getTotalHits();
        long total = totalHits.value;
        SearchHit[] hits = searchHits.getHits();
        List<HotelDoc> hotelDocList = new ArrayList<>();
        for (SearchHit hit : hits) {
            String json = hit.getSourceAsString();
            HotelDoc hotelDoc = JSONObject.parseObject(json, HotelDoc.class);

            Object[] sortValues = hit.getSortValues();
            if (sortValues != null && sortValues.length > 0) {
                hotelDoc.setDistance(sortValues);
            }

            hotelDocList.add(hotelDoc);
        }
        return new PageResult(total, hotelDocList);
    }

}
