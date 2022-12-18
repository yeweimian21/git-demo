package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HotelDocumentTest {

    @Autowired
    private IHotelService hotelService;

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

    @Test
    void testAddDocument() {
        Hotel hotel = hotelService.getById(36934);
        HotelDoc hotelDoc = new HotelDoc(hotel);

        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        request.source(JSONObject.toJSONString(hotelDoc), XContentType.JSON);
        try {
            client.index(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void getDocumentById() {
        GetRequest request = new GetRequest("hotel", "36934");
        try {
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            String json = response.getSourceAsString();
            HotelDoc hotelDoc = JSONObject.parseObject(json, HotelDoc.class);
            System.out.println(hotelDoc);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testUpdateDocument() {
        UpdateRequest request = new UpdateRequest("hotel", "36934");
        request.doc(
                "price", 300,
                "starName", "三钻"
        );
        try {
            client.update(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testDeleteDocument() {
        DeleteRequest request = new DeleteRequest("hotel", "36934");
        try {
            client.delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testBulkRequest() {
        // 1.查询数据
        List<Hotel> hotelList = hotelService.list();

        BulkRequest bulkRequest = new BulkRequest();

        for (Hotel hotel: hotelList) {
            HotelDoc hotelDoc = new HotelDoc(hotel);
            IndexRequest indexRequest = new IndexRequest("hotel").id(hotelDoc.getId().toString());
            indexRequest.source(JSONObject.toJSONString(hotelDoc), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        try {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
