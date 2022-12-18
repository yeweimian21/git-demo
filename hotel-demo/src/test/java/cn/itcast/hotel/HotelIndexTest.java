package cn.itcast.hotel;

import cn.itcast.hotel.constants.HotelConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class HotelIndexTest {

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
    void testInit() {
        System.out.println(client);
    }

    @Test
    void createHotelIndex() {
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        request.source(HotelConstants.MAPPING_TEMPLATE, XContentType.JSON);
        try {
            client.indices().create(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testDeleteHotelIndex() {
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        try {
            client.indices().delete(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testExistHotelIndex() {
        GetIndexRequest request = new GetIndexRequest("hotel");
        try {
            boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
            if (exists) {
                System.err.println("hotel索引库存在");
            } else {
                System.err.println("hotel索引库不存在");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
