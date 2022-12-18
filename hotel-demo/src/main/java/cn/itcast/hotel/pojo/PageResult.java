package cn.itcast.hotel.pojo;

import java.util.List;

public class PageResult {
    private Long total;
    private List<HotelDoc> hotels;

    public PageResult() {
    }

    public PageResult(Long total, List<HotelDoc> hotels) {
        this.total = total;
        this.hotels = hotels;
    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public List<HotelDoc> getHotels() {
        return hotels;
    }

    public void setHotels(List<HotelDoc> hotels) {
        this.hotels = hotels;
    }

}
