package com.doris.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class UserBehavior {
    //用户id
    private long userId;
    //商品id
    private long itemId;
    //商品类目id
    private int categoryId;
    // 用户行为, 包括("pv", "buy", "cart", "fav")
    private String behavior;
    // 行为发生的时间戳，单位秒
    private long timestamp;
    private int pvFlag;
    private int buyFlag;
    private int cartFlag;
    private int favFlag;


    @Override
    public String toString() {

        //user_id,item_id,category_id,behavior,event_timestamp,pv,buy,cart,fav
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s,%s",
            userId, itemId, categoryId, behavior, timestamp, pvFlag, buyFlag, cartFlag, favFlag);
    }


}
