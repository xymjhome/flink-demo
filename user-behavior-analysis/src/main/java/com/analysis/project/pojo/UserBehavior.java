package com.analysis.project.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
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
    public String behavior;
    // 行为发生的时间戳，单位秒
    private long timestamp;
}
