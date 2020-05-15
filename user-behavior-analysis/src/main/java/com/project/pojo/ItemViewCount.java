package com.project.pojo;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


//@Data(staticConstructor = "test") staticConstructor参数不生效
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
/** 商品点击量(窗口操作的输出类型) */
public class ItemViewCount {

    public long itemId;     // 商品ID
    public long windowEnd;  // 窗口结束时间戳
    public long viewCount;  // 商品的点击量
}

