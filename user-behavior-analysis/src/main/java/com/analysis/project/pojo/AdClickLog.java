package com.analysis.project.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AdClickLog {

    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long  timestamp;
}
