package com.analysis.project.pojo;


import java.sql.Timestamp;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MarketingCountView {
    private String behavior;
    private String channel;
    private int count;
    private Timestamp windowStart;
    private Timestamp windowEnd;
}
