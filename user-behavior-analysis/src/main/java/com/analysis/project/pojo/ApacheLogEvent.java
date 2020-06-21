package com.analysis.project.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long eventTime;
    private String method;
    private String url;
}
