package com.analysis.project.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class LoginEvent {
    private long userId;
    private String ip;
    private String eventType;
    private long eventTime;
}
