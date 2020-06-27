package com.analysis.project.pojo;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ReceiptEvent {
    private String txId;
    private String payChannel;
    private long eventTime;
}
