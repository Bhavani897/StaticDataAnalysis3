package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class EbsConnectionReqPo {
    private String hostName;
    private String targetUserName;
    private String targetPassword;
    private Integer portNumber;
    private String sid;
    private String databaseName;
    private String targetType;
}
