package com.rite.products.convertrite.po;

import lombok.Data;

@Data
public class CrSourceConnectionResPo {
    private String authDetails;
    private String secretKey;
    private String folderPath;
    private String dbName;
    private String connectionUrl;
    private String additionalParams;

    // Connection-related fields
    private Long connectionId;
    private String connectionName;
    private String hostName;
    private String serviceName;
    private String userName;
    private String password;
    private int port;
    private String databaseLink;
    private String connectionType;
}

