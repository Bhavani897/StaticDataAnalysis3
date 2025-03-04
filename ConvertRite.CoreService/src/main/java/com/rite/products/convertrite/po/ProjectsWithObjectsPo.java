package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.io.Serializable;
import java.sql.Date;
import java.util.Set;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProjectsWithObjectsPo implements Serializable {
    private Long projectId;
    private String projectName;
    private String projectCode;
    private Long clientId;
    private String pod;
    private String client;
    private Long podId;
    Set<ObjectsPo> objects;
    private Date creationDate;
    private String createdBy;
    private Date lastUpdatedDate;
    private String lastUpdatedBy;

}
