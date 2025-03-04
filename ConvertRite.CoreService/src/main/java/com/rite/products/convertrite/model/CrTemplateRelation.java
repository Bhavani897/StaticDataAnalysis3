package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "CR_TEMPLATE_RELATION")
public class CrTemplateRelation {
    @Id
    @Column(name = "ID")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    @Column(name = "TEMPLATE_IDS")
    private Long templateIds;
    @Column(name = "IS_ZIPPED")
    private String isZipped;
    @Column(name = "VERSION")
    private String version;
    @Column(name = "GROUP_ID")
    private Long groupId;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
    @Column(name = "CREATION_DATE")
    private Date creationDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdatedDate;
    @Column(name = "CREATED_BY")
    private String createdBy;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdateBy;
}
