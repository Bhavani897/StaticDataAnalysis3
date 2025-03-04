package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;
import java.sql.Date;


@Data
@Entity
@Table(name = "CR_FILE_DETAILS")
public class CrFileDetails {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="FILE_ID")
    private Long fileId;
    @Column(name = "FILE_TYPE")
    private String fileType;
    @Column(name = "FILE_NAME")
    private String fileName;
    @Lob
    @Column(name = "FILE_CONTENT")
    private String fileContent;
    @Column(name = "CREATED_BY")
    public String createdBy;
    @Column(name = "LAST_UPDATE_DATE")
    public String lastUpdateDate;
    @Column(name = "LAST_UPDATED_BY")
    public String lastUpdateBy;
    @Column(name = "CREATION_DATE")
    public Date createDate;
    @Column(name ="CLD_FILE_ID")
    private Long cldFileId;

}
