package com.rite.products.convertrite.model;
import lombok.Data;

import javax.persistence.*;
import java.sql.Timestamp;

@Entity
@Data
@Table(name = "cr_source_connections")
public class CRSourceConnections {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "storage_type", nullable = false, length = 50)
    private String storageType;

    @Column(name = "host_link", nullable = false, length = 255)
    private String hostLink;

    @Column(name = "auth_details", length = 500)
    private String authDetails;

    @Column(name = "secret_key", length = 500)
    private String secretKey;

    @Column(name = "storage_name", length = 500)
    private String storageName;

    @Column(name = "folder_path", length = 500)
    private String folderPath;

    @Column(name = "db_port", nullable = false)
    private Integer dbPort;

    @Column(name = "db_name", nullable = false, length = 100)
    private String dbName;

    @Column(name = "username", length = 100)
    private String userName;

    @Column(name = "db_password", length = 255)
    private String password;

    @Column(name = "connection_url", length = 1000)
    private String connectionUrl;

    @Column(name = "additional_params", columnDefinition = "CLOB")
    private String additionalParams;

    @Column(name = "created_by", nullable = false, length = 100)
    private String createdBy;

    @Column(name = "created_at", nullable = false)
    private Timestamp createdAt;

    @Column(name = "updated_by", length = 100)
    private String updatedBy;

    @Column(name = "updated_at")
    private Timestamp updatedAt;


}
