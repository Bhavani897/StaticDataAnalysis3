package com.rite.products.convertrite.model;


import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(name="cr_table_sync_status")
public class CRTableSyncStatus {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long syncId;

    private Long jobId;
    private Long syncEntityId;
    private Long syncStatusId;

    @Column(length = 200)
    private String tableName;

    @Column(length = 200)
    private String status;
}
