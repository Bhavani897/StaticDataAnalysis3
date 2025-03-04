package com.rite.products.convertrite.model;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name="CR_SOURCE_TABLES")
//@NamedStoredProcedureQueries({
//        @NamedStoredProcedureQuery(name = "source_metadata_sp",
//                procedureName = "CR_SOURCE_METADATA_PROC",
//                parameters = {
//                        @StoredProcedureParameter(mode = ParameterMode.IN, name = "p_file_name", type = String.class),
//                        @StoredProcedureParameter(mode = ParameterMode.IN, name = "p_user_id", type = String.class),
//                        @StoredProcedureParameter(mode = ParameterMode.IN, name = "p_object_id", type = Long.class),
//                        @StoredProcedureParameter(mode = ParameterMode.OUT, name = "p_status", type = String.class)
//                })
//})
@IdClass(CrSourceTableId.class)
public class CrSourceTable {
    @Id
    @Column(name = "TABLE_ID")
    private long tableId;

    @Column(name="OBJECT_ID")
    private Long objectId;
    @Id
    @Column(name = "TABLE_NAME")
    private String tableName;


}
