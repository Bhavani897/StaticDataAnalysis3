package com.rite.products.convertrite.po;

import com.rite.products.convertrite.model.CrCustomSourceTableDtls;
import lombok.Data;

@Data
public class CrCustomTableDtlsResPo {

    private CrCustomSourceTableDtls customSourceTableDtls;
    private String metadataTableName;

    public CrCustomTableDtlsResPo(CrCustomSourceTableDtls customSourceTableDtls, String metadataTableName) {
        this.customSourceTableDtls = customSourceTableDtls;
        this.metadataTableName = metadataTableName;
    }
}
