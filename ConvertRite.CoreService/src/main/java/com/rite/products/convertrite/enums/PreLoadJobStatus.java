package com.rite.products.convertrite.enums;

public enum PreLoadJobStatus {

   SETUP_INITIATED("0. Setup Initiated"),
    METADATA_SUCCESS("1. MetaData Created Successfully"),
    CLD_TEMPLATE_SUCCESS("2. Cloud Template Created Successfully"),
    CREATE_CLD_STGTABLE_SUCCESS("3. Cloud Staging table created Successfully"),
    SYNC_VALIDATION_TABLE_SUCCESS ("1. Validation Tables Created Successfully"),
    METADATA_LOAD_ERROR("METADATA_LOAD_ERROR"),
    CLOUD_TEMPLATE_LOAD_ERROR("CLOUD_TEMPLATE_LOAD_ERROR"),
    DB_CONNECTION_ERROR("DB_CONNECTION_ERROR"),
    SYNC_VALIDATION_TABLE_ERROR("SYNC_VALIDATION_TABLE_ERROR"),
    CREATE_STAGING_TABLE_ERROR("CREATE_STAGING_TABLE_ERROR"),
    SUCCESS("SUCCESS");
    private final String value;
    PreLoadJobStatus(String value){
        this.value=value;
    }

    public String getValue() {
        return value;
    }


}
