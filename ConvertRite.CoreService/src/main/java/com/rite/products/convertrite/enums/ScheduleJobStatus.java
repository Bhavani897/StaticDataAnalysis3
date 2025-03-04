package com.rite.products.convertrite.enums;

public enum ScheduleJobStatus {

    PENDING("PENDING"),
    IN_PROGRESS("IN_PROGRESS"),
    SUCCESS("SUCCESS"),
    ERROR("ERROR"),
    WARNING("WARNING"),
    TERMINATED("TERMINATED"),

    // Data Load statuses
    DATA_LOAD_IN_PROGRESS("DIP"),
    DATA_LOAD_SUCCESS("DS"),
    DATA_LOAD_ERROR("DE"),

    // Transformation statuses
    TRANSFORMATION_IN_PROGRESS("TIP"),
    TRANSFORMATION_SUCCESS("TS"),
    TRANSFORMATION_ERROR("TE"),

    // Import statuses
    IMPORT_IN_PROGRESS("IIP"),
    IMPORT_SUCCESS("IS"),
    IMPORT_ERROR("IE"),

    // Syncing statuses
    DATA_SYNC_IN_PROGRESS("DSI"),
    DATA_SYNC_SUCCESS("DSS"),
    DATA_SYNC_ERROR("DSE");
    private final String code;

    ScheduleJobStatus(String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public static ScheduleJobStatus fromCode(String code) {
        for (ScheduleJobStatus status : values()) {
            if (status.code.equals(code)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Invalid JobStatus code: " + code);
    }
}

