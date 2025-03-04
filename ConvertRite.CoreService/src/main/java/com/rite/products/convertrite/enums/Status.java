package com.rite.products.convertrite.enums;

public enum Status {

    PENDING("Pending"),
    IN_PROGRESS("InProgress"),
    PROCESSING("Processing"),
    ERROR("Error"),
    COMPLETED("Completed"),
    WARNING("Warning"),

    SUCCESS("Success") ;
    private final String status;

    Status(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }
}

