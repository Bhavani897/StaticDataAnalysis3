package com.rite.products.convertrite.model;

import lombok.Data;

import java.io.Serializable;
@Data
public class ProjectObjectId implements Serializable {
    public Integer objectId;
    public Long projectId;
}
