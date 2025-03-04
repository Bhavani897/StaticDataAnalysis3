package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CloudLoginDetails {
    public Long credentialId;
    public Long clientId;
    public Long podId;
    public String username;
    public String password;
    public String url;
    public String moduleCode;
}
