package com.rite.products.convertrite.service;

public interface CrErpIntegrationService {

	byte[] downloadESSJobExecutionDetails(String requestId, Long podId, String bearerToken) throws Exception;

}
