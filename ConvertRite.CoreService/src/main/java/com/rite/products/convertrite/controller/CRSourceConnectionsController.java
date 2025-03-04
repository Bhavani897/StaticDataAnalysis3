package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.model.CRSourceConnections;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.service.CRSourceConnectionsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/convertritecore")
@Slf4j
public class CRSourceConnectionsController {
    @Autowired
    private CRSourceConnectionsService crSourceConnectionsService;

    @PostMapping("/saveSourceConnectionDetails")
    public ResponseEntity<BasicResponsePo> addCRSourceConnections(
            @RequestBody CRSourceConnections crSourceConnections,
            @RequestHeader("userId") String userId) {

        log.info("Received request to add CRSourceConnections: {}", crSourceConnections);
        ResponseEntity<BasicResponsePo> response = crSourceConnectionsService.saveCRSourceConnections(crSourceConnections, userId);

        if (response.getStatusCode() == HttpStatus.CREATED) {
            log.info("CRSourceConnections saved successfully.");
        } else {
            log.error("Error saving CRSourceConnections: {}", response.getBody().getMessage());
        }
        return response;
    }

    @DeleteMapping("/delete/{id}")
    public ResponseEntity<BasicResponsePo> deleteConnection(@PathVariable Long id) {
        log.info("Received request to delete connection by ID: {}", id);
        return crSourceConnectionsService.deleteConnection(id);
    }
}