package com.rite.products.convertrite.service;

import com.rite.products.convertrite.model.CRSourceConnections;
import com.rite.products.convertrite.po.BasicResponsePo;
import com.rite.products.convertrite.respository.CRSourceConnectionsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.catalina.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import java.sql.Timestamp;
import java.util.Optional;

@Service
@Slf4j
public class CRSourceConnectionsService {

    @Autowired
    private CRSourceConnectionsRepository crSourceConnectionsRepository;

    public ResponseEntity<BasicResponsePo> saveCRSourceConnections(CRSourceConnections crSourceConnections, String userId) {
        log.info("Attempting to save CRSourceConnections: {}", crSourceConnections);
        BasicResponsePo response = new BasicResponsePo();
        try {
            crSourceConnections.setCreatedBy(userId);
            crSourceConnections.setUpdatedBy(userId);
            if (crSourceConnectionsRepository.existsByStorageName(crSourceConnections.getStorageName()) && crSourceConnections.getId() == null) {
                log.warn("STORAGE_NAME already exists: {}", crSourceConnections.getStorageName());

                response.setError("true");
                response.setMessage("STORAGE_NAME already exists: " + crSourceConnections.getStorageName());
                response.setPayload(null);
                log.info("Returning failure response: STORAGE_NAME already exists");
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
            }

            Timestamp currentTime = new Timestamp(System.currentTimeMillis());
            crSourceConnections.setCreatedAt(currentTime);
            crSourceConnections.setUpdatedAt(currentTime);
            CRSourceConnections savedEntity = crSourceConnectionsRepository.save(crSourceConnections);

            log.info("CRSourceConnections saved successfully with ID: {}", savedEntity.getId());

            response.setError("false");
            response.setMessage("CRSourceConnections saved successfully");
            response.setPayload(savedEntity);

            log.info("Returning success response");

        } catch (Exception e) {
            log.error("Exception in saveCRSourceConnections----{}", e.getMessage());
            response.setError("true");
            response.setMessage(e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    public ResponseEntity<BasicResponsePo> getConnectionById(Long id) {
        Optional<CRSourceConnections> connection= crSourceConnectionsRepository.findById(id);
        BasicResponsePo response = new BasicResponsePo();
        if (connection.isPresent()) {
            response.setError("false");
            response.setMessage("Connection details fetched successfully");
            response.setPayload(connection.get());
            return ResponseEntity.ok(response);
        } else {
            response.setError("true");
            response.setMessage("Connection not found for ID: " + id);
            response.setPayload(null);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        }
    }

    public ResponseEntity<BasicResponsePo> deleteConnection(Long id) {
        Optional<CRSourceConnections> connection = crSourceConnectionsRepository.findById(id);
        BasicResponsePo response = new BasicResponsePo();
        if (connection.isPresent()) {
            crSourceConnectionsRepository.deleteById(id);
            response.setError("false");
            response.setMessage("Connection deleted successfully");
            return ResponseEntity.ok(response);
        } else {
            response.setError("true");
            response.setMessage("Connection not found for ID: " + id);
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(response);
        }
    }
}
