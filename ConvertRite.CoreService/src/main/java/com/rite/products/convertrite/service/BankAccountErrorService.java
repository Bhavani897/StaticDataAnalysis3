package com.rite.products.convertrite.service;

import com.opencsv.CSVWriter;
import com.rite.products.convertrite.model.CrBankAccountErrors;
import com.rite.products.convertrite.model.CrCreateBankBranchErrors;
import com.rite.products.convertrite.respository.CrBankAccountErrorsRepository;
import com.rite.products.convertrite.respository.CrCreateBankBranchErrorsRepository;
import com.rite.products.convertrite.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.persistence.Column;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@Slf4j
@Service
public class BankAccountErrorService {

    @Autowired
    private CrBankAccountErrorsRepository crBankAccountErrorsRepository;
    @Autowired
    private CrCreateBankBranchErrorsRepository crCreateBankBranchErrorsRepository;

    public byte[] downloadBankAccountErrorRecords(Long cldTempId, String batchName) throws IOException {
        List<CrBankAccountErrors> crBankAccountErrorsList = crBankAccountErrorsRepository.findAllByCldTempIdAndCrBatchName(cldTempId,batchName);
        log.info("crBankAccountErrorsList---->" + crBankAccountErrorsList.size());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(outputStream))) {

            // Get headers dynamically from the class
            List<String> headers = Utils.getHeaders(CrBankAccountErrors.class);
            writer.writeNext(headers.toArray(new String[0]));
            log.info("headers---->" + headers.size());
            // Write data rows
            for (CrBankAccountErrors error : crBankAccountErrorsList) {
                String[] record = new String[headers.size()+1];
                // Map field names to column values
                for (Field field : CrBankAccountErrors.class.getDeclaredFields()) {
                    field.setAccessible(true);
                    try {
                        // Use the field's name as header and get its value
                        String fieldName = field.getName();
                        Column columnAnnotation = field.getAnnotation(Column.class);
                        int headerIndex = headers.indexOf(columnAnnotation.name());
                        if (headerIndex != -1) {
                            Object value = field.get(error);
                            if(value!=null){
                                record[headerIndex] = (value != null) ? value.toString() : "";
                            }
                        } else {
                            // Log if header is not found
                            log.warn("No header found for field: {}", fieldName);
                        }
                    } catch (Exception e) {
                       // e.printStackTrace();
                        log.warn("Exception in generateCsv : {}", e.getMessage());
                    }
                }
                writer.writeNext(record);
            }
        }
        return outputStream.toByteArray();
    }
    public byte[] downloadBankOrBranchesErrorRecords(Long cldTempId, String batchName) throws IOException {
        List<CrCreateBankBranchErrors> crBankErrorsList = crCreateBankBranchErrorsRepository.findAllByCldTempIdAndCrBatchName(cldTempId,batchName);
        log.info("crBankErrorsList---->" + crBankErrorsList.size());

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (CSVWriter writer = new CSVWriter(new OutputStreamWriter(outputStream))) {

            // Get headers dynamically from the class
            List<String> headers = Utils.getHeaders(CrCreateBankBranchErrors.class);
            writer.writeNext(headers.toArray(new String[0]));
            log.info("headers---->" + headers.size());
            // Write data rows
            for (CrCreateBankBranchErrors error : crBankErrorsList) {
                String[] record = new String[headers.size()+1];
                // Map field names to column values
                for (Field field : CrCreateBankBranchErrors.class.getDeclaredFields()) {
                    field.setAccessible(true);
                    try {
                        // Use the field's name as header and get its value
                        String fieldName = field.getName();
                        Column columnAnnotation = field.getAnnotation(Column.class);
                        int headerIndex = headers.indexOf(columnAnnotation.name());
                        if (headerIndex != -1) {
                            Object value = field.get(error);
                            if(value!=null){
                                record[headerIndex] = (value != null) ? value.toString() : "";
                            }
                        } else {
                            // Log if header is not found
                            log.warn("No header found for field: {}", fieldName);
                        }
                    } catch (Exception e) {
                        // e.printStackTrace();
                        log.warn("Exception in generateCsv : {}", e.getMessage());
                    }
                }
                writer.writeNext(record);
            }
        }
        return outputStream.toByteArray();
    }

}
