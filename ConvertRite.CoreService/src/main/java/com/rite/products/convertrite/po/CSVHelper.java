package com.rite.products.convertrite.po;



import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CSVHelper {
    public static String TYPE = "text/csv";
    String[] HEADERs = {"XlsmFileName", "CtlFileName", "ObjectCode", "InterfaceTableName", "SheetName"};

    public static boolean hasCSVFormat(MultipartFile file) {
        return TYPE.equals(file.getContentType())
                || file.getContentType().equals("application/vnd.ms-excel");
    }

    public static List<CloudCSVConfigPojo> csvParser(InputStream is) {
        try (BufferedReader fileReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(fileReader,
                     CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {

            List<CloudCSVConfigPojo> developerTutorialList = new ArrayList<>();

            Iterable<CSVRecord> csvRecords = csvParser.getRecords();

            for (CSVRecord csvRecord : csvRecords) {
                CloudCSVConfigPojo developerTutorial = new CloudCSVConfigPojo();
                developerTutorial.setCtlFileName(csvRecord.get("CtlFileName"));
                developerTutorial.setInterfaceTableName(csvRecord.get("InterfaceTableName"));
                developerTutorial.setObjectCode(csvRecord.get("ObjectCode"));
                developerTutorial.setSheetName(csvRecord.get("SheetName"));
                developerTutorial.setParentObjectName(csvRecord.get("ParentObjectName"));
                developerTutorial.setXlsmFileName(csvRecord.get("XlsmFileName"));
                developerTutorial.setRejectionTableName(csvRecord.get("RejectionTableName"));
                developerTutorialList.add(developerTutorial);
            }

            return developerTutorialList;
        } catch (IOException e) {
            throw new RuntimeException("fail to parse CSV file: " + e.getMessage());
        }
    }
    public static List<CloudCSVConfigPojo> csvFilereader(InputStream is) {
        try (BufferedReader fileReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
             CSVParser csvParser = new CSVParser(fileReader,
                     CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim())) {
            List<CloudCSVConfigPojo> cloudCSVConfigPojos = new ArrayList<>();
            Iterable<CSVRecord> csvRecords = csvParser.getRecords();
            for (CSVRecord csvRecord : csvRecords) {
                CloudCSVConfigPojo csvConfigPojo = new CloudCSVConfigPojo();
                csvConfigPojo.setParentObjectName(csvRecord.get("ParentObjectName"));
                cloudCSVConfigPojos.add(csvConfigPojo);
            }
            return cloudCSVConfigPojos;
        } catch (IOException e) {
            throw new RuntimeException("fail to parse CSV file: " + e.getMessage());
        }
    }
}
