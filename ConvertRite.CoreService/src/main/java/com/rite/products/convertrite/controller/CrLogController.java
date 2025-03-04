package com.rite.products.convertrite.controller;

import com.rite.products.convertrite.service.CrLogService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@RestController
@RequestMapping("/api/convertritecore")
@Slf4j
//@CrossOrigin
public class CrLogController {

    @Value("${logging.file.name}")
    private String coreLogFilePath;

    @Autowired
    CrLogService crLogService;

    @GetMapping("/logfile")
    @ResponseBody
    public void viewLogs(@RequestParam(required = false) String date,HttpServletResponse response) throws Exception {
        Path logPath;
        String logFileName;
        String filePath = coreLogFilePath;

        if (date != null) {
            logFileName = filePath + "." + date + ".0.gz";
        } else {
            logFileName = filePath;
        }

        logPath = Paths.get(logFileName).toAbsolutePath();
        log.info("Fetching log file from path: {}", logPath);

        if (!Files.exists(logPath)) {
            throw new IllegalArgumentException("Log file not found for the specified date.");
        }

        if (logFileName.endsWith(".gz")) {
            crLogService.convertDecompressedLogFileToString(logPath, response);
        } else {
            crLogService.convertLogFileToString(logPath, response);
        }
    }

}
