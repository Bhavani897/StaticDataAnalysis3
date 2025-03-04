package com.rite.products.convertrite.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rite.products.convertrite.model.CrScheduleJobView;
import com.rite.products.convertrite.model.CrScheduleMaster;
import com.rite.products.convertrite.po.BasicResPo;
import com.rite.products.convertrite.po.CrDataSyncScheduleReqPo;
import com.rite.products.convertrite.service.CrScheduleService;

import java.util.List;
import java.util.Map;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/convertritecore/schedule")
public class CrScheduleController {

    @Autowired
    private CrScheduleService scheduleService;

    //    @PostMapping("/create")
//    public ResponseEntity<BasicResPo> createSchedule(@RequestBody CrDataSyncScheduleReqPo schedule, HttpServletRequest request) throws Exception {
//        BasicResPo resPo = scheduleService.saveSchedule(schedule,request);
//        return ResponseEntity.ok(resPo);
//    }
    @PostMapping("/create")
    public ResponseEntity<BasicResPo> createSchedule(
            @RequestParam("data") String jsonMetadata,
            @RequestParam List<MultipartFile> files, HttpServletRequest request) throws Exception {
        CrDataSyncScheduleReqPo schedule = new ObjectMapper().readValue(jsonMetadata, CrDataSyncScheduleReqPo.class);

        BasicResPo resPo = scheduleService.saveSchedule(schedule, files,request);
        return ResponseEntity.ok(resPo);
    }
    @PostMapping("/filesystem/create")
    public ResponseEntity<BasicResPo> createScheduleWithFiles(
            @RequestParam("data") String jsonMetadata,
            @RequestParam List<MultipartFile> files, HttpServletRequest request) throws Exception {
        CrDataSyncScheduleReqPo schedule = new ObjectMapper().readValue(jsonMetadata, CrDataSyncScheduleReqPo.class);

        BasicResPo resPo = scheduleService.saveSchedule(schedule, files,request);
        return ResponseEntity.ok(resPo);
    }
    @PutMapping("/terminate")
    public ResponseEntity<BasicResPo> terminateSchedule(@RequestParam Long scheduleId, HttpServletRequest request) throws Exception {
        BasicResPo resPo = scheduleService.terminateSchedule(scheduleId, request);
        return ResponseEntity.ok(resPo);
    }

    @GetMapping("/schedule-job-views")
    public ResponseEntity<?> getScheduleJobViews(
            @RequestParam @NotNull(message = "Page cannot be null") @Min(value = 0, message = "Page number must be 0 or greater") int page,
            @RequestParam @NotNull(message = "Size cannot be null") @Min(value = 1, message = "Size must be greater than 0") @Max(value = 100, message = "Size cannot be more than 100") int size,
            @RequestParam @NotNull(message = "The text should be all ") List<String> statuses
    ) {

        try {
            Pageable pageable = PageRequest.of(page, size);
            // Fetch data from the service
            Page<CrScheduleJobView> scheduleJobViews = scheduleService.getAllScheduleJobViews(pageable, statuses);
            if (scheduleJobViews.isEmpty()) {
                return ResponseEntity.status(HttpStatus.NO_CONTENT).body("No schedule job views found.");
            }
            return new ResponseEntity<>(scheduleJobViews, HttpStatus.OK);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("An error occurred while fetching schedule job views: " + e.getMessage());
        }
    }
}

