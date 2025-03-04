package com.rite.products.convertrite.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

@Entity
@Table(name = "CR_PROJECTS")
public class CrProjects {
        @Id
        @Column(name = "PROJECT_ID")
        private Long projectId;

        @Column(name = "PROJECT_NAME")
        private String projectName;
        @Column(name = "PROJECT_CODE")
        private String projectCode;
        @Column(name = "DESCRIPTION")
        private String description;

        @Column(name = "PROJECT_MANAGER")
        private String projecManager;
        @Column(name = "CLIENT_MANAGER")
        private String clientManager;
        @Column(name = "KPI_Agg_LEVEL")
        private String kpiAggLevel;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
        @Column(name = "START_DATE")
        private Date startDate;
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
        @Column(name = "END_DATE")
        private Date endDate;
        @Column(name = "PROJECT_STATUS")
        private String projectStatus;
        @Column(name = "ACCESS_LEVEL")
        private String accessLevel;
        @Column(name = "CLIENT_PROJECT_NUMBER")
        private String clientProjectNumber;
        @Column(name = "PROGRAM_NUMBER")
        private String programNumber;
    @Column(name = "LAST_UPDATED_BY")
    private String lastUpdatedBy;
    @Column(name = "LAST_UPDATE_DATE")
    private Date lastUpdateDate;

    public String getLastUpdatedBy() {
        return lastUpdatedBy;
    }

    public void setLastUpdatedBy(String lastUpdatedBy) {
        this.lastUpdatedBy = lastUpdatedBy;
    }

    public Date getLastUpdateDate() {
        return lastUpdateDate;
    }

    public void setLastUpdateDate(Date lastUpdateDate) {
        this.lastUpdateDate = lastUpdateDate;
    }


    public String getProjectCode() {
        return projectCode;
    }

    public void setProjectCode(String projectCode) {
        this.projectCode = projectCode;
    }



    public String getKpiAggLevel() {
            return kpiAggLevel;
        }

        public void setKpiAggLevel(String kpiAggLevel) {
            this.kpiAggLevel = kpiAggLevel;
        }

        public Long getProjectId() {
            return projectId;
        }

        public void setProjectId(Long projectId) {
            this.projectId = projectId;
        }

        public String getProjectName() {
            return projectName;
        }

        public void setProjectName(String projectName) {
            this.projectName = projectName;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public String getProjecManager() {
            return projecManager;
        }

        public void setProjecManager(String projecManager) {
            this.projecManager = projecManager;
        }

        public String getClientManager() {
            return clientManager;
        }

        public void setClientManager(String clientManager) {
            this.clientManager = clientManager;
        }

        public Date getStartDate() {
            return startDate;
        }

        public void setStartDate(Date startDate) {
            this.startDate = startDate;
        }

        public Date getEndDate() {
            return endDate;
        }

        public void setEndDate(Date endDate) {
            this.endDate = endDate;
        }

        public String getProjectStatus() {
            return projectStatus;
        }

        public void setProjectStatus(String projectStatus) {
            this.projectStatus = projectStatus;
        }

        public String getAccessLevel() {
            return accessLevel;
        }

        public void setAccessLevel(String accessLevel) {
            this.accessLevel = accessLevel;
        }


        public String getClientProjectNumber() {
            return clientProjectNumber;
        }

        public void setClientProjectNumber(String clientProjectNumber) {
            this.clientProjectNumber = clientProjectNumber;
        }

        public String getProgramNumber() {
            return programNumber;
        }

        public void setProgramNumber(String programNumber) {
            this.programNumber = programNumber;
        }


}
