package com.rite.products.convertrite.multitenancy.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.sql.Date;

@Entity
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "cr_pod_information")
public class Pod {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "pod_id")
    private Long podId;

    @Column(name = "pod_name")
    private String podName;

    @Column(name = "pod_db_user")
    private String podDbUser;

    @Column(name = "pod_db_password")
    private String podDbPassword;

    @Column(name = "pod_target_url")
    private String podTargetUrl;

    @Column(name = "pod_tablespace_size")
    private String tablespaceSize;

    @Column(name="scheduled_job_flag")
    private String scheduledJobFlag;


    /*@Column(name = "license_id", insertable = false, updatable = false)
    private Long licenseId;

    @OneToOne
    @JoinColumn(name = "license_id", referencedColumnName = "license_id")
    private License license;

    @OneToOne
    @JoinColumn(name = "client_id", referencedColumnName = "client_id")
    private Client client;*/

    @Column(name = "creation_date")
    private Date creationDate;
    @Column(name = "created_by")
    private String createdBy;
    @Column(name = "last_update_date")
    private Date lastUpdatedDate;
    @Column(name = "last_update_by")
    private String lastUpdatedBy;

    public String getScheduledJobFlag() {
        return scheduledJobFlag;
    }

    public void setScheduledJobFlag(String scheduledJobFlag) {
        this.scheduledJobFlag = scheduledJobFlag;
    }

    public Long getPodId() {
        return podId;
    }

    public void setPodId(Long podId) {
        this.podId = podId;
    }

    public String getPodName() {
        return podName;
    }

    public void setPodName(String podName) {
        this.podName = podName;
    }

    public String getPodDbUser() {
        return podDbUser;
    }

    public void setPodDbUser(String podDbUser) {
        this.podDbUser = podDbUser;
    }

    public String getPodDbPassword() {
        return podDbPassword;
    }

    public void setPodDbPassword(String podDbPassword) {
        this.podDbPassword = podDbPassword;
    }

    public String getPodTargetUrl() {
        return podTargetUrl;
    }

    public void setPodTargetUrl(String podTargetUrl) {
        this.podTargetUrl = podTargetUrl;
    }

    public String getTablespaceSize() {
        return tablespaceSize;
    }

    public void setTablespaceSize(String tablespaceSize) {
        this.tablespaceSize = tablespaceSize;
    }

    /*public License getLicense() {
        return license;
    }

    public void setLicense(License license) {
        this.license = license;
    }

    public Long getLicenseId() {
        return licenseId;
    }

    public void setLicenseId(Long licenseId) {
        this.licenseId = licenseId;
    }

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }*/

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getLastUpdatedDate() {
        return lastUpdatedDate;
    }

    public void setLastUpdatedDate(Date lastUpdatedDate) {
        this.lastUpdatedDate = lastUpdatedDate;
    }

    public String getLastUpdatedBy() {
        return lastUpdatedBy;
    }

    public void setLastUpdatedBy(String lastUpdatedBy) {
        this.lastUpdatedBy = lastUpdatedBy;
    }
}
