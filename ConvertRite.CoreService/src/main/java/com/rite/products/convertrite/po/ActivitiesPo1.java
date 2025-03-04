package com.rite.products.convertrite.po;

import com.fasterxml.jackson.annotation.JsonFormat;

import javax.persistence.Entity;
import java.io.Serializable;
import java.sql.Date;

public class ActivitiesPo1 implements
        Serializable {
    public Integer getProject_id() {
        return project_id;
    }

    public void setProject_id(Integer project_id) {
        this.project_id = project_id;
    }

    public Integer getTask_id() {
        return task_id;
    }

    public void setTask_id(Integer task_id) {
        this.task_id = task_id;
    }

    public Integer getSeq() {
        return seq;
    }

    public void setSeq(Integer seq) {
        this.seq = seq;
    }

    public Integer getWbs_id() {
        return wbs_id;
    }

    public void setWbs_id(Integer wbs_id) {
        this.wbs_id = wbs_id;
    }

    public Integer getPod_id() {
        return pod_id;
    }

    public void setPod_id(Integer pod_id) {
        this.pod_id = pod_id;
    }

    public String getTask_num() {
        return task_num;
    }

    public void setTask_num(String task_num) {
        this.task_num = task_num;
    }

    public String getTask_name() {
        return task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public Integer getObject_id() {
        return object_id;
    }

    public void setObject_id(Integer object_id) {
        this.object_id = object_id;
    }

    public Integer getParent_object_id() {
        return parent_object_id;
    }

    public void setParent_object_id(Integer parent_object_id) {
        this.parent_object_id = parent_object_id;
    }

    public String getTask_type() {
        return task_type;
    }

    public void setTask_type(String task_type) {
        this.task_type = task_type;
    }

    public String getPre_req_task() {
        return pre_req_task;
    }

    public void setPre_req_task(String pre_req_task) {
        this.pre_req_task = pre_req_task;
    }

    public Date getStart_date() {
        return start_date;
    }

    public void setStart_date(Date start_date) {
        this.start_date = start_date;
    }

    public Date getEnd_date() {
        return end_date;
    }

    public void setEnd_date(Date end_date) {
        this.end_date = end_date;
    }

    public Integer getWeightage() {
        return weightage;
    }

    public void setWeightage(Integer weightage) {
        this.weightage = weightage;
    }

    public Integer getComplete_percentage() {
        return complete_percentage;
    }

    public void setComplete_percentage(Integer complete_percentage) {
        this.complete_percentage = complete_percentage;
    }

    public Integer getLegacy_resource_id() {
        return legacy_resource_id;
    }

    public void setLegacy_resource_id(Integer legacy_resource_id) {
        this.legacy_resource_id = legacy_resource_id;
    }

    public String getTask_status() {
        return task_status;
    }

    public void setTask_status(String task_status) {
        this.task_status = task_status;
    }

    public Integer getDestination_resource_id() {
        return destination_resource_id;
    }

    public void setDestination_resource_id(Integer destination_resource_id) {
        this.destination_resource_id = destination_resource_id;
    }

    public Integer getTask_owner_id() {
        return task_owner_id;
    }

    public void setTask_owner_id(Integer task_owner_id) {
        this.task_owner_id = task_owner_id;
    }

    public String getCompletion_flag() {
        return completion_flag;
    }

    public void setCompletion_flag(String completion_flag) {
        this.completion_flag = completion_flag;
    }

    public Integer getCloud_resource_id() {
        return cloud_resource_id;
    }

    public void setCloud_resource_id(Integer cloud_resource_id) {
        this.cloud_resource_id = cloud_resource_id;
    }

    public Integer getIntegrator_resource_id() {
        return integrator_resource_id;
    }

    public void setIntegrator_resource_id(Integer integrator_resource_id) {
        this.integrator_resource_id = integrator_resource_id;
    }

    public Integer getClient_resource_id() {
        return client_resource_id;
    }

    public void setClient_resource_id(Integer client_resource_id) {
        this.client_resource_id = client_resource_id;
    }

    public String getAttribute1() {
        return attribute1;
    }

    public void setAttribute1(String attribute1) {
        this.attribute1 = attribute1;
    }

    public String getAttribute2() {
        return attribute2;
    }

    public void setAttribute2(String attribute2) {
        this.attribute2 = attribute2;
    }

    public String getAttribute3() {
        return attribute3;
    }

    public void setAttribute3(String attribute3) {
        this.attribute3 = attribute3;
    }

    public String getAttribute4() {
        return attribute4;
    }

    public void setAttribute4(String attribute4) {
        this.attribute4 = attribute4;
    }

    public String getAttribute5() {
        return attribute5;
    }

    public void setAttribute5(String attribute5) {
        this.attribute5 = attribute5;
    }

    private Integer project_id;
    private Integer task_id;
    private Integer seq;
    private Integer wbs_id;
    private Integer pod_id;
    private String task_num;
    private String task_name;
    private Integer object_id;
    private Integer parent_object_id;
    private String task_type;
    private String pre_req_task;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
    private Date start_date;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "Asia/Kolkata")
    private Date end_date;
    private Integer weightage;
    private Integer complete_percentage;
    private Integer legacy_resource_id;
    private String task_status;
    private Integer destination_resource_id;
    private Integer task_owner_id;
    private String completion_flag;
    private Integer cloud_resource_id;
    private Integer integrator_resource_id;
    private Integer client_resource_id;
    private String attribute1;
    private String attribute2;
    private String attribute3;
    private String attribute4;
    private String attribute5;

}