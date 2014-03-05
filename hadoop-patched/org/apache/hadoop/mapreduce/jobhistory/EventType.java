/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package org.apache.hadoop.mapreduce.jobhistory;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public enum EventType { 
  JOB_SUBMITTED, JOB_INITED, JOB_FINISHED, JOB_PRIORITY_CHANGED, JOB_STATUS_CHANGED, JOB_FAILED, JOB_KILLED, JOB_ERROR, JOB_INFO_CHANGED, TASK_STARTED, TASK_FINISHED, TASK_FAILED, TASK_UPDATED, NORMALIZED_RESOURCE, MAP_ATTEMPT_STARTED, MAP_ATTEMPT_FINISHED, MAP_ATTEMPT_FAILED, MAP_ATTEMPT_KILLED, REDUCE_ATTEMPT_STARTED, REDUCE_ATTEMPT_FINISHED, REDUCE_ATTEMPT_FAILED, REDUCE_ATTEMPT_KILLED, SETUP_ATTEMPT_STARTED, SETUP_ATTEMPT_FINISHED, SETUP_ATTEMPT_FAILED, SETUP_ATTEMPT_KILLED, CLEANUP_ATTEMPT_STARTED, CLEANUP_ATTEMPT_FINISHED, CLEANUP_ATTEMPT_FAILED, CLEANUP_ATTEMPT_KILLED, AM_STARTED  ;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"enum\",\"name\":\"EventType\",\"namespace\":\"org.apache.hadoop.mapreduce.jobhistory\",\"symbols\":[\"JOB_SUBMITTED\",\"JOB_INITED\",\"JOB_FINISHED\",\"JOB_PRIORITY_CHANGED\",\"JOB_STATUS_CHANGED\",\"JOB_FAILED\",\"JOB_KILLED\",\"JOB_ERROR\",\"JOB_INFO_CHANGED\",\"TASK_STARTED\",\"TASK_FINISHED\",\"TASK_FAILED\",\"TASK_UPDATED\",\"NORMALIZED_RESOURCE\",\"MAP_ATTEMPT_STARTED\",\"MAP_ATTEMPT_FINISHED\",\"MAP_ATTEMPT_FAILED\",\"MAP_ATTEMPT_KILLED\",\"REDUCE_ATTEMPT_STARTED\",\"REDUCE_ATTEMPT_FINISHED\",\"REDUCE_ATTEMPT_FAILED\",\"REDUCE_ATTEMPT_KILLED\",\"SETUP_ATTEMPT_STARTED\",\"SETUP_ATTEMPT_FINISHED\",\"SETUP_ATTEMPT_FAILED\",\"SETUP_ATTEMPT_KILLED\",\"CLEANUP_ATTEMPT_STARTED\",\"CLEANUP_ATTEMPT_FINISHED\",\"CLEANUP_ATTEMPT_FAILED\",\"CLEANUP_ATTEMPT_KILLED\",\"AM_STARTED\"]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
}
