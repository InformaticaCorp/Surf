/* 
 * Copyright 2014 Informatica Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.informatica.vds.api;

/**
 * 
 *
 */
public enum VDSErrorCode {

    /**
     * Node is unable to connect to ADMIND. Check if ADMIND is running and is reachable from the node host
     * using URL given in node.cnf file.
     */
    ADMIND_CONNECTION_ERROR(
            10000,
            "Node is unable to connect to ADMIND. Check if ADMIND is running and is reachable from the node host using the URL [%s]"),
    
    /**
     * Failed to download plugin from admind.
     */
    PLUGIN_DOWNLOAD_FAILED(10001, "Failed to download plugin from ADMIND. connect url: [%s]"),
    
    /**
     * Exception happened while trying to obtain the hostname of the machine on which this application is running.
     */
    UNKNOWN_HOST(10002, "Host couldn't be resolved"),
    
    /**
     * Error when not able to connect to MQTT Broker
     */
    MQTT_CONNECTION_EXCEPTION(10003, "MQTT Connection exception"),

    /**
     * Error when expecting a plain file but found a directory/symlink
     */
    NOT_A_FILE(10004, "[%s] is not a file. verify if this is directory/symlink instead"),
    
    /**
     * error due to insufficient permissions
     */
    INSUFFICIENT_PERMISSIONS(10005, "insufficient permissions for path: [%s], expected permissions: [%s]"),
    
    /**
     * hadoop environment variable is not set. needed to run hadoop target.
     */
    HADOOP_NOT_SET(10006, "Failed to start hdfs target. Mandatory environment variable: [%s] not found."),

    /**
     * Zookeeper connection lost. This is a fatal error which will shutdown the node.
     */
    ZK_CONNECTION_LOST(10007, "zookeeper connection lost"),

    /**
     * Error when expecting a existing directory at given path
     */
    DIRECTORY_NOT_FOUND(10008, "directory not found at path [%s]"),

    /**
     * Error when expecting a existing file at given path
     */
    FILE_NOT_FOUND(10009, "file not found at path [%s]"),

    /**
     * Regex parse error.
     */
    REGEX_SYNTAX_ERROR(10010, "syntax error in regex [%s]"),

    /**
     * json format error.
     */
    JSON_PARSE_ERROR(10011, "json parse error occurred while expecting a json array in file [%s]"),

    /**
     * malformed admind URL in the node.cnf file.
     */
    MALFORMED_ADMIND_URL(10012,
            "ADMIND confUrl [%s] specified in the node.cnf is malformed. It should be of the form http://host:port"),

    /**
     * unable to create plugin folder
     */
    PLUGIN_FOLDER_CREATE_ERROR(10013, "Unable to create plugin folder [%s]. Check the permisssions of parent folder."),

    @Deprecated
    TEST_ERROR(20000, "test error code");
    // DON'T ADD ANY ERROR CODE AFTER THIS
    
    private String msg;
    private short code;

    /**
     * 
     * @param code - Error code
     * @param msg - Error message
     */
    VDSErrorCode(int code, String msg) {
        this.code = (short) code;
        this.msg = msg;
    }

    /**
     * Get the error code
     * @return
     */
    public int getCode() {
        return code;
    }

    /**
     * Get the error message
     * @param args - Dynamic arguments to the error message
     * @return
     */
    public String getMessage(Object[] args) {
        return String.format(msg, args);
    }

}
