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
package com.informatica.um.binge;

public abstract class BingeConstants {
    // named json keys - these named constants are shared between UMSM & Binge. should be in sync.
    // make changes together.

    // Common for both source and targets
    public static final String PROJECT_ID = "prjid";
    public static final String DATA_FLOWID = "dfid";
    public static final String ENTITY_ID = "id";
    public static final String ENTITY_NAME = "name";
    public static final String ENTITY_TYPE = "type";
    public static final String ENTITY_SUBTYPE = "subtype";
    public static final String ENTITY_CONFIG = "config";
    public static final String ENTITY_PLUGIN_CONFIG = "plugin";
    public static final String ENTITY_IN_PATHS = "inpaths";
    public static final String ENTITY_OUT_PATHS = "outpaths";
    public static final String ENTITY_PATH = "path";
    public static final String ENTITY_PATH_LEAVES = "leaves";

    // node json keys
    // ## should be in sync with MOND (NodeDef.java).
    public static final String NODE_ID = "id";
    public static final String NODE_NAME = "name";
    public static final String NODE_DATA = "data";
    public static final String NODE_HOSTS = "hosts";

    // public static final String SRC_TOPIC = "topic";
    // public static final String SRC_TARGETS = "targets";
    // public static final String SRC_TRANSFORMS = "transforms";
    public static final String SRC_FLIGHT_SIZE = "flight_size";

    public static final String SRC_CFG_DIRECTORY = "directory";
    public static final String SRC_CFG_FILENAME = "filename";
    public static final String SRC_CFG_REGEX_FILENAME = "filename_regex";
    public static final String SRC_CFG_HOST = "host";
    public static final String SRC_CFG_TYPE = "type";
    public static final String SRC_CFG_TYPE_TCP = "TCP";
    public static final String SRC_CFG_TYPE_UDP = "UDP";

    public static final String TARGET_TOPICS = "topics";

    public static final String TARGET_CFG_HDFS_BINARY = "binary";
    public static final String TARGET_CFG_CAS_DEFAULT_CF = "default_column_family";
    public static final String TARGET_CFG_CAS_HOST = "host";
    public static final String TARGET_CFG_CAS_KEYSPACE = "keyspace";
    public static final String TARGET_CFG_CAS_CLUSTER = "cluster";

    // named json values - these named constants are shared between UMSM & Binge. should be in sync.
    // make changes together.
    public static final int UDS_FILE_SOURCE = 0;
    public static final int UDS_TCP_SOURCE = 1;
    public static final int UDS_UDP_SOURCE = 2;
    public static final int UDS_SYSLOG_SOURCE = 3;
    public static final int UDS_JSON_SOURCE = 4;
    public static final int UDS_MQTT_SOURCE = 5;
    // Introduced to make factory code cleaner.
    public static final int UDS_REGEX_FILE_SOURCE = 6;

    // UDS target entity types file and console target types are internal only
    public static final int UDS_CONSOLE_TARGET = 999;
    public static final int UDS_FILE_TARGET = 998;
    public static final int UDS_CASANDRA_TARGET = 1001;
    public static final int UDS_HDFS_TARGET = 1002;

    // UDS transformation types.
    public static final int UDS_REGEX_TRANSFORM = 2001;
    public static final int UDS_NODUPES_TRANSFORM = 2002;
    public static final int UDS_JS_TRANSFORM = 2003;
    public static final int UDS_CUSTOM_TRANSFORM = 2004;
    public static final int UDS_RECORD_PARSER_TRANSFORM = 2005;

    // um configuration keys
    public static final String UMQ_SRC_APPSET = "umq_ulb_application_set";
    public static final String UMQ_SRC_RCVR_PORTION = "umq_ulb_receiver_portion";
    public static final String UMQ_SRC_ULB_EVENTS = "umq_ulb_events";
    public static final String UMQ_SRC_FLIGHT_SIZE = "umq_ulb_flight_size";

    public static final int UMQ_DEFAULT_RCVR_PORTION = 1000;
    public static final String UMQ_DEFAULT_SRC_ULB_EVENTS = "MSG_CONSUME,MSG_COMPLETE,RCV_REGISTRATION";

    public static final String UMQ_RCVR_RCV_TYPE_ID = "umq_receiver_type_id";
    public static final String UM_UNICAST_DAEMON = "resolver_unicast_daemon";
    public static final String UM_MULTICAST_ADDR = "resolver_multicast_address";

    public static final String INSTALL_DIR = "INFA_HOME";
    public static final String WORK_DIR_NAME = "work";
    public static final String CONFIG_PATH = "config";
    public static final String PLUGINS_PATH = "plugins";
    public static final String CUSTOM_PLUGINS = "custom";
    public static final String INFA_PLUGINS = "infa";
    public static final String NATIVE = "native";
    public static final String LIB = "lib";
    public static final String BINGE_CONFIG_FILENAME = "node.cnf";

    // BINGE data flow entity types.
    public static final int BINGE_DF_SOURCE_ENTITY = 0;
    public static final int BINGE_DF_TARGET_ENTITY = 1;
    public static final int BINGE_DF_TRANSFORM_ENTITY = 2;
    // zk paths
    public static final String SRC_CFG_MQTT_URL = "url";
    public static final String SRC_CFG_MQTT_TOPIC = "topics";
    public static final String SRC_CFG_MQTT_USERNAME = "user";
    public static final String SRC_CFG_MQTT_PASSWORD = "pass";

    // dataflow configuration
    public static final String DATAFLOW_CONFIG = "data";
    public static final String RESOLUTION = "resolution";
    public static final String ADDRESS = "address";

    // should be in sync with mond (BingeDeployer.java). DON'T EDIT.
    public static final String BINGE_TOPOLOGY = "topology";
    public static final String BINGE_DATAFLOWS = "dataflows";
    public static final String BINGE_NODES = "nodes";
    public static final String BINGE_ENTITIES = "entities";
    public static final String BINGE_LOCKS = "locks";

    // stats related
    public static final int BINGE_APP_TYPE = 101;
    public static final int BINGE_SOURCE_ENTITY = 102;
    public static final int BINGE_TARGET_ENTITY = 103;
    public static final int BINGE_TRANSFORM_ENTITY = 104;
    public static final int BINGE_SOURCE_STATS = 105;
    public static final int BINGE_TARGET_STATS = 106;
    public static final int BINGE_TRANSFORM_STATS = 107;
    public static final int BINGE_APP_ACTIVE_STATE = 1;
    public static final int BINGE_APP_STANDBY_STATE = 2;

    // TODO - the below constants are for record parser transform special handling. These can be removed once the record
    // parser transform is handled at design time
    public static final String RECORD_PARSER_TRANSFORM = "record-parser-transform";
    public static final String RECORD_PARSER = "com.informatica.binge.transforms.parser.RecordParser";
    public static final String RECORD_PARSE_ENTITY_SUB_TYPE = "ent_sub_type";
    public static final String RECORD_PARSE_ENTITY_TYPE = "ent_type";

    // TODO - Remove it. It should be passed as part of VDSEvent headers.
    public static final String SRC_CFG_SEPARATOR = "sepValue";
    public static final String SRC_CFG_SEPARATOR_FLAG = "sepFlag";

    // Flags to be sent as part of event
    public static final long VDS_RECORD_TYPE = 0x01;

    // general
    public static final String DEFAULT_CHARSET = "UTF-8";
    public static final String ENV_HADOOP_BASEDIR = "HADOOPBASEDIR";
    public static final String ENV_NODENAME = "nodename";
}
