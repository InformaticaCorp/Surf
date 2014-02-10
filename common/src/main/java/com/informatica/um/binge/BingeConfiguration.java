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

import static com.informatica.um.binge.BingeConstants.ENV_NODENAME;
import static com.informatica.um.binge.BingeConstants.INSTALL_DIR;
import static com.informatica.um.binge.BingeConstants.WORK_DIR_NAME;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;
/**
 * To load the binge boot strap configuration file.
 * @author nraveend
 *
 */
public class BingeConfiguration {

    private static final String ZK_ROOT_DIR = "zkrootdir";
    private static final String ZK_SERVERS = "zkservers";
    private static final String ZK_SESSION_TIMEOUT = "zkSessionTimeout";
    private static final String ZK_CONNECT_TIMEOUT = "zkConnectTimeout";
    private static final String MOND_URL = "confUrl";

    private static final String DEFAULT_ZK_SERVERS = "localhost:2181";
    private static final String DEFAULT_ZK_ROOT_DIR = "/";
    private static final String HOSTNAME = "hostname";
    private static final String MONITORIP = "monitorIp";
    private static final String MONITORPORT = "monitorPort";

    private Properties _configProps;
    private Properties _defaultProps;

    public BingeConfiguration(String confFile) {
        this();
        // set the default properties
        FileInputStream in;
        try {
            in = new FileInputStream(confFile);
            _configProps.load(in);
            in.close();
        } catch (FileNotFoundException ex) {
            throw new IllegalArgumentException("Could not find the Binge configuration file: " + confFile, ex);
        } catch (IOException ex) {
            throw new IllegalArgumentException(
                    "Caught IOException while loading Binge configuration file: " + confFile, ex);
        }
    }

    public BingeConfiguration() {
        _defaultProps = new Properties();
        // binge related
        _defaultProps.setProperty(ZK_SERVERS, DEFAULT_ZK_SERVERS);
        _defaultProps.setProperty(ZK_ROOT_DIR, DEFAULT_ZK_ROOT_DIR);
        _defaultProps.setProperty(ZK_SESSION_TIMEOUT, "5");
        _defaultProps.setProperty(ZK_CONNECT_TIMEOUT, "5");
        _defaultProps.setProperty(MOND_URL, "localhost:15381");
        _defaultProps.setProperty(HOSTNAME, System.getProperty(HOSTNAME, "localhost"));
        _defaultProps.setProperty(MONITORIP, System.getProperty(MONITORIP, "127.0.0.1"));
        _defaultProps.setProperty(MONITORPORT, System.getProperty(MONITORPORT, "15387"));
        _configProps = new Properties(_defaultProps);
    }

    public String getConfigurationURL() {
        String url = getStringValue(MOND_URL);
        if (!url.endsWith("/"))
            url = url + "/";
        return url;
    }

    public String getZooKeeperConStr() {
        return getStringValue(ZK_SERVERS);
    }

    public String getZKRootPath() {
        return getStringValue(ZK_ROOT_DIR);
    }

    /**
     * zookeeper session timeout in seconds
     */
    public int getZKSessionTimeout() {
        return Integer.parseInt(getStringValue(ZK_SESSION_TIMEOUT));
    }

    /**
     * zookeeper connection timeout in seconds
     */
    public int getZKConnectTimeout() {
        return Integer.parseInt(getStringValue(ZK_CONNECT_TIMEOUT));
    }

    private String getStringValue(String name) {
        String t = _configProps.getProperty(name);
        if (t == null) {
            t = _defaultProps.getProperty(name);
        }
        return t;
    }

    public String getHostname() {
        return getStringValue(HOSTNAME);
    }

    public String getMonitoringIP() {
        return getStringValue(MONITORIP);
    }

    public short getMonitoringPort() {
        return Short.parseShort(getStringValue(MONITORPORT));
    }

    /**
     * 
     * @return working directory of this node. default value is "<INSTALL_HOME>/work/<nodename>"
     */
    public String getWorkingDirectory() {
        return Paths.get(System.getProperty(INSTALL_DIR), WORK_DIR_NAME, System.getProperty(ENV_NODENAME)).toString();
    }
}
