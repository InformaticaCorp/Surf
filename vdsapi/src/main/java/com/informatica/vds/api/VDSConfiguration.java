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
 * VDSConfiguration is a key-value store used to retrieve the configuration of
 * VDS entities(sources, targets and transformations).
 *
 */
public interface VDSConfiguration {

    /**
     * 
     * @param key
     * @return true if given key is configured, false otherwise
     */
    public boolean contains(String key);

    /**
     * Retrieves value of the key from configuration as a boolean.
     * @param key
     * @return boolean value
     * @throws Exception when key not found or value is not a valid boolean.
     */
    public boolean getBoolean(String key) throws Exception;

    /**
     * Retrieves value of the key from configuration as a integer.
     * @param key
     * @return integer value
     * @throws Exception when key not found or value is not a valid integer.
     */
    public int getInt(String key) throws Exception;

    /**
     * Retrieves value of the key from configuration as a long.
     * @param key
     * @return long value
     * @throws Exception when key not found or value is not a valid long.
     */
    public long getLong(String key) throws Exception;

    /**
     * Retrieves value of the key from configuration as a double.
     * @param key
     * @return double value
     * @throws Exception when key not found or value is not a valid double.
     */
    public double getDouble(String key) throws Exception;

    /**
     * Retrieves value of the key from configuration as a string.
     * @param key
     * @return string value
     * @throws Exception when key not found or value is not a valid string.
     */
    public String getString(String key) throws Exception;

    /**
     * Retrieves value of the key from configuration as a boolean. Returns default value in case if key not found or not valid. 
     * @param key
     * @param defaultValue
     * @return if key exists and valid, returns value as a boolean. Otherwise, defaultValue is returned
     */
    public boolean optBoolean(String key, boolean defaultValue);

    /**
     * Retrieves value of the key from configuration as a integer. Returns default value in case if key not found or not valid. 
     * @param key
     * @param defaultValue
     * @return if key exists and valid, returns value as a integer. Otherwise, defaultValue is returned
     */
    public int optInt(String key, int defaultValue);

    /**
     * Retrieves value of the key from configuration as a long. Returns default value in case if key not found or not valid. 
     * @param key
     * @param defaultValue
     * @return if key exists and valid, returns value as a long. Otherwise, defaultValue is returned
     */
    public long optLong(String key, long defaultValue);

    /**
     * Retrieves value of the key from configuration as a double. Returns default value in case if key not found or not valid. 
     * @param key
     * @param defaultValue
     * @return if key exists and valid, returns value as a double. Otherwise, defaultValue is returned
     */
    public double optDouble(String key, double defaultValue);

    /**
     * Retrieves value of the key from configuration as a string. Returns default value in case if key not found or not valid. 
     * @param key
     * @param defaultValue
     * @return if key exists and valid, returns value as a string. Otherwise, defaultValue is returned
     */
    public String optString(String key, String defaultValue);
}
