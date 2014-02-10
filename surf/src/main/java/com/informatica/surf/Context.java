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
package com.informatica.surf;

import com.informatica.vds.api.VDSConfiguration;
import java.util.HashMap;
import java.util.Properties;

/**
 *
 * @author jraj
 */
public class Context implements VDSConfiguration{

    private HashMap<String, String> _context = new HashMap<>();
    @Override
    public boolean contains(String key) {
        return _context.containsKey(key);
    }

    @Override
    public boolean getBoolean(String key) throws Exception {
        try{
            return Boolean.parseBoolean(_context.get(key));
        }
        catch(Exception ex){
            return false;
        }
    }

    @Override
    public int getInt(String key) throws Exception {
        return Integer.parseInt(_context.get(key));
    }

    @Override
    public long getLong(String key) throws Exception {
        return Long.parseLong(_context.get(key));
    }

    @Override
    public double getDouble(String key) throws Exception {
        return Double.parseDouble(_context.get(key));
    }

    @Override
    public String getString(String key) throws Exception {
        return _context.get(key);
    }

    @Override
    public boolean optBoolean(String key, boolean defaultValue) {
        try{
            return Boolean.parseBoolean(_context.get(key));
        }
        catch(Exception ex){
            return defaultValue;
        }
    }

    @Override
    public int optInt(String key, int defaultValue) {
        try{
            return Integer.parseInt(_context.get(key));
        }
        catch(Exception ex){
            return defaultValue;
        }
    }

    @Override
    public long optLong(String key, long defaultValue) {
        try{
            return Long.parseLong(_context.get(key));
        }
        catch(Exception ex){
            return defaultValue;
        }
    }

    @Override
    public double optDouble(String key, double defaultValue) {
        try{
            return Double.parseDouble(_context.get(key));
        }
        catch(Exception ex){
            return defaultValue;
        }
    }

    @Override
    public String optString(String key, String defaultValue) {
        if(contains(key)){
            return _context.get(key);
        }
        else{
            return defaultValue;
        }
    }
    
    public void set(String key, String value){
        _context.put(key, value);
    }

    void setFromProperties(Properties props) {
        for(String key: props.stringPropertyNames()){
            _context.put(key, props.getProperty(key));
        }
    }
}
