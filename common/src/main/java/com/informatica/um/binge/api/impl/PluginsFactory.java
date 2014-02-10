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
package com.informatica.um.binge.api.impl;

import static com.informatica.um.binge.BingeConstants.*;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.informatica.um.binge.BingeConfiguration;
import com.informatica.um.binge.BingeUtils;
import com.informatica.vds.api.*;

/**
 * @author nraveend
 * All the get methods are synchronized to ensure that only one copy of plugin is downloaded. Since the get calls
 * are made only once while the source/target/transform thread starts the synchronized overhead is negligible.
 * All the standard plugins are extracted to location $INSTALL/plugins/infa/$NODENAME .
 * All the custom plugins are extracted to location $INSTALL/plugins/custom/$NODENAME
 *
 */
public class PluginsFactory {

    private static final Logger LOG = LoggerFactory.getLogger(PluginsFactory.class);
    private static final String CONF_URL_PATH = "configuration/binge?type=%d&subtype=%d&data=%s";
    private static final String ZIP = "zip";

    private final File plugins;
    private final File nativeLibs;

    private final BingeConfiguration config;
    private Map<Plugin, Class> pluginClasses = new HashMap<Plugin, Class>(10);

    // JSON keys
    public static final String J_NAME = "name";
    public static final String PLUGINJAR = "pluginJar";
    public static final String PLUGINCLASS = "pluginClass";
    public static final String VERSION = "version";

    /**
     * 
     * @param bootConf
     * @param nodeName - Name of the node
     */
    public PluginsFactory(BingeConfiguration bootConf, String nodeName) {
        this.config = bootConf;
        this.plugins = Paths.get(config.getWorkingDirectory(), PLUGINS_PATH).toFile();
        this.nativeLibs = Paths.get(config.getWorkingDirectory(), NATIVE).toFile();
    }

    /**
     * Get the source object
     * @param subType - SubType of the source
     * @param runTimeConfig - run time configuration which tells main class name, version and jar file name.
     * @return
     * @throws Exception
     */
    public synchronized VDSSource getSource(final int subType, final JSONObject runTimeConfig) throws Exception {
        VDSSource source = null;
        Plugin plugin = new Plugin(runTimeConfig, subType);
        if (!pluginClasses.containsKey(plugin)) {
            LOG.info("Loading source plugin with configuration {}", runTimeConfig);
            Class bingeSourceClass = loadPlugin(BINGE_DF_SOURCE_ENTITY, plugin);
            source = (VDSSource) bingeSourceClass.newInstance();
            if (source != null) {
                LOG.info("Successfully loaded source {} implementation class {} from jar {}", plugin.pluginName,
                        plugin.pluginClass, plugin.pluginJar);
                pluginClasses.put(plugin, bingeSourceClass);
            }
        } else {
            LOG.info("Creating an instance of class type {} from already downloaded zip", subType);
            source = (VDSSource) getPlugin(plugin);
        }
        return source;
    }

    private Object getPlugin(Plugin plugin) throws InstantiationException, IllegalAccessException {
        return pluginClasses.get(plugin).newInstance();
    }

    /**
     * Get the transform  object
     * @param subType - SubType of the transform
     * @param runTimeConfig - run time configuration which tells main class name, version and jar file name.
     * @return
     * @throws Exception
     */
    public synchronized VDSTransform getTransform(final int subType, final JSONObject runTimeConfig) throws Exception {
        VDSTransform transform = null;
        Plugin plugin = new Plugin(runTimeConfig, subType);
        if (!pluginClasses.containsKey(plugin)) {
            LOG.info("Loading transform plugin with configuration {}", runTimeConfig.toString());
            Class bingeTransformClass = loadPlugin(BINGE_DF_TRANSFORM_ENTITY, plugin);
            transform = (VDSTransform) bingeTransformClass.newInstance();
            if (transform != null) {
                LOG.info("Successfully loaded transform {} implementation class {} from jar {}", plugin.pluginName,
                        plugin.pluginClass, plugin.pluginJar);
                pluginClasses.put(plugin, bingeTransformClass);
            }
        } else {
            LOG.info("Creating an instance of class type {} from already downloaded zip", subType);
            transform = (VDSTransform) getPlugin(plugin);
        }
        return transform;
    }

    /**
     * Get the source object
     * @param subType - SubType of the transform
     * @param runTimeConfig - run time configuration which tells main class name, version and jar file name.
     * @return
     * @throws Exception
     */
    public synchronized VDSTarget getTarget(final int subType, final JSONObject runTimeConfig) throws Exception {
        VDSTarget target = null;
        Plugin plugin = new Plugin(runTimeConfig, subType);
        if (!pluginClasses.containsKey(plugin)) {
            LOG.info("Loading target plugin with configuration {}", runTimeConfig.toString());
            Class bingeTargetClass = loadPlugin(BINGE_DF_TARGET_ENTITY, plugin);
            target = (VDSTarget) bingeTargetClass.newInstance();
            if (target != null) {
                LOG.info("Successfully loaded target {} implementation class {} from jar {}", plugin.pluginName,
                        plugin.pluginClass, plugin.pluginJar);
                pluginClasses.put(plugin, bingeTargetClass);
            }
        } else {
            LOG.info("Creating an instance of class type {} from already downloaded zip", subType);
            target = (VDSTarget) getPlugin(plugin);
        }
        return target;
    }

    private File getPluginFolder(final int subType) {
        File pluginFolder = new File(plugins, Integer.toString(subType));
        pluginFolder.mkdirs();
        return pluginFolder;
    }

    /**
     *  Load the plugin using the run time configuration. 
     * @param type
     * @param subType
     * @param runTimeConfig
     * @return
     * @throws Exception
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     */
    public Class loadPlugin(final int type, final Plugin plugin) throws Exception, MalformedURLException,
            ClassNotFoundException {
        // Build the url string to download the jar file from mond
        String url = config.getConfigurationURL() + String.format(CONF_URL_PATH, type, plugin.type, ZIP);
        File pluginFolder = new File(getPluginFolder(plugin.type), plugin.version);
        Multimap<String, File> deps = null;
        // Use the local copy of jars if available.
        if (pluginFolder.exists() && Arrays.asList(pluginFolder.list()).contains(plugin.pluginJar)) {
            deps = getPluginClassDeps(pluginFolder);
            LOG.info("Plugin folder {} already exists. Not going to download it", pluginFolder);
            // Download it from admind as the plugin jars are not locally available.
        } else {

            File zipFile = new File(getPluginFolder(plugin.type), plugin.version + ".zip");
            if (zipFile.exists()) {
                zipFile.delete();
                LOG.info("Deleted existing zip file {}", zipFile.getAbsolutePath());
            }
            downloadPluginZip(url, zipFile.getAbsolutePath());
            LOG.info("Successfully downloaded zip file {}", zipFile.getAbsolutePath());

            LOG.info("Plugin folder name is {} and pluginName is {}", pluginFolder, plugin.pluginName);
            LOG.debug("Plugin Folder  {}", pluginFolder.getAbsolutePath());
            // Delete the output folder if it already exist.
            if (pluginFolder.exists()) {
                LOG.debug("Going to delete plugin folder  {}", pluginFolder.getAbsolutePath());
                if (FileUtils.deleteQuietly(pluginFolder)) {
                    LOG.info("Successfully deleted  plugin folder  {}", pluginFolder.getAbsolutePath());
                } else
                    LOG.warn("unable to delete directory {}", pluginFolder.getAbsolutePath());
            }
            deps = extractZipFile(zipFile.getAbsolutePath(), pluginFolder.getAbsolutePath());
            LOG.info("Successfully extracted  zip file to folder {}", pluginFolder.getAbsolutePath());
        }
        return loadPluginClass(plugin.pluginClass, deps, pluginFolder);
    }

    /**
     * Load the plugin class and return the native and java dependencies for the plugin
     * @param pluginClass
     * @param deps
     * @return
     * @throws MalformedURLException
     * @throws ClassNotFoundException
     */
    private Class loadPluginClass(String pluginClass, Multimap<String, File> deps, File pluginFolder)
            throws MalformedURLException, ClassNotFoundException {
        URL[] jars = new URL[deps.get(LIB).size()];
        int i = 0;
        for (File file : deps.get(LIB)) {
            LOG.info("Loading jar file {}", file.getAbsolutePath());
            jars[i++] = file.toURI().toURL();
        }
        // Create URL class loader and load the class file
        return new URLClassLoader(jars).loadClass(pluginClass);
    }

    private InputStream getInputStream(String urlstr) throws VDSException {
        HttpURLConnection con = null;
        URL url;
        try {
            url = new URL(urlstr);
            con = (HttpURLConnection) url.openConnection();
            con.setDoOutput(true);
            con.setDoInput(true);
            con.setRequestMethod("GET");
            con.setUseCaches(false);
            con.connect();
            return con.getInputStream();
        } catch (MalformedURLException e) {
            throw new VDSException(VDSErrorCode.MALFORMED_ADMIND_URL, e, urlstr);
        } catch (IOException e) {
            throw new VDSException(VDSErrorCode.ADMIND_CONNECTION_ERROR, e, urlstr);
        }
    }

    /**
     * Download the plugin zip file from mond and save it locally.
     * @param url
     * @param fileName
     * @throws Exception
     */
    private void downloadPluginZip(String url, String fileName) throws Exception {
        BufferedInputStream in = null;
        BufferedOutputStream out = null;
        try {
            in = new BufferedInputStream(getInputStream(url));
            out = new BufferedOutputStream(new FileOutputStream(new File(fileName)));
            byte[] buffer = new byte[8192];
            int len;
            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        } catch (Exception e) {
            BingeUtils.rethrowVDSException(e);
            throw new VDSException(VDSErrorCode.PLUGIN_DOWNLOAD_FAILED, e, url);
        } finally {
            BingeUtils.close(LOG, url, in);
            BingeUtils.close(LOG, fileName, out);
        }
    }

    /**
     *  Extract the zip file into the output folder
     * @param fileName - Name of the zip file
     * @param outFolderName - output folder name.
     * @throws Exception
     */
    protected Multimap<String, File> extractZipFile(String fileName, String outFolderName) throws Exception {
        LOG.debug("Going to extract zip file {} to folder {}", fileName, outFolderName);
        Multimap<String, File> deps = HashMultimap.create(2, 5);
        File outFolder = new File(outFolderName);
        ZipInputStream zipInput = new ZipInputStream(new FileInputStream(new File(fileName)));
        ZipEntry ze = null;
        boolean status = outFolder.mkdirs();
        if (status)
            LOG.info("Successfully created  plugin folder  {}", outFolder.getAbsolutePath());
        else {
            throw new VDSException(VDSErrorCode.PLUGIN_FOLDER_CREATE_ERROR, outFolder.getAbsolutePath());
        }

        byte[] buffer = new byte[1024];
        while ((ze = zipInput.getNextEntry()) != null) {
            String name = ze.getName();
            // process files only
            if (ze.isDirectory() == false) {
                File file = new File(name);
                File newFile = null;
                if (file.getParentFile() != null && file.getParentFile().getName().equals(NATIVE)) {
                    newFile = new File(nativeLibs, file.getName());
                } else if (file.getParentFile() != null && file.getParentFile().getName().equals(LIB)) {
                    newFile = Paths.get(outFolderName, LIB, file.getName()).toFile();
                    deps.put(LIB, newFile);
                } else {
                    newFile = new File(outFolderName, file.getName());
                    if (name.endsWith(".jar"))
                        deps.put(LIB, newFile);
                }
                newFile.getParentFile().mkdirs();
                FileOutputStream fos = new FileOutputStream(newFile);

                int len;
                while ((len = zipInput.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }

                fos.close();
            }
        }
        return deps;
    }

    /**
     * Get all the dependent jars for the plugin
     * @param pluginFolder - plugin folder
     * @return
     * @throws Exception
     */
    protected Multimap<String, File> getPluginClassDeps(File pluginFolder) throws Exception {
        Multimap<String, File> deps = HashMultimap.create(2, 5);
        for (File file : pluginFolder.listFiles()) {
            if (file.isDirectory() && file.getName().equals(LIB)) {
                File[] subFiles = file.listFiles(new FilenameFilter() {

                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(".jar");
                    }
                });
                for (File subfile : subFiles) {
                    deps.put(LIB, subfile);
                }
            } else if (file.getName().endsWith(".jar")) {
                deps.put(LIB, file);
            }
        }
        return deps;
    }
}

class Plugin {

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + type;
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Plugin other = (Plugin) obj;
        if (type != other.type)
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }

    String pluginName;
    String pluginJar;
    String pluginClass;
    String version;
    int type;

    Plugin(JSONObject runTimeConfig, int type) throws JSONException {
        this.pluginName = runTimeConfig.getString(PluginsFactory.J_NAME);
        this.pluginJar = runTimeConfig.getString(PluginsFactory.PLUGINJAR);
        this.pluginClass = runTimeConfig.getString(PluginsFactory.PLUGINCLASS);
        this.version = runTimeConfig.getString(PluginsFactory.VERSION);
        this.type = type;
    }
}
