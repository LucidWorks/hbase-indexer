/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.util.solr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.ngdata.hbaseindexer.util.MavenUtil;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.zookeeper.KeeperException;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

/**
 * Helps booting up SolrCloud in test code.
 *
 * <p>It basically starts up an empty Solr (no cores, collections or configs).
 * A config can be uploaded using {@link #uploadConfig(String, byte[], byte[])} and a core/collection can be
 * added through {@link #createCore(String, String, String, int)}.</p>
 *
 * <p>The Solr ZooKeeper is chroot'ed to "/solr".</p>
 */
public class SolrTestingUtility {
    private final int solrPort;
    private Server server;
    private String solrWarPath;
    private File tmpDir;
    private File solrHomeDir;
    private int zkClientPort;
    private String zkConnectString;
    private Map<String,String> configProperties;

    public SolrTestingUtility(int zkClientPort, int solrPort) throws IOException {
        this(zkClientPort, solrPort, ImmutableMap.<String,String>of());
    }
    
    public SolrTestingUtility(int zkClientPort, int solrPort, Map<String,String> configProperties) throws IOException {
        this.zkClientPort = zkClientPort;
        this.zkConnectString = "localhost:" + zkClientPort + "/solr";
        this.solrPort = solrPort;
        this.configProperties = configProperties;
    }

    public void start() throws Exception {
        // Make the Solr home directory
        this.tmpDir = Files.createTempDir();
        this.solrHomeDir = new File(tmpDir, "home");
        if (!this.solrHomeDir.mkdir()) {
            throw new RuntimeException("Failed to create directory " + this.solrHomeDir.getAbsolutePath());
        }
        writeCoresConf();

        // Set required system properties
        System.setProperty("solr.solr.home", solrHomeDir.getAbsolutePath());
        System.setProperty("zkHost", zkConnectString);
        
        for (Entry<String, String> entry : configProperties.entrySet()) {
            System.setProperty(entry.getKey().toString(), entry.getValue());
        }

        // Determine location of Solr war file. The Solr war is a dependency of this project, so we should
        // be able to find it in the local maven repository.
        String solrVersion = getSolrVersion();
        solrWarPath = MavenUtil.findLocalMavenRepository().getAbsolutePath() +
                "/org/apache/solr/solr/" + solrVersion + "/solr-" + solrVersion + ".war";

        if (!new File(solrWarPath).exists()) {
            throw new Exception("Solr war not found at " + solrWarPath);
        }

        server = createServer();
        server.start();
    }

    public String getSolrVersion() throws IOException {
       
        Properties properties = new Properties();
        // solr.properties is created by a plugin in the pom.xml
        InputStream is = getClass().getResourceAsStream("solr.properties");
        if (is != null) {
            properties.load(is);
            is.close();
            String solrVersion = properties.getProperty("solr.version");
            return solrVersion;
        } else {
            return SolrClient.class.getPackage().getSpecificationVersion();
        }
    }

    public File getSolrHomeDir() {
        return solrHomeDir;
    }

    public String getZkConnectString() {
        return zkConnectString;
    }

    private void writeCoresConf() throws FileNotFoundException {
        File coresFile = new File(solrHomeDir, "solr.xml");
        PrintWriter writer = new PrintWriter(coresFile);
        writer.println("<solr persistent='true'>");
        writer.println(" <cores adminPath='/admin/cores' host='localhost' hostPort='" + solrPort + "'>");
        writer.println(" </cores>");
        writer.println("</solr>");
        writer.close();
    }

    private Server createServer() throws Exception{
        // create path on zookeeper for solr cloud
        ZooKeeperItf zk = ZkUtil.connect("localhost:" + zkClientPort, 10000);
        ZkUtil.createPath(zk, "/solr");
        zk.close();

        Server server = new Server(solrPort);
        WebAppContext ctx = new WebAppContext(solrWarPath, "/solr");
        // The reason to change the classloading behavior was primarily so that the logging libraries would
        // be inherited, and hence that Solr would use the same logging system & conf.
        ctx.setParentLoaderPriority(true);
        server.addHandler(ctx);
        return server;
    }

    public Server getServer() {
        return server;
    }

    public void stop() throws Exception {
        if (server != null) {
            server.stop();
        }

        if (tmpDir != null) {
            FileUtils.deleteDirectory(tmpDir);
        }

        System.getProperties().remove("solr.solr.home");
        System.getProperties().remove("zkHost");
        for (String configPropertyKey : configProperties.keySet()) {
            System.getProperties().remove(configPropertyKey);
        }
    }


    /**
     * Utility method to upload a Solr config into ZooKeeper. This method only allows to supply schema and
     * solrconf, if you want to upload a full directory use {@link #uploadConfig(String, File)}.
     */
    public void uploadConfig(String confName, byte[] schema, byte[] solrconf)
            throws InterruptedException, IOException, KeeperException {
        // Write schema & solrconf to temporary dir, upload dir, delete tmp dir
        File tmpConfDir = Files.createTempDir();
        Files.copy(ByteStreams.newInputStreamSupplier(schema), new File(tmpConfDir, "schema.xml"));
        Files.copy(ByteStreams.newInputStreamSupplier(solrconf), new File(tmpConfDir, "solrconfig.xml"));
        uploadConfig(confName, tmpConfDir);
        FileUtils.deleteDirectory(tmpConfDir);
    }

    /**
     * Utility method to upload a Solr config into ZooKeeper. If you don't have the config in the form of
     * a filesystem directory, you might want to use {@link #uploadConfig(String, byte[], byte[])}.
     */
    public void uploadConfig(String confName, File confDir) throws InterruptedException, IOException, KeeperException {
        SolrZkClient zkClient = new SolrZkClient(zkConnectString, 30000, 30000,
                new OnReconnect() {
                    @Override
                    public void command() {}
                });
        ZkConfigManager configManager = new ZkConfigManager(zkClient);
        configManager.uploadConfigDir(confDir.toPath(), confName);
        zkClient.close();
    }

    /**
     * Creates a new core, associated with a collection, in Solr.
     */
    public void createCore(String coreName, String collectionName, String configName, int numShards) throws IOException {
        createCore(coreName, collectionName, configName, numShards, null);
    }
    
    /**
     * Creates a new core, associated with a collection, in Solr.
     */
    public void createCore(String coreName, String collectionName, String configName, int numShards, String dataDir) throws IOException {
        String url = "http://localhost:" + solrPort + "/solr/admin/cores?action=CREATE&name=" + coreName
                + "&collection=" + collectionName + "&configName=" + configName + "&numShards=" + numShards;
        
        if (dataDir != null) {
            url += "&dataDir=" + dataDir;
        }
        
        URL coreActionURL = new URL(url);
        HttpURLConnection conn = (HttpURLConnection)coreActionURL.openConnection();
        conn.connect();
        int response = conn.getResponseCode();
        conn.disconnect();
        if (response != 200) {
            throw new RuntimeException("Request to " + url + ": expected status 200 but got: " + response + ": "
                    + conn.getResponseMessage());
        }
    }

    /**
     * Create a Solr collection with a given number of shards.
     * 
     * @param collectionName name of the collection to be created
     * @param configName name of the config for the collection
     * @param numShards number of shards in the collection
     */
    public void createCollection(String collectionName, String configName, int numShards) throws IOException {
        for (int shardIndex = 0; shardIndex < numShards; shardIndex++) {
            String coreName = String.format("%s_shard%d", collectionName, shardIndex + 1);
            createCore(coreName, collectionName, configName, numShards, coreName + "_data");
        }
    }

}
