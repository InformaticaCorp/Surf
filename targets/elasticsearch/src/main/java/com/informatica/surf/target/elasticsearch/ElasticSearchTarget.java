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
package com.informatica.surf.target.elasticsearch;

import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSTarget;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by jerry on 11/03/14.
 */
public class ElasticSearchTarget implements VDSTarget {
    private Client _client;
    private Node _node;
    private String _indexName;
    private String _typeName;
    private static final Logger _logger = LoggerFactory.getLogger(ElasticSearchTarget.class);
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        _logger.info("Initializing ElasticSearch target");
        _indexName = ctx.getString("index-name");
        _typeName = ctx.getString("type-name");
        String cluster = ctx.optString("cluster-name", "elasticsearch");
        _node = nodeBuilder()
                  .client(true)
                  .clusterName(cluster)
                  .node();
        _client = _node.client();
        _logger.info("ElasticSearch target initialized");
    }

    @Override
    public void write(VDSEvent strm) throws Exception {

        _client.prepareIndex(_indexName, _typeName)
                .setSource(strm.getBuffer().array(), 0, strm.getBufferLen())
                .execute();
        _logger.debug("Wrote data to ElasticSearch index");
    }

    @Override
    public void close() throws IOException {
        _client.close();
        _node.close();
        _logger.debug("Shutdown ElasticSearch target");
    }
}
