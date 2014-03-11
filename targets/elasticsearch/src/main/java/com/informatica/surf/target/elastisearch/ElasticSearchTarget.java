package com.informatica.surf.target.elastisearch;

import com.informatica.vds.api.VDSConfiguration;
import com.informatica.vds.api.VDSEvent;
import com.informatica.vds.api.VDSTarget;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

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
    @Override
    public void open(VDSConfiguration ctx) throws Exception {
        _indexName = ctx.getString("index-name");
        _typeName = ctx.getString("type-name");
        String cluster = ctx.getString("cluster-name");
        _node = nodeBuilder()
                  .client(true)
                  .clusterName(cluster)
                  .node();
        _client = _node.client();
    }

    @Override
    public void write(VDSEvent strm) throws Exception {
        _client.prepareIndex(_indexName, _typeName).setSource(strm.getBuffer().array());
    }

    @Override
    public void close() throws IOException {
        _client.close();
        _node.close();
    }
}
