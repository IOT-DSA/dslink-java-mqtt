package org.dsa.iot.mqtt;

import io.moquette.BrokerConstants;
import io.moquette.server.Server;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.MemoryConfig;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Writable;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValuePair;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.handler.Handler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * @author Samuel Grenier
 */
public class Broker {

    private static final Logger LOGGER = LoggerFactory.getLogger(Mqtt.class);

    private Server server;

    private Broker() {
    }

    public synchronized void start() {
        stop();

        Properties props = new Properties();
        props.put(BrokerConstants.WEB_SOCKET_PORT_PROPERTY_NAME, "8000");
        final IConfig conf = new MemoryConfig(props);
        server = new Server();
        try {
            server.startServer(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void stop() {
        if (server != null) {
            server.stopServer();
            server = null;
        }
    }

    public static Broker init(Node root) {
        final Broker broker = new Broker();

        Node n = root.getChild("brokerEnabled");
        if ( n == null ) {
            NodeBuilder b = root.createChild("brokerEnabled");
            b.setDisplayName("Broker Enabled");
            b.setValueType(ValueType.BOOL);
            b.setWritable(Writable.CONFIG);
            b.setValue(new Value(false));
            n = b.build();
        }

        n.getListener().setValueHandler(new Handler<ValuePair>() {
                @Override
                public void handle(ValuePair event) {
                    Value v = event.getCurrent();
                    if (v.getBool()) {
                        broker.start();
                    } else {
                        broker.stop();
                    }
                }
        });

        if (n.getValue().getBool()) {
            LOGGER.info("Restoring broker" );
            broker.start();
        }
        return broker;
    }
}
