package org.dsa.iot.mqtt;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeManager;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.dslink.util.URLInfo;
import org.dsa.iot.mqtt.utils.InsecureSslSocketFactory;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author Samuel Grenier
 */
public class Mqtt implements MqttCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(Mqtt.class);
    private final Node parent;

    private ScheduledFuture<?> future;
    private MqttClient client;

    public Mqtt(Node parent) throws MqttException {
        this.parent = parent;

        NodeBuilder child = parent.createChild("subscribe");
        child.setAction(Actions.getSubscribeAction(this));
        child.build();
    }

    protected synchronized void connect(boolean checked) {
        if (future != null) {
            future.cancel(false);
            future = null;
        }
        if (client == null) {
            try {
                String url = parent.getRoConfig("url").getString();
                String id = parent.getRoConfig("clientId").getString();
                this.client = new MqttClient(url, id);

                MqttConnectOptions opts = new MqttConnectOptions();

                URLInfo info = URLInfo.parse(url);
                if ("ssl".equals(info.protocol)) {
                    opts.setSocketFactory(new InsecureSslSocketFactory());
                }

                Value vUser = parent.getRoConfig("user");
                if (vUser != null) {
                    opts.setUserName(vUser.getString());
                }

                char[] pass = parent.getPassword();
                if (pass != null) {
                    opts.setPassword(pass);
                }

                client.setCallback(this);
                client.connect(opts);
                LOGGER.info("Opened connection to MQTT at {}", url);
            } catch (MqttException e) {
                if (checked) {
                    throw new RuntimeException(e);
                }

                scheduleReconnect();
            }
        }
    }

    public synchronized void subscribe(String topic, int qos) {
        if (ensureConnected()) {
            try {
                client.subscribe(topic, qos);
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public synchronized void connectionLost(Throwable throwable) {
        LOGGER.error("Lost connection to MQTT", throwable);
        if (future != null) {
            future.cancel(false);
            future = null;
        }
        scheduleReconnect();
    }

    @Override
    public synchronized void messageArrived(String s, MqttMessage msg) throws Exception {
        s = NodeManager.normalizePath(s);
        String[] split = s.split("/");
        if (split.length > 0) {
            String filtered = StringUtils.filterBannedChars(split[0]);
            Node node = parent.createChild(filtered).build();
            node.setSerializable(false);
            for (int i = 1; i < split.length; i++) {
                filtered = StringUtils.filterBannedChars(split[i]);
                node = node.createChild(filtered).build();
            }
            node.setValue(new Value(msg.toString()));
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Updating '{}' with '{}'", node.getPath(), msg);
            }
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
    }

    private synchronized boolean ensureConnected() {
        if (client == null) {
            connect(false);
        }
        return client != null;
    }

    private synchronized void scheduleReconnect() {
        LOGGER.warn("Reconnection to MQTT server scheduled");
        client = null;
        future = Objects.getDaemonThreadPool().schedule(new Runnable() {
            @Override
            public void run() {
                connect(false);
            }
        }, 5, TimeUnit.SECONDS);
    }

    public static void init(Node superRoot) {
        {
            NodeBuilder child = superRoot.createChild("addServer");
            child.setAction(Actions.getAddServerAction(superRoot));
            child.build();
        }

        {
            Map<String, Node> children = superRoot.getChildren();
            if (children != null) {
                for (Node child : children.values()) {
                    if (child.getAction() == null) {
                        try {
                            Mqtt mqtt = new Mqtt(child);
                            mqtt.connect(false);
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
        }
    }
}
