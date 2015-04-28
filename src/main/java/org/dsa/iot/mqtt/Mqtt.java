package org.dsa.iot.mqtt;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.NodeManager;
import org.dsa.iot.dslink.node.actions.Action;
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
    private final Node data;
    private final Node subs;

    private ScheduledFuture<?> future;
    private MqttClient client;

    public Mqtt(Node parent) throws MqttException {
        this.parent = parent;

        NodeBuilder child = parent.createChild("subscribe");
        child.setAction(Actions.getSubscribeAction(this));
        child.build();

        child = parent.createChild("data");
        data = child.build();

        child = parent.createChild("subscriptions");
        subs = child.build();
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

                Map<String, Node> children = subs.getChildren();
                if (children != null) {
                    for (Map.Entry<String, Node> entry : children.entrySet()) {
                        String name = entry.getKey();
                        Node child = entry.getValue();
                        LOGGER.info("Restoring subscription for '{}'", name);

                        String topic = child.getValue().getString();
                        int qos = child.getRoConfig("qos").getNumber().intValue();
                        subscribe(name, topic, qos);
                    }
                }
            } catch (MqttException e) {
                if (checked) {
                    throw new RuntimeException(e);
                }

                scheduleReconnect();
            }
        }
    }

    public synchronized void subscribe(String name, String topic, int qos) {
        if (ensureConnected()) {
            try {
                client.subscribe(topic, qos);
                if (subs.getChild(name) == null) {
                    NodeBuilder builder = subs.createChild(name);
                    builder.setValue(new Value(topic));
                    builder.setRoConfig("qos", new Value(qos));
                    Node node = builder.build();

                    builder = node.createChild("unsubscribe");
                    Action act = Actions.getUnsubscribeAction(this, name);
                    builder.setAction(act);
                    builder.build();
                }
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void unsubscribe(String name) {
        Node child = subs.removeChild(name);
        if (child != null) {
            String topic = child.getValue().getString();
            try {
                client.unsubscribe(topic);
            } catch (MqttException ignored) {
            }

            destroyTree(topic, data);
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
        String[] split = NodeManager.splitPath(s);
        if (split.length > 0) {
            String filtered = StringUtils.filterBannedChars(split[0]);
            Node node = data.createChild(filtered).build();
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

                            Map<String, Node> subs = mqtt.subs.getChildren();
                            if (subs != null) {
                                for (Node node : subs.values()) {
                                    String name = node.getName();
                                    NodeBuilder b = node.createChild("unsubscribe");
                                    b.setAction(Actions.getUnsubscribeAction(mqtt, name));
                                    b.build();
                                }
                            }
                        } catch (Exception ignored) {
                        }
                    }
                }
            }
        }
    }

    public static void destroyTree(String topic, Node node) {
        if ("#".equals(topic) || "+".equals(topic)) {
            node.clearChildren();
            removeParent(node);
        } else {
            String[] split = NodeManager.splitPath(topic);
            for (int i = 0; i < split.length; i++) {
                topic = split[i];
                if ("#".equals(topic)) {
                    destroyTree(topic, node);
                } else if ("+".equals(topic)) {
                    Map<String, Node> children = node.getChildren();
                    if (children != null) {
                        StringBuilder b = new StringBuilder();
                        for (int x = ++i;;) {
                            b.append(split[x]);
                            if (++x < split.length) {
                                b.append("/");
                            } else {
                                break;
                            }
                        }
                        String built = b.toString();
                        for (Node child : children.values()) {
                            destroyTree(built, child);
                        }
                    }
                    break;
                } else if (i + 1 >= split.length) {
                    String filtered = StringUtils.filterBannedChars(topic);
                    node = node.removeChild(filtered);
                    removeParent(node);
                    break;
                } else {
                    String filtered = StringUtils.filterBannedChars(topic);
                    node = node.getChild(filtered);
                    if (node == null) {
                        break;
                    }
                }
            }
        }
    }

    private static void removeParent(Node child) {
        Node parent = child.getParent();
        if (parent != null && parent.getValue() == null) {
            Map<String, Node> children = parent.getChildren();
            if (children == null || children.size() <= 1) {
                parent.removeChild(child);
            }
        }
    }
}
