package org.dsa.iot.mqtt;

import org.dsa.iot.commons.GuaranteedReceiver;
import org.dsa.iot.dslink.link.Linkable;
import org.dsa.iot.dslink.node.*;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValuePair;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.mqtt.utils.ClientReceiver;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * @author Samuel Grenier
 */
public class Mqtt implements MqttCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(Mqtt.class);
    private final Node parent;
    private Node status;
    private Node subs;
    private Node qos;

    private final Object dataLock = new Object();
    private Node data;

    private GuaranteedReceiver<MqttClient> clientReceiver;
    private final Object receiverLock = new Object();

    public Mqtt(Node parent) {
        this.parent = parent;
        parent.setMetaData(this);
    }

    public void init() {
        synchronized (receiverLock) {
            clientReceiver = new ClientReceiver(parent, this);
        }

        NodeBuilder child = parent.createChild("delete");
        child.setDisplayName("Delete");
        child.setAction(Actions.getRemoveServerAction(this, parent));
        child.setSerializable(false);
        child.build();

        child = parent.createChild("publish");
        child.setDisplayName("Publish");
        child.setAction(Actions.getPublishAction(this));
        child.setSerializable(false);
        child.build();

        child = parent.createChild("data");
        child.setDisplayName("Data");
        child.setSerializable(false);
        child.setRoConfig("preserve", new Value(true));
        synchronized (dataLock) {
            data = child.build();
        }

        child = parent.createChild("subscriptions");
        child.setDisplayName("Subscriptions");
        subs = child.build();

        child = subs.createChild("subscribe");
        child.setDisplayName("Subscribe");
        child.setAction(Actions.getSubscribeAction(this));
        child.setSerializable(false);
        child.build();

        child = parent.createChild("qos");
        child.setDisplayName("QoS");
        child.setValueType(ValueType.makeEnum("0", "1", "2"));
        child.setValue(new Value("0"));
        child.setWritable(Writable.WRITE);
        child.getListener().setValueHandler(new Handler<ValuePair>() {
            @Override
            public void handle(ValuePair event) {
                int qos = getQos();
                LOGGER.debug("Setting up MQTT server with new QoS of {}", qos);
                disconnect();
                destroyEverything(data);
                synchronized (receiverLock) {
                    clientReceiver = new ClientReceiver(parent, Mqtt.this);
                }
                restoreSubscriptions();
            }
        });
        qos = child.build();

        child = parent.createChild("status");
        child.setDisplayName("Status");
        child.setSerializable(false);
        child.setValueType(ValueType.makeBool("Connected", "Disconnected"));
        child.setValue(new Value(false));
        status = child.build();
    }

    public void setStatus(boolean connected) {
        if (status != null) {
            status.setValue(new Value(connected));
        }
    }

    protected int getQos() {
        return Integer.parseInt(qos.getValue().getString());
    }

    protected void get(Handler<MqttClient> onClientReceived) {
        clientReceiver.get(onClientReceived, false);
    }

    protected void disconnect() {
        setStatus(false);
        synchronized (receiverLock) {
            MqttClient client = clientReceiver.shutdown();
            try {
                if (client != null) {
                    client.setCallback(null);
                    client.close();
                }
            } catch (Exception ignored) {
            }
            clientReceiver = null;
        }
    }

    protected void restoreSubscriptions() {
        Map<String, Node> children = subs.getChildren();
        if (children != null) {
            for (Map.Entry<String, Node> entry : children.entrySet()) {
                String name = entry.getKey();
                Node child = entry.getValue();
                if (child.getAction() != null) {
                    continue;
                }
                LOGGER.info("Restoring subscription for '{}'", name);

                String topic = child.getValue().getString();
                subscribe(name, topic);
            }
        }

        synchronized (dataLock) {
            recursiveResubscribe(data.getChildren());
        }
    }

    private void recursiveResubscribe(Map<String, Node> children) {
        if (children == null) {
            return;
        }
        final int qos = getQos();
        for (final Node node : children.values()) {
            if (hasSub(node)) {
                get(new Handler<MqttClient>() {
                    @Override
                    public void handle(MqttClient event) {
                        String fullTopic = node.getPath();
                        int length = data.getPath().length() + 1;
                        fullTopic = fullTopic.substring(length);
                        fullTopic = StringUtils.decodeName(fullTopic);
                        LOGGER.info("Restoring subscription for '{}'", fullTopic);
                        try {
                            event.subscribe(fullTopic, qos);
                        } catch (MqttException e) {
                            throw new RuntimeException(e);
                        }
                    }
                });
            }
            recursiveResubscribe(node.getChildren());
        }
    }

    public void publish(final String topic,
                        String value,
                        boolean retained) {
        try {
            byte[] payload = value.getBytes("UTF-8");
            final MqttMessage msg = new MqttMessage();
            msg.setPayload(payload);
            msg.setQos(getQos());
            msg.setRetained(retained);
            get(new Handler<MqttClient>() {
                @Override
                public void handle(MqttClient event) {
                    try {
                        event.publish(topic, msg);
                    } catch (MqttException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public void subscribe(final String name,
                          final String topic) {
        if (subs.getChild(name) == null) {
            NodeBuilder builder = subs.createChild(name);
            builder.setValueType(ValueType.STRING);
            builder.setValue(new Value(topic));
            Node node = builder.build();

            builder = node.createChild("unsubscribe");
            builder.setDisplayName("Unsubscribe");
            builder.setSerializable(false);
            Mqtt mqtt = Mqtt.this;
            Action act = Actions.getUnsubscribeAction(mqtt, name);
            builder.setAction(act);
            builder.build();
        }
        get(new Handler<MqttClient>() {
            @Override
            public void handle(MqttClient event) {
                try {
                    event.subscribe(topic, getQos());
                } catch (MqttException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public void unsubscribe(String name) {
        final String topic;
        Node child = subs.removeChild(name);
        if (child == null) {
            return;
        }
        topic = child.getValue().getString();

        get(new Handler<MqttClient>() {
            @Override
            public void handle(MqttClient event) {
                try {
                    event.unsubscribe(topic);
                } catch (MqttException e) {
                    throw new RuntimeException(e);
                } finally {
                    synchronized (dataLock) {
                        destroyTree(topic, data);
                    }
                }
            }
        });
    }

    @Override
    public void connectionLost(Throwable throwable) {
        LOGGER.error("Lost connection to MQTT", throwable);
        restoreSubscriptions();
    }

    @Override
    public void messageArrived(final String s,
                                            final MqttMessage msg)
                                                    throws Exception {
        if (s.contains("//")) {
            return;
        }
        final String[] split = NodeManager.splitPath(s);
        if (split.length <= 0) {
            return;
        }

        String name = split[0];
        NodeBuilder b = data.createChild(name);
        b.setSerializable(false);
        Node node = b.build();
        node.setSerializable(false);
        for (int i = 1; i < split.length; i++) {
            name = split[i];
            b = node.createChild(name);
            b.setSerializable(false);
            node = b.build();
        }
        node.setValueType(ValueType.STRING);
        node.setValue(new Value(msg.toString()));
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Updating '{}' with '{}'", node.getPath(), msg);
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
    }

    public void destroyTree(String topic, Node node) {
        if ("#".equals(topic) || "+".equals(topic)) {
            destroyEverything(node);
        } else {
            String[] split = NodeManager.splitPath(topic);
            for (int i = 0; i < split.length; i++) {
                topic = split[i];
                if ("#".equals(topic)) {
                    destroyEverything(node);
                } else if ("+".equals(topic)) {
                    destroyWildCardTree(node, split, i);
                    break;
                } else if (i + 1 >= split.length) {
                    destroyIndividualNode(topic, node);
                    break;
                } else {
                    node = node.getChild(topic);
                    if (node == null) {
                        break;
                    }
                }
            }
        }
    }

    private void destroyEverything(Node node) {
        if (hasSub(node)) {
            destroyIndividualNode(node.getName(), node.getParent());
            return;
        }
        Map<String, Node> children = node.getChildren();
        if (children != null) {
            for (Node n : children.values()) {
                if (!hasSub(n)) {
                    destroyEverything(n);
                }
            }
        }
        Value value = node.getRoConfig("preserve");
        if (!(hasSub(node) || (value != null && value.getBool()))
                && (children == null || children.size() <= 0)) {
            node.getParent().removeChild(node);
        }
    }

    private void destroyWildCardTree(Node node, String[] split, int i) {
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
    }

    private void destroyIndividualNode(String topic, Node node) {
        final Node tmp = node.getChild(topic);
        if (!hasSub(tmp)) {
            node.removeChild(topic);
            return;
        }

        get(new Handler<MqttClient>() {
            @Override
            public void handle(MqttClient event) {
                String fullTopic = tmp.getPath();
                int length = data.getPath().length() + 1;
                fullTopic = fullTopic.substring(length);
                fullTopic = StringUtils.decodeName(fullTopic);
                try {
                    event.subscribe(fullTopic, getQos());
                } catch (MqttException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private boolean hasSub(Node node) {
        Linkable link = node.getLink();
        SubscriptionManager sm = null;
        if (link != null) {
            sm = link.getSubscriptionManager();
        }
        return sm != null && sm.hasValueSub(node);
    }

    public static void init(Node superRoot) {
        {
            NodeBuilder child = superRoot.createChild("addServer");
            child.setAction(Actions.getAddServerAction(superRoot));
            child.setSerializable(false);
            child.setDisplayName("Add Server");
            child.build();
        }

        {
            Map<String, Node> rootChildren = superRoot.getChildren();
            ScheduledThreadPoolExecutor stpe = Objects.getDaemonThreadPool();
            for (final Node child : rootChildren.values()) {
                if (child.getAction() != null) {
                    continue;
                }
                stpe.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            Mqtt mqtt = new Mqtt(child);
                            mqtt.init();

                            Map<String, Node> subs = mqtt.subs.getChildren();
                            if (subs == null) {
                                return;
                            }
                            mqtt.restoreSubscriptions();
                            for (Node node : subs.values()) {
                                String name = node.getName();
                                NodeBuilder b = node.createChild("unsubscribe");
                                b.setSerializable(false);
                                b.setDisplayName("Unsubscribe");
                                Action a = Actions.getUnsubscribeAction(mqtt, name);
                                b.setAction(a);
                                b.build();
                            }
                        } catch (Exception e) {
                            LOGGER.warn("", e);
                        }
                    }
                });
            }
        }
    }
}
