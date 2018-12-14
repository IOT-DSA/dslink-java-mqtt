package org.dsa.iot.mqtt;

import org.dsa.iot.commons.GuaranteedReceiver;
import org.dsa.iot.dslink.link.Linkable;
import org.dsa.iot.dslink.node.*;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValuePair;
import org.dsa.iot.dslink.node.value.ValueType;
import org.dsa.iot.dslink.util.Objects;
import org.dsa.iot.dslink.util.StringUtils;
import org.dsa.iot.dslink.util.handler.Handler;
import org.dsa.iot.mqtt.utils.ClientReceiver;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private Node data;

    private GuaranteedReceiver<MqttClient> clientReceiver;
    private final Object receiverLock = new Object();

    public Mqtt(Node parent) {
        this.parent = parent;
        parent.setMetaData(this);
    }

    public void init() {
        synchronized (receiverLock) {
            clientReceiver = new ClientReceiver(this);
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
        data = child.build();

        child = data.createChild("clean");
        child.setSerializable(false);
        child.setDisplayName("Clean");
        child.setRoConfig("preserve", new Value(true));
        child.setAction(new Action(Permission.WRITE,
                        new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                destroyEverything(data);
                restoreSubscriptions();
            }
        }));
        child.build();

        child = parent.createChild("subscriptions");
        child.setDisplayName("Subscriptions");
        subs = child.build();

        child = subs.createChild("subscribe");
        child.setDisplayName("Subscribe");
        child.setAction(Actions.getSubscribeAction(this));
        child.setSerializable(false);
        child.build();

        child = parent.createChild("status");
        child.setDisplayName("Status");
        child.setSerializable(false);
        child.setValueType(ValueType.makeBool("Connected", "Disconnected"));
        child.setValue(new Value(false));
        status = child.build();

        child = parent.createChild("editServer");
        child.setDisplayName("Edit Server");
        child.setSerializable(false);
        child.setAction(Actions.getEditServerAction(this));
        child.build();
    }

    public void edit(String url,
                     String user,
                     char[] pass,
                     String clientId, int qos,
                     String caFile, String certFile, String privKeyFile) {
        if (user == null) {
            parent.removeRoConfig("user");
            parent.setPassword(null);
        } else {
            parent.setRoConfig("user", new Value(user));
            if (pass != null) {
                parent.setPassword(pass);
            }
        }

        parent.setRoConfig("url", new Value(url));
        parent.setRoConfig("clientId", new Value(clientId));
        parent.setRoConfig("qos", new Value(qos));
        if (!(caFile == null || certFile == null || privKeyFile == null)) {
            parent.setRoConfig("ca", new Value(caFile));
            parent.setRoConfig("cert", new Value(certFile));
            parent.setRoConfig("privKey", new Value(privKeyFile));
        } else {
            parent.removeRoConfig("ca");
            parent.removeRoConfig("cert");
            parent.removeRoConfig("privKey");
        }
        disconnect();
        destroyEverything(data);
        synchronized (receiverLock) {
            clientReceiver = new ClientReceiver(this);
        }
        restoreSubscriptions();
    }

    public String getUrl() {
        return parent.getRoConfig("url").getString();
    }

    public String getClientId() {
        return parent.getRoConfig("clientId").getString();
    }

    public String getUsername() {
        Value vUser = parent.getRoConfig("user");
        if (vUser != null) {
            return vUser.getString();
        }
        return null;
    }

    public char[] getPassword() {
        char[] password = parent.getPassword();
        if (password != null) {
            return password;
        }
        return null;
    }

    public String getCa() {
        Value v = parent.getRoConfig("ca");
        return v == null ? null : v.getString();
    }

    public String getCert() {
        Value v = parent.getRoConfig("cert");
        return v == null ? null : v.getString();
    }

    public String getPrivateKey() {
        Value v = parent.getRoConfig("privKey");
        return v == null ? null : v.getString();
    }

    public void setStatus(boolean connected) {
        if (status != null) {
            status.setValue(new Value(connected));
        }
    }

    protected int getQos() {
        return parent.getRoConfig("qos").getNumber().intValue();
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

        recursiveResubscribe(data.getChildren());
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
                    destroyTree(topic, data);
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
    public synchronized void messageArrived(final String s,
                                            final MqttMessage msg) throws Exception {
        if (s.contains("//")) {
            return;
        }
        final String[] split = NodeManager.splitPath(s);
        if (split.length <= 0) {
            return;
        }

        //Create a zero node if there is a leading slash
        NodeBuilder b;
        int offset;
        if (s.startsWith("/")) {
            b = data.createChild("%2f");
            offset = 0;
        } else {
            b = data.createChild(split[0]);
            offset = 1;
        }

        b.setSerializable(false);
        b.build();

        for (int i = offset; i < split.length; i++) {
            String name = split[i];
            b = b.build().createChild(name);
            b.setSerializable(false);
        }
        b.setValueType(ValueType.STRING);
        Node node = b.build();
        // Extra assurance in case a parent never had its type set
        node.setValueType(ValueType.STRING);
        node.setValue(new Value(msg.toString()));
        node.setWritable(Writable.WRITE);
        node.getListener().setValueHandler(new Handler<ValuePair>() {
            @Override
            public void handle(ValuePair event) {
                event.setReject(true);
                publish(s, event.getCurrent().toString(), false);
            }
        });
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
            while (node != null) {
                if (hasSub(node) || node.getChildren().size() > 0) {
                    break;
                }

                Value value = node.getRoConfig("preserve");
                if (value != null && value.getBool()) {
                    break;
                }

                node.getParent().removeChild(node);
                node = node.getParent();
            }
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
        if (node == null) {
            return false;
        }
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
                if (child.getAction() != null
                    || child.getWritable() != null 
		    || child.isHidden()) {
                    continue;
                }
                stpe.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
			    LOGGER.info("Restoring connection to server '{}'", child.getName() );
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
