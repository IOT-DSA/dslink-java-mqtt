package org.dsa.iot.mqtt.utils;

import org.dsa.iot.commons.GuaranteedReceiver;
import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.util.URLInfo;
import org.dsa.iot.mqtt.Mqtt;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class ClientReceiver extends GuaranteedReceiver<MqttClient> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientReceiver.class);

    private final Node mqttNode;
    private final Mqtt callback;

    public ClientReceiver(Node node, Mqtt callback) {
        this.mqttNode = node;
        this.callback = callback;
    }

    @Override
    protected MqttClient instantiate() throws Exception {
        String url = mqttNode.getRoConfig("url").getString();
        String id = mqttNode.getRoConfig("clientId").getString();
        MqttClient client = new MqttClient(url, id, new MemoryPersistence());

        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setMaxInflight(1000);
        URLInfo info = URLInfo.parse(url);
        if ("ssl".equals(info.protocol)) {
            opts.setSocketFactory(new InsecureSslSocketFactory());
        }

        Value vUser = mqttNode.getRoConfig("user");
        if (vUser != null) {
            opts.setUserName(vUser.getString());
        }

        char[] pass = mqttNode.getPassword();
        if (pass != null) {
            opts.setPassword(pass);
        }

        client.setCallback(callback);
        client.connect(opts);
        callback.setStatus(true);
        LOGGER.info("Opened connection to MQTT at {}", url);
        return client;
    }

    @Override
    protected boolean invalidateInstance(Exception e) {
        Throwable cause = e.getCause();
        if (cause instanceof MqttException) {
            MqttException ex = (MqttException) cause;
            int code = ex.getReasonCode();
            if (code == MqttException.REASON_CODE_CLIENT_NOT_CONNECTED
                    || code == MqttException.REASON_CODE_CONNECTION_LOST) {
                callback.setStatus(false);
                LOGGER.error("Connection died ({})", code);
                return true;
            }
        }
        return false;
    }
}
