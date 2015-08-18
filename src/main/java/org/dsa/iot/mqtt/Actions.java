package org.dsa.iot.mqtt;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;

/**
 * @author Samuel Grenier
 */
public class Actions {

    private static final Logger LOGGER = LoggerFactory.getLogger(Actions.class);

    public static Action getAddServerAction(final Node node) {
        final ValueType vt = ValueType.STRING;
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vName = event.getParameter("name", vt);
                Value vUrl = event.getParameter("url", vt);
                Value vClientId = event.getParameter("clientId", vt);
                Value vUser = event.getParameter("username");
                Value vPass = event.getParameter("password");

                String name = vName.getString();
                if (node.getChild(name) == null) {
                    NodeBuilder child = node.createChild(name);
                    if (vUser != null) {
                        child.setRoConfig("user", vUser);
                    }

                    if (vPass != null) {
                        child.setPassword(vPass.getString().toCharArray());
                    }

                    child.setRoConfig("url", vUrl);
                    child.setRoConfig("clientId", vClientId);

                    try {
                        Mqtt mqtt = new Mqtt(child.build());
                        mqtt.init();
                    } catch (MqttException e) {
                        LOGGER.warn("Error adding server", e);
                        node.removeChild(name);
                    }
                }
            }
        });
        a.addParameter(new Parameter("name", vt));
        {
            Parameter p = new Parameter("url", vt);
            p.setPlaceHolder("tcp://test.mosquitto.org/");

            String desc = "URL schemes must either: tcp, ssl, or local";
            p.setDescription(desc);
            a.addParameter(p);
        }
        a.addParameter(new Parameter("username", vt));
        a.addParameter(new Parameter("password", vt));
        a.addParameter(new Parameter("clientId", vt).setPlaceHolder("dsa1234"));
        return a;
    }

    public static Action getRemoveServerAction(final Mqtt mqtt,
                                               final Node parent) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                parent.getParent().removeChild(parent);

                try {
                    mqtt.disconnect();
                } catch (Exception ignored) {
                }
            }
        });
    }

    public static Action getPublishAction(final Mqtt mqtt) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vTopic = event.getParameter("topic", ValueType.STRING);
                Value vValue = event.getParameter("value", ValueType.STRING);
                Value vQos = event.getParameter("qos", ValueType.NUMBER);
                Value vRetained = event.getParameter("retained", ValueType.BOOL);

                String topic = vTopic.getString();
                String value = vValue.getString();
                int qos = vQos.getNumber().intValue();
                boolean retained = vRetained.getBool();

                mqtt.publish(topic, value, qos, retained);
            }
        });
        a.addParameter(new Parameter("topic", ValueType.STRING));
        a.addParameter(new Parameter("value", ValueType.STRING));
        {
            Parameter p = new Parameter("qos", ValueType.NUMBER);
            p.setDefaultValue(new Value(2));

            String desc = "QoS ranges from 0-2";
            desc += "\nQoS 0 delivers the message but is not acknowledged by the network";
            desc += "\nQos 1 guarantees the network has the message but can be duplicated";
            desc += "\nQos 2 guarantees the network has the message and receives it only once";
            p.setDescription(desc);
            a.addParameter(new Parameter("qos", ValueType.NUMBER, new Value(2)));
        }
        a.addParameter(new Parameter("retained", ValueType.BOOL, new Value(true)));
        return a;
    }

    public static Action getSubscribeAction(final Mqtt mqtt) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vName = event.getParameter("name", ValueType.STRING);
                Value vTopic = event.getParameter("topic", ValueType.STRING);
                Value vQos = event.getParameter("qos", ValueType.NUMBER);

                String name = vName.getString();
                String topic = vTopic.getString();
                int qos = vQos.getNumber().intValue();
                MqttMessage.validateQos(qos);

                mqtt.subscribe(name, topic, qos);
            }
        });
        a.addParameter(new Parameter("name", ValueType.STRING));
        a.addParameter(new Parameter("topic", ValueType.STRING));
        a.addParameter(new Parameter("qos", ValueType.NUMBER, new Value(2)));
        return a;
    }

    public static Action getUnsubscribeAction(final Mqtt mqtt,
                                              final String name) {
        return new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                mqtt.unsubscribe(name);
            }
        });
    }
}
