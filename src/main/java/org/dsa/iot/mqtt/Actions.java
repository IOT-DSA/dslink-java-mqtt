package org.dsa.iot.mqtt;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeBuilder;
import org.dsa.iot.dslink.node.Permission;
import org.dsa.iot.dslink.node.actions.Action;
import org.dsa.iot.dslink.node.actions.ActionResult;
import org.dsa.iot.dslink.node.actions.Parameter;
import org.dsa.iot.dslink.node.value.Value;
import org.dsa.iot.dslink.node.value.ValueType;
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
                    } catch (RuntimeException e) {
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
                Value vRetained = event.getParameter("retained", ValueType.BOOL);

                String topic = vTopic.getString();
                String value = vValue.getString();
                boolean retained = vRetained.getBool();

                mqtt.publish(topic, value, retained);
            }
        });
        a.addParameter(new Parameter("topic", ValueType.STRING));
        a.addParameter(new Parameter("value", ValueType.STRING));
        a.addParameter(new Parameter("retained", ValueType.BOOL, new Value(true)));
        return a;
    }

    public static Action getSubscribeAction(final Mqtt mqtt) {
        Action a = new Action(Permission.READ, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vName = event.getParameter("name", ValueType.STRING);
                Value vTopic = event.getParameter("topic", ValueType.STRING);

                String name = vName.getString();
                String topic = vTopic.getString();
                mqtt.subscribe(name, topic);
            }
        });
        a.addParameter(new Parameter("name", ValueType.STRING));
        {
            Parameter p = new Parameter("topic", ValueType.STRING);
            p.setPlaceHolder("+/path/topics/#");
            a.addParameter(p);
        }
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
