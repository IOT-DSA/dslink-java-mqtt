package org.dsa.iot.mqtt;

import org.dsa.iot.commons.ParameterizedAction;
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

import java.util.Map;

import static org.dsa.iot.commons.ParameterizedAction.ParameterInfo;

/**
 * @author Samuel Grenier
 */
public class Actions {

    private static final Logger LOGGER = LoggerFactory.getLogger(Actions.class);

    public static Action getAddServerAction(final Node node) {
        final ValueType vt = ValueType.STRING;
        Action a = new Action(Permission.WRITE, new Handler<ActionResult>() {
            @Override
            public void handle(ActionResult event) {
                Value vName = event.getParameter("name", vt);
                Value vUrl = event.getParameter("url", vt);
                Value vClientId = event.getParameter("clientId", vt);
                Value vUser = event.getParameter("username");
                Value vPass = event.getParameter("password");
                Value vQos = event.getParameter("qos", vt);

                String name = vName.getString();
                if (!node.hasChild(name)) {
                    NodeBuilder child = node.createChild(name);
                    if (vUser != null) {
                        child.setRoConfig("user", vUser);
                    }

                    if (vPass != null) {
                        child.setPassword(vPass.getString().toCharArray());
                    }

                    child.setRoConfig("url", vUrl);
                    child.setRoConfig("clientId", vClientId);
                    {
                        vQos = new Value(Integer.parseInt(vQos.getString()));
                        child.setRoConfig("qos", vQos);
                    }

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

            String desc = "URL schemes must be either: tcp, ssl, or local";
            p.setDescription(desc);
            a.addParameter(p);
        }
        a.addParameter(new Parameter("username", vt));
        a.addParameter(new Parameter("password", vt));
        {
            Parameter p = new Parameter("clientId", vt);
            String desc = "The client ID must always be unique to the server";
            p.setDescription(desc);
            a.addParameter(p);
        }
        {
            ValueType type = ValueType.makeEnum("0", "1", "2");
            Parameter p = new Parameter("qos", type);
            String desc =
                    "0: The broker/client will deliver the message once, with no confirmation.\n" +
                    "1: The broker/client will deliver the message at least once, with confirmation required.\n" +
                    "2: The broker/client will deliver the message exactly once by using a four step handshake.";
            p.setDescription(desc);
            a.addParameter(p);
        }
        return a;
    }

    public static Action getEditServerAction(final Mqtt mqtt) {
        ParameterizedAction a = new ParameterizedAction(Permission.WRITE) {
            @Override
            public void handle(ActionResult actRes,
                               Map<String, Value> params) {
                String username = null;
                char[] password = null;

                Value vUser = params.get("username");
                if (vUser != null) {
                    username = vUser.getString();
                    if (username.isEmpty()) {
                        username = null;
                    } else {
                        Value vPass = params.get("password");
                        if (vPass != null) {
                            password = vPass.getString().toCharArray();
                        }
                    }
                }

                String url = params.get("url").getString();
                String clientId = params.get("clientId").getString();
                String sQos = params.get("qos").getString();
                int qos = Integer.parseInt(sQos);
                mqtt.edit(url, username, password, clientId, qos);
            }
        };
        {
            ParameterInfo info = new ParameterInfo("url", ValueType.STRING);
            info.setDefaultValue(new Value(mqtt.getUrl()));
            {
                String desc = "URL schemes must be either: tcp, ssl, or local";
                info.setDescription(desc);
            }
            info.setPersistent(true);
            a.addParameter(info);
        }
        {
            ParameterInfo info = new ParameterInfo("username", ValueType.STRING);
            info.setDefaultValue(new Value(mqtt.getUsername()));
            info.setOptional(true);
            info.setPersistent(true);
            a.addParameter(info);
        }
        {
            ParameterInfo info = new ParameterInfo("password", ValueType.STRING);
            info.setPersistent(false);
            info.setOptional(true);
            info.setPlaceHolder("Leave empty to prevent change");
            a.addParameter(info);
        }
        {
            ParameterInfo info = new ParameterInfo("clientId", ValueType.STRING);
            info.setDefaultValue(new Value(mqtt.getClientId()));
            info.setPersistent(true);
            String desc = "The client ID must always be unique to the server";
            info.setDescription(desc);
            a.addParameter(info);
        }
        {
            ValueType type = ValueType.makeEnum("0", "1", "2");
            ParameterInfo info = new ParameterInfo("qos", type);
            info.setDefaultValue(new Value(String.valueOf(mqtt.getQos())));
            info.setPersistent(true);

            String desc =
                    "0: The broker/client will deliver the message once, with no confirmation.\n" +
                    "1: The broker/client will deliver the message at least once, with confirmation required.\n" +
                    "2: The broker/client will deliver the message exactly once by using a four step handshake.";
            info.setDescription(desc);

            a.addParameter(info);
        }
        return a;
    }

    public static Action getRemoveServerAction(final Mqtt mqtt,
                                               final Node parent) {
        return new Action(Permission.WRITE, new Handler<ActionResult>() {
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
