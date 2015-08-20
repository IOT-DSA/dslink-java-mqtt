package org.dsa.iot.mqtt;

import org.dsa.iot.dslink.node.Node;
import org.dsa.iot.dslink.node.NodeManager;
import org.dsa.iot.dslink.node.exceptions.NoSuchPathException;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Samuel Grenier
 */
public class MqttTest {

    @Test
    public void destroyTree() {
        final NodeManager manager = new NodeManager(null, "node");
        Node data = manager.createRootNode("data").build();
        final Mqtt mqtt = new Mqtt(data);

        Node node = data.createChild("a").build();
        node.createChild("b").build();
        mqtt.destroyTree("#", data);
        Assert.assertTrue(data.getChildren().isEmpty());

        node = data.createChild("a").build();
        node.createChild("b").build();
        mqtt.destroyTree("+", data);
        Assert.assertTrue(data.getChildren().isEmpty());

        data = manager.createRootNode("data").build();
        node = data.createChild("a").build();
        Node child = node.createChild("b").build();
        child.createChild("1").build();
        child.createChild("2").build();

        final String pathA = "/data/a/b/1";
        final String pathB = "/data/a/b/2";

        manager.getNode(pathA);
        mqtt.destroyTree("/a/+/1", data);
        manager.getNode(pathB);

        boolean exception = false;
        try {
            manager.getNode(pathA);
        } catch (NoSuchPathException e) {
            exception = true;
        }
        Assert.assertTrue(exception);

        mqtt.destroyTree("/a/#", data);
        exception = false;
        try {
            manager.getNode(pathB);
        } catch (NoSuchPathException e) {
            exception = true;
        }
        Assert.assertTrue(exception);
    }

}
