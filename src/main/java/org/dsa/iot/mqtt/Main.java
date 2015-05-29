package org.dsa.iot.mqtt;

import org.dsa.iot.dslink.DSLink;
import org.dsa.iot.dslink.DSLinkFactory;
import org.dsa.iot.dslink.DSLinkHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Samuel Grenier
 */
public class Main extends DSLinkHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    @Override
    public void onResponderInitialized(DSLink link) {
        Mqtt.init(link.getNodeManager().getNode("/").getNode());
        LOGGER.info("Initialized");
    }

    @Override
    public void onResponderConnected(DSLink link) {
        LOGGER.info("Connected");
    }

    public static void main(String[] args) {
        DSLinkFactory.startResponder("mqtt", args, new Main());
    }
}
