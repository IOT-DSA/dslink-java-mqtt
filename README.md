# dslink-java-mqtt

[![Build Status](https://drone.io/github.com/IOT-DSA/dslink-java-mqtt/status.png)](https://drone.io/github.com/IOT-DSA/dslink-java-mqtt/latest)

A DSLink that works with MQTT. More information about MQTT can be
located at <http://mqtt.org/>

[![IMAGE ALT TEXT HERE](http://img.youtube.com/vi/kmZljxH9r3I/0.jpg)](http://www.youtube.com/watch?v=kmZljxH9r3I)

## Distributions

Distributions can be ran independent of Gradle and anywhere Java is installed.
Prebuilt distributions can be found [here](https://drone.io/github.com/IOT-DSA/dslink-java-mqtt/files).

### Creating a distribution

Run `./gradlew distZip` from the command line. Distributions will be located
in `build/distributions`.

### Running a distribution

Run `./bin/dslink-java-mqtt -b http://localhost:8080/conn` from the command
line. The link will then be running.

## Test running

A local test run requires a broker to be actively running. When adding MQTT
servers all URLs must be prefixed with either:

* tcp://
* ssl://
* local://

Running: <br />
`./gradlew run -Dexec.args="--broker http://localhost:8080/conn"`

## Common issues/troubleshooting

### Status of new MQTT connection is "Disconnected"

When a new MQTT connection is created, the status always starts as 'Disconnected' since there is no subscription. Subscribe to the topics then if it works the status will be changed to 'Connected'.

### After subscription data is not updating

This could be because of (1) server settings or (2) Broker is not reachable from server

#### (1) Server Settings:

 - Check server URL is correct. remove unwanted '/', ':' or any other character.

- Verify if the server needs any authentication

- Check the broker port details. Default port for most MQTT brokers is 1883 for plain MQTT or 8883 for MQTT over TLSs 

- Ensure that the client id is unique

#### (2)  Network Analysis:

- Try to connect to broker from server using a standlone tool like 'mosquitto_sub' (more details at https://mosquitto.org/man/mosquitto_sub-1.html). 
    - Simple example:  `mosquitto_sub -h  <host url> -t <topics> -v`. 
    - You could use 'mosquitto_pub' tool to publish topics for testing.
