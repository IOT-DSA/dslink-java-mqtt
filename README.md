# dslink-java-mqtt

[![Build Status](https://drone.io/github.com/IOT-DSA/dslink-java-mqtt/status.png)](https://drone.io/github.com/IOT-DSA/dslink-java-mqtt/latest)

A DSLink that works with MQTT. More information about MQTT can be
located at <http://mqtt.org/>

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
