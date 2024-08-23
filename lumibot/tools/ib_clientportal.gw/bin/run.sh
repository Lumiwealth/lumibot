#!/bin/bash

# example setting a JAVA_HOME and adding to PATH
#export JAVA_HOME=/usr/local/jdk1.8_x64/
#export PATH=$JAVA_HOME/bin:$PATH

if [ $# -lt 1 ]; then
    >&2 echo "usage: $0 /path/to/conf.yaml"
    exit 1
fi

config_file=$1
config_path=$(dirname $1)
name=$(basename $config_path)

export RUNTIME_PATH="$config_path:dist/ibgroup.web.core.iblink.router.clientportal.gw.jar:build/lib/runtime/*"

echo "running $verticle "
echo " runtime path : $RUNTIME_PATH"
echo " config file  : $config_file"

java \
-server \
-Dvertx.disableDnsResolver=true \
-Djava.net.preferIPv4Stack=true \
-Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory \
-Dnologback.statusListenerClass=ch.qos.logback.core.status.OnConsoleStatusListener \
-Dnolog4j.debug=true \
-Dnolog4j2.debug=true \
-cp "${RUNTIME_PATH}" \
ibgroup.web.core.clientportal.gw.GatewayStart \
--conf ../$config_file \
