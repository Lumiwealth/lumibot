@echo off

rem example setting a JAVA_HOME and adding to PATH
#set JAVA_HOME=c:\jdk
#set PATH=%JAVA_HOME%\bin;%PATH%

if exist %1 goto FOUND_CONF
echo "usage: %0 /path/to/conf.yaml"
goto END

:FOUND_CONF
set config_file=%1
for /F %%i in ("%config_file%") do set config_path=%%~dpi
echo "config path :%config_path%"

set RUNTIME_PATH="%config_path%;dist\ibgroup.web.core.iblink.router.clientportal.gw.jar;build\lib\runtime\*"

echo "running %verticle% "
echo "runtime path : %RUNTIME_PATH%"

java -server -Dvertx.disableDnsResolver=true -Djava.net.preferIPv4Stack=true -Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory -Dnologback.statusListenerClass=ch.qos.logback.core.status.OnConsoleStatusListener -Dnolog4j.debug=true -Dnolog4j2.debug=true -classpath %RUNTIME_PATH% ibgroup.web.core.clientportal.gw.GatewayStart
rem optional arguments
rem -conf conf.beta.yaml --nossl

:END
