# Getting Started

The Client Portal gateway is available for download at: [http://download2.interactivebrokers.com/portal/clientportal.gw.zip](http://download2.interactivebrokers.com/portal/clientportal.gw.zip)

You can download and extract to any location your user has access to. We will install it under 

C:\gateway\ in Windows or ~user\gateway in Linux.

The gateway requires Java 1.8 update 192 or higher to run, and has been tested successfully with OpenJDK 11. 

Oracle Java 8 download: [https://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html](https://www.oracle.com/technetwork/java/javase/downloads/jre8-downloads-2133155.html)

Once you extract the .zip file, you will see the following directories:

- **bin** contains the run scripts for Linux and Windows

- **build** contains all the 3rd party libraries required for the gateway to run

- **dist** contains the .jar file for the gateway

- **doc** contains this GettingStarted.md guide

- **root** contains files required for the runtime configuration of the gateway and is also the location where webapps reside. We will explain those in more detail later.

To start the gateway you need to open a command prompt or bash on the directory the files were extracted. In our case we will open windows -> run -> cmd and go to c:\gateway\.

Once in that directory you can run *"bin\run.sh root/conf.yaml"* or *"bin\run.bat root\conf.yaml"*

Once the gateway is running, you should see the following entry in the console:
"Server listening on port 5000" 
By default the gateway runs in SSL mode and port 5000. 

Now that the gateway is running, you are ready to authenticate, to do that open your browser and go to:
[https://localhost:5000/](https://localhost:5000/)

In this page you should see our regular login page which is also visible here:
[https://gdcdyn.interactivebrokers.com/sso/Login?forwardTo=22](https://gdcdyn.interactivebrokers.com/sso/Login?forwardTo=22)

Once you login, the gateway will confirm the client is authenticated and is ok to close the browser window. Or will display any reasons why the authentication may have failed.

Once the gateway is authenticated you can close the browser or navigate away.

From this point on, the end points documented in the API spec should be available for you to query with curl or any other HTTP client.

[https://gdcdyn.interactivebrokers.com/portal.proxy/v1/portal/swagger/swagger?format=yaml](https://gdcdyn.interactivebrokers.com/portal.proxy/v1/portal/swagger/swagger?format=yaml)

[https://rebilly.github.io/ReDoc/?url=https://rebilly.github.io/ReDoc/?url=https://gdcdyn.interactivebrokers.com/portal.proxy/v1/portal/swagger/swagger?format=yaml](https://rebilly.github.io/ReDoc/?url=https://rebilly.github.io/ReDoc/?url=https://gdcdyn.interactivebrokers.com/portal.proxy/v1/portal/swagger/swagger?format=yaml)

There is an external Client Portal API guide with test pages at: [https://interactivebrokers.github.io/cpwebapi](https://interactivebrokers.github.io/cpwebapi)

