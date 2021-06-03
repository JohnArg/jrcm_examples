# jRCM Code Examples

## Description
This project was created to provide examples on how to use the 
[jRCM Java library](https://github.com/JohnArg/jrcm) for RDMA
communications and RPCs.
There are two examples:

    1. A client/server example where a client and a server
        exchange messages with SEND/RECV through jRCM. 
    2. A discovery service example. The discovery service is 
        used for server registration, deregistration, 
        getting registered servers and getting a server's port
        by using it's IP. A client is also provided for 
        connecting to the discovery service and invoking its
        API through RPCs. This example shows have can jRCM
        be used for RDMA RPCs with SEND/RECV.

## Usage

### Client/Server Example

Run the TwoSidedClientServerApp for in server and client.

* For the server, pass the arguments : s \<server ip\> \<server port\>
* For the client, pass the arguments : c \<server ip\> \<server port\>

### Discovery Service Example

Run the ServerDiscoveryRpcApp in for both the discovery service
and the discovery service client.

* For the discovery service, pass the arguments : s \<service ip\> \<service port\>
* For the discovery service client, pass the arguments : c \<service ip\> \<service port\>