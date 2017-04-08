

/**
 * Vibe gestire la ricezione di dati da websocket con una wait bloccante, cosa ottima per un protocollo req/rep, ma
 * complicata se si vuol gestire in contemporanea anche un protocollo push lato server, o task paralleli.
 *
 * Questo modulo si occupa di isolare il websocket, inviando i dati ricevuti ad un task, e facendo da proxy per i 
 * send.
 */

module mars.websocket;


import vibe.core.concurrency;
import vibe.core.core;
import vibe.core.log;
import vibe.core.task;
import vibe.http.websockets;

enum Flow { received, toSend, connectionLost }
struct SocketData { Flow flow; string data; }
struct SocketBinaryData { Flow flow; immutable(ubyte)[] data; }
struct HandlerData { string data; }
struct HandlerBinaryData { immutable(ubyte)[] data; }

enum ReceiveMode { text, binary }

/**
 * Entry point of the task that is handling the websocket connection with the client that has just joined us. */
void handleWebSocketConnectionClientToService(scope WebSocket socket)
{
    logInfo("mars - C ... S - a webclient has opened the 'Client to Service' channel - socket:%s", &socket);
    scope(success) logInfo("mars - C ... S - exiting the websocket handler task with success, the socket will be disposed - socket:%s", &socket);
    scope(failure) logError("mars - C ... S - exiting the websocket handler task for a failure! the socket will be disposed socket:%s", &socket);

    vibe.core.core.yield();

    // ... the HTTP request that established the web socket connection, let's extract the client address & session...
    string clientAddress = socket.request.clientAddress.toString();
    string sessionId = socket.request.session.id;

    // ... we can receive text and binary data, and we start with text ...
    ReceiveMode receiveMode = ReceiveMode.text; 

    // Identify the client type and start processing it ...
    import mars.protohelo : protoHelo;
    //protoHelo(Proxy(dataDispatcherTask, &receiveMode));
    protoHelo(socket);

    // ... we have terminated the client process, 
    
}

/**
 * Entry point of the task that is handling the connection that allow the service to push messages to the web client.
 *
 * First the client opens the connection that it uses to send messages to the service, THEN this one.
 */
void handleWebSocketConnectionServiceToClient(scope WebSocket socket)
{
    import mars.server : marsServer;

    logInfo("mars - S ... C - a webclient has opened the 'Service to Client' channel - socket:%s", &socket);
    scope(success) logInfo("mars - S ... C - exiting the websocket handler task with success, the socket will be disposed - socket:%s", &socket);
    scope(failure) logError("mars - S ... C - exiting the websocket handler task for a failure! the socket will be disposed - socket:%s", &socket);

    import mars.server : marsServer;

    string clientId = socket.receiveText();
    logInfo("mars - S ... C - received the client identifier:%s", clientId);

    // expose this task to the marsClient, so that it can push request to the web client
    assert(marsServer !is null);
    auto client = marsServer.getClient(clientId);
    if( client is null ){
        logError("mars - S ... C - can't find the mars client with id %s in the server registered clients", clientId);
        return;
    }
    import mars.protomars : MarsProxyStoC;
    client.wireSocket(MarsProxyStoC!WebSocket(socket, clientId));

    logInfo("mars - S ... C - waiting for termination");
    string terminate = receiveOnly!string();
}

struct Proxy {
    
    import std.experimental.logger : logInfo = log, trace;
    alias logError = logInfo;

    this(Task dispatcher, ReceiveMode* receiveMode)
    { 
        import core.atomic : atomicOp;

        this.dispatcher = dispatcher;
        this.receiveMode = receiveMode;
        this.seqIdentifier = sequence; 
        atomicOp!"+="(sequence, 1);
    }

    void switchToBinaryMode() { *receiveMode = ReceiveMode.binary; }

    string receive() {
        auto data = receiveOnly!HandlerData();
        return data.data;
    }

    immutable(ubyte)[] receiveBinary() {
        auto data = receiveOnly!HandlerBinaryData();
        return data.data;
    }
    
    void send(string data) {
        //trace("tracing...");
        dispatcher.send(SocketData(Flow.toSend, data));
        //trace("tracing...");
    }

    void send(immutable(ubyte)[] data) {
        //trace("tracing...");
        dispatcher.send(SocketBinaryData(Flow.toSend, data));
        //trace("tracing...");
    }

    private {
        Task dispatcher;
        ReceiveMode* receiveMode; 

        int seqIdentifier;
        static shared int sequence = 1;
    }
}
