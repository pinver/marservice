

/**
 * Vibe gestisce la ricezione di dati da websocket con una wait bloccante, cosa ottima per un protocollo req/rep, ma
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
void handleWebSocketConnectionClientToService(scope WebSocket webSocket)
{
    logInfo("mars - C ... S - a webclient has opened the 'Client to Service' channel - socket:%s", &webSocket);
    scope(success) logInfo("mars - C ... S - exiting the websocket handler task with success, the socket will be disposed - socket:%s", &webSocket);
    scope(failure) logError("mars - C ... S - exiting the websocket handler task for a failure! the socket will be disposed socket:%s", &webSocket);

    try {
    vibe.core.core.yield();

    // ... the HTTP request that established the web socket connection, let's extract the client address & session...
    string clientAddress = webSocket.request.clientAddress.toString();
    string sessionId = webSocket.request.session.id;

    // ... we can receive text and binary data, and we start with text ...
    ReceiveMode receiveMode = ReceiveMode.text; 

    // Identify the client type and start processing it ...
    import mars.protohelo : protoHelo;
    auto socket = ResilientWebSocket(webSocket);
    protoHelo(socket);
    } catch(Exception e){ logError("mars - C ... S - catched throwable and retrowing:%s", e.msg); throw e; }
}

/**
 * Entry point of the task that is handling the connection that allow the service to push messages to the web client.
 *
 * First the client opens the connection that it uses to send messages to the service, THEN this one.
 */
void handleWebSocketConnectionServiceToClient(scope WebSocket webSocket)
{
    import mars.server : marsServer;

    try {
    logInfo("mars - S ... C - a webclient has opened the 'Service to Client' channel - socket:%s", &webSocket);
    scope(success)
        logInfo("mars - S ... C - exiting the websocket handler task with success, the websocket will be disposed - websocket:%s", &webSocket);
    scope(failure)
        logError("mars - S ... C - exiting the websocket handler task for a failure! the websocket will be disposed - socket:%s", &webSocket);

    auto socket = ResilientWebSocket(webSocket);

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
    auto proxy = MarsProxyStoC!ResilientWebSocket(socket, clientId);
    client.wireSocket(proxy, Task.getThis);

    logInfo("mars - S ... C - waiting for termination"); string terminate = receiveOnly!string();
    logInfo("mars - S ... C - received the terminate signal:%s", terminate);

    } catch(Exception e){ logError("mars - C ... S - catched throwable and retrowing:%s", e.msg); throw e; }
}


/**
 * WebSocket resilient to network disconnections.
 *
 * The actual implementation of Ws imply a throw during a read/write if the connection goes down.
 * Every driver has it's own way of throwing, the actual one it's throwing a plain exception: we try to catch
 * and inspect the exception to detect if the WS was disconnected, reporting that fact in a clear way.
 */
struct ResilientWebSocket
{
    @disable this(this);

    /// send data over the websocket. returns true/false if sent or not.
    bool send(ubyte[] data) {
        bool sent = true;
        try {
            socket.send(data); // ..."throws WebSocketException if the connection is closed." ...
        }
        catch(WebSocketException e){
            logInfo("mars - the websocket seems closed, data not sent");
            sent = false;
        }
        catch(Exception e){
            logInfo("mars - catched during websocket.send! the exception message is '%s'! trying to handle it", e.msg);
            switch(e.msg){
                case "The remote peer has closed the connection.":
                case "WebSocket connection already actively closed.":
                case "Remote hung up while writing to TCPConnection.":
                case "Connection error while writing to TCPConnection.":
                    logWarn("mars - please classify the exception in websocket module!");
                    sent = false;
                    break;
                default:
                    logError("mars - catched during socket.send! the exception message is '%s'! now rethrowing!", e.msg);
                    throw e;
            }
        }
        return sent;
    }
    bool send ( scope const(char)[] data) {
        bool sent = true;
        try {
            socket.send(data); // ..."throws WebSocketException if the connection is closed." ...
        }
        catch(WebSocketException e){
            logInfo("mars - the websocket seems closed, data not sent");
            sent = false;
        }
        catch(Exception e){
            logInfo("mars - catched during websocket.send! the exception message is '%s'! trying to handle it", e.msg);
            switch(e.msg){
                case "The remote peer has closed the connection.":
                case "WebSocket connection already actively closed.":
                case "Remote hung up while writing to TCPConnection.":
                case "Connection error while writing to TCPConnection.":
                    logWarn("mars - please classify the exception in websocket module!");
                    sent = false;
                    break;
                default:
                    logError("mars - catched during socket.send! the exception message is '%s'! now rethrowing!", e.msg);
                    throw e;
            }
        }
        return sent;
    }

    string receiveText ( bool strict = true ){
        try {
            return socket.receiveText(strict);
        }
        catch(Exception e){
            logError("mars - catched during websocket receiveText! Rethrowing! msg:%s", e);
            throw e;
        }
    }

    private {
        WebSocket socket;
    }

    ubyte[] receiveBinary ( bool strict = true ){
        try {
            return socket.receiveBinary(strict);
        }
        catch(WebSocketException e){
            logInfo("mars - the sebsocket seems closed, data not received");
            return [];
        }
        catch(Exception e){
            logInfo("mars - catched during websocket.receiveBinary! the exception message is '%s'! trying to handle it", e.msg);
            switch(e.msg){
                default:
                    logError("mars - catched during socket.send! the exception message is '%s'! now rethrowing!", e.msg);
                    throw e;
            }
        }
    }
}

