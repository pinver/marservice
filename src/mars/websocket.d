

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
void handleWebSocketConnection(scope WebSocket socket)
{


    // ... the HTTP request that established the web socket connection, let's extract the client address & session...
    string clientAddress = socket.request.clientAddress.toString();
    string sessionId = socket.request.session.id;

    // ... we can receive text and binary data, and we start with text ...
    ReceiveMode receiveMode = ReceiveMode.text;

    // Task that receives and dispatch data to/for the socket
    void dataDispatcher(Task receiver)
    {
        //logInfo("task dataDispatcher starting for sessionid %s", sessionId);
        scope(exit) logInfo("task dataDispatcher terminating for sessionid %s", sessionId);
        try {
            while(true)
            {
                if( receiveMode == ReceiveMode.text ){
                    auto socketData = receiveOnly!SocketData();
                    final switch(socketData.flow) with(Flow) {
                        case connectionLost:
                            logInfo("mars - connection lost received by data dispatcher, so terminating");
                            return;
                        case received:
                            receiver.send(HandlerData(socketData.data));
                            break;
                        case toSend:
                            //logInfo("mars - dispatcher sending data via websocket: %s", socketData.data);
                            socket.send(socketData.data);
                            break;
                    }
                }
                else {
                    auto socketData = receiveOnly!SocketBinaryData();
                    final switch(socketData.flow) with(Flow) {
                        case connectionLost:
                            //logInfo("mars - connection lost received by web socket receiver, so terminating");
                            import mars.msg : MarsMsgType = MsgType;
                            int messageId = 0; auto msgType = MarsMsgType.aborting;
                            immutable(ubyte)[8] prefix = (cast(immutable(ubyte)*)(&(messageId)))[0 .. 4] 
                                                           ~ (cast(immutable(ubyte)*)(&(msgType)))[0 .. 4];
                            //trace("sending forged abort to the protocol", prefix);
                            // XXX non ho capito perchè, ma con solo il prefix, viene ricevuto sminchio ...
                            receiver.send(HandlerBinaryData( (prefix ~ prefix).idup ));
                            //trace("sending forged abort done, returning and exiting the task");
                            
                            return;
                        case received:
                            receiver.send(HandlerBinaryData(socketData.data));
                            break;
                        case toSend:
                            //logInfo("mars - dispatcher sending binary data via websocket: length %d", socketData.data.length);
                            socket.send(socketData.data.dup);
                            break;
                    }
                }
            }
        }
        catch(Exception e){
            logError("mars - task dataDispatcher exception!");
            logError(e.toString());
        }
    }

    // Task that receives from the websocket and dispatch to the above task
    void wsReceiver(Task receiver)
    {
        //logInfo("task wsReceiver starting");
        //scope(exit) logInfo("task wsReceiver terminating");
        try {
            while( socket.waitForData() )
            {
                if( receiveMode == ReceiveMode.text ) {
                    string data = socket.receiveText(true);
                    //logInfo("mars - received data from websocket:%s", data);
                    receiver.send(SocketData(Flow.received, data));
                }
                else {
                    immutable(ubyte)[] data = socket.receiveBinary().idup;
                    //logInfo("mars - received binary data from websocket with length %d", data.length);
                    receiver.send(SocketBinaryData(Flow.received, data));
                }
            }
            logInfo("mars - task websocket receiver connection lost!");
            // inform the other task that the connection is lost
            if( receiveMode == ReceiveMode.binary ){
                receiver.send(SocketBinaryData(Flow.connectionLost));
            } else {
                receiver.send(SocketData(Flow.connectionLost));
            }
            //logInfo("mars - task websocket receiver exiting");
        }
        catch(Exception e){
            logError("mars - task wsReceiver exception!");
            logError(e.toString());
        }
    }

    // Activate the tasks for this client ....
    auto dataDispatcherTask = runTask( &dataDispatcher, Task.getThis );
    auto wsReceiverTask = runTask( &wsReceiver, dataDispatcherTask );   

    // Identify the client type and start processing it ...
    import mars.protohelo : protoHelo;
    protoHelo(Proxy(dataDispatcherTask, &receiveMode)); // XXX I don't like this, but...

    // ... we have terminated the client process, 
    logInfo("mars - exiting the task that handled the websocker:%d tasks are running", dataDispatcherTask.taskCounter());
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
