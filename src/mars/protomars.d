

module mars.protomars;

import vibe.core.core;
import vibe.core.log;

import mars.client;
import mars.server;
import mars.msg;

import mars.protoauth;
import mars.protocallmethod;
import mars.protoinsertvaluerequest;
import mars.protodeleterecordrequest;
import mars.protoupd;
import mars.protosub;

void protoMars(S)(MarsClient* client, S socket_)
{
    // ... we are switching to binary msgpack ...
    //socket_.switchToBinaryMode();
    auto socket = MarsProxy!S(socket_, client.id);
    
    // ... client must be wired to this socket, to be able to 'broadcast' or 'push' message to the browser ...
    while( ! client.socketWired ) vibe.core.core.yield; 

    // ... now the procol between client and server is fully operative, inform the server
    assert(marsServer !is null);
    marsServer.onMarsProtocolReady(client);

    while(true)
    {
        auto msgType = socket.receiveType();
        if( msgType == MsgType.aborting) break;

        switch(msgType) with(MsgType)
        {
            case authenticationRequest:
                logInfo("mars - S<--%s - received an authenticationRequest", client.id);
                protoAuth(client, socket);
                break;

            case discardAuthenticationRequest:
                logInfo("mars - S<--%s - received a discardAuthenticationRequest", client.id);
                protoDeauth(client, socket);
                break;

            //case syncOperationReply:
            //    logInfo("mars - S<--%s - received a syncOperationReply", client.id);
            //    break;

            case importValuesReply:
                logInfo("mars - S<--%s - received an importValuesReply", client.id);
                break;

            case insertValuesReply:
                logInfo("mars - S<--%s - received an insertValuesReply", client.id);
                break;

            case updateValuesReply:
                logInfo("mars - S<--%s - received an updateValuesReply", client.id);
                break;

            case callServerMethodRequest:
                logInfo("mars - S<--%s - received a callServerMethodRequest", client.id);
                protoCallServerMathod(client, socket);
                break;

            case insertValuesRequest:
                logInfo("mars - S<--%s - received an insertValueRequest", client.id);
                protoInsertValueRequest(client, socket);
                break;

            case deleteRecordRequest:
                logInfo("mars - S<--%s - received a deleteRecordRequest", client.id);
                protoDeleteRecordRequest(client, socket);
                break;

            case deleteRecordReply:
                logInfo("mars - S<--%s - received an deleteRecordReply", client.id);
                break;

            case optUpdateReq:
                logInfo("mars - S<--%s - received an update originating from an optimistic client update", client.id);
                protoOptUpdate(client, socket);
                break;

            case subscribeReq:
                logInfo("mars - S<--%s - id:%s - received a request for subscription", client.id, socket.messageId, client.id);
                protoSubscribe(client, socket);
                break;

            default:
                logInfo("mars - S<--%s - received a message of type %s, skipping!", client.id, msgType);
                assert(false);
        }
    }

    // ... cleanup the client
    //client.wireSocket( typeof(socket).init );
}


struct MarsProxy(S)
{
    import msgpack : pack, unpack;

    struct ReceivedMessage(M) {
        bool wrongMessageReceived = false;
        int messageId;
        
        M m; alias m this;
    }

    this(S s, string ci){ this.socket = s; this.clientId = ci; }

    void sendReply(Q, A)(ReceivedMessage!Q req, A rep){
        ubyte[8] prefix = (cast(ubyte*)(&(req.messageId)))[0 .. 4] 
                                   ~ (cast(ubyte*)(&(rep.type)))[0 .. 4];
        ubyte[] packed = rep.pack!true();
        logInfo("mars - %s<--S - sending reply %s of type %s with a payload of %s bytes", clientId, req.messageId, rep.type, packed.length);
        socket.send(prefix ~ packed);
    }

    void sendRequest(A)(int messageId, A req){
        immutable(ubyte)[8] prefix = (cast(immutable(ubyte)*)(&messageId))[0 .. 4] 
                                   ~ (cast(immutable(ubyte)*)(&(req.type)))[0 .. 4];
        immutable(ubyte)[] packed = req.pack!true().idup;
        logInfo("mars - S-->%s - sending request %s of type %s with a payload of %s bytes", clientId, messageId, req.type, packed.length);
        socket.send(prefix ~ packed);
    }

    ReceivedMessage!M receiveMsg(M)(){
        auto msgType = receiveType();
        if( msgType != M.type ) return ReceivedMessage!M(true);
        auto rm = ReceivedMessage!M(false, messageId, binaryAs!M);
        return rm;
    }

    ReceivedMessage!M binaryAs(M)(){
        auto msg = binary.unpack!(M, true);
        return ReceivedMessage!M(false, messageId, binary.unpack!(M, true));
    }

    MsgType receiveType(){
        auto data = socket.receiveBinary();
        if( data.length < 8 ){
            logError("mars - S<--%s - received message as binary data from websocket, length:%d, expected at least 8; closing connection", clientId, data.length);
            return MsgType.aborting;
        }
        //logInfo("mars - S<--%s - message data:%s", clientId, data);
        messageId = * cast(int*)(data[0 .. 4].ptr);
        int msgType = * cast(int*)(data[4 .. 8].ptr);
        //logInfo("mars - message id %d of type %d", messageId, msgType);
        if( msgType < MsgType.min || msgType > MsgType.max ){
            logError("mars - S<--%s - received message of type %d, unknown; closing connection.", clientId, msgType);
            return MsgType.aborting;
        }

        binary = data[8 .. $];
        return cast(MsgType)msgType;
    }
        
    private {
        S socket;
        ubyte[] binary;
        int messageId;
        string clientId;
    }
}

struct MarsProxyStoC(S)
{
    import msgpack : pack, unpack;

    struct ReceivedMessage(M) {
        enum { success, wrongMessageReceived, channelDropped }
        int status;
        int messageId;
        
        M m; alias m this;
    }

    this(S s, string ci){ this.socket = s; this.clientId = ci; }

    void sendReply(Q, A)(ReceivedMessage!Q req, A rep){
        immutable(ubyte)[8] prefix = (cast(immutable(ubyte)*)(&(req.messageId)))[0 .. 4] 
                                   ~ (cast(immutable(ubyte)*)(&(rep.type)))[0 .. 4];
        immutable(ubyte)[] packed = rep.pack!true().idup;
        logInfo("mars - S-->%s - sending message %d of type %s with a payload of %d bytes", clientId, req.messageId, rep.type, packed.length);
        socket.send(prefix ~ packed);
    }

    /**
    Returns: true/false on success. */
    bool sendRequest(A)(int messageId, A req){
        immutable(ubyte)[8] prefix = (cast(immutable(ubyte)*)(&messageId))[0 .. 4] 
                                   ~ (cast(immutable(ubyte)*)(&(req.type)))[0 .. 4];
        immutable(ubyte)[] packed = req.pack!true().idup;
        logInfo("mars - S-->%s - sending message request %d of type %s with a payload of %d bytes", clientId, messageId, req.type, packed.length);
        try { socket.send( (prefix ~ packed).dup ); }
        catch(Exception e){
            // XXX libasync is raising a standard exception...
            if( e.msg == "The remote peer has closed the connection." || 
                e.msg == "WebSocket connection already actively closed." ||
                e.msg == "Remote hung up while writing to TCPConnection." || // vibe 0.7.31 vanilla
                e.msg == "Connection error while writing to TCPConnection.")
            {
                return false;
            }
            logInfo("mars - catched during socket.send! the exception message is '%s'! now rethrowing!", e.msg);
            throw e;
        }
        return true;
    }

    ReceivedMessage!M receiveMsg(M)(){
        auto msgType = receiveType();

        ReceivedMessage!M msg;
        if(msgType == MsgType.aborting) msg.status = msg.channelDropped;
        else if( msgType != M.type ) msg.status = msg.wrongMessageReceived;
        else {
            msg.status = msg.success;
            msg.messageId = messageId;
            msg.m =  binaryAs!M;
        }
        return msg;
    }

    ReceivedMessage!M binaryAs(M)(){
        import std.experimental.logger;
        static if( M.sizeof == 1 ){
            return ReceivedMessage!M(false, messageId, M());
        }
        else {
            auto msg = binary.unpack!(M, true);
            return ReceivedMessage!M(false, messageId, binary.unpack!(M, true));
        }
    }

    MsgType receiveType(){
        import vibe.http.websockets : WebSocketException;

        ubyte[] data;
        try {  
            data = socket.receiveBinary(); 
        }
        catch(WebSocketException e){
            logInfo("mars - S<--%s - connection closed while reading message", clientId);
            return MsgType.aborting; // XXX need a better message?
        }
        if( data.length < 8 ){
            logError("mars - S<--%s - received message as binary data from websocket, length:%d, expected at least 8; closing connection", clientId, data.length);
            return MsgType.aborting;
        }
        //logInfo("mars - S<--%s - message data:%s", clientId, data);
        messageId = * cast(int*)(data[0 .. 4].ptr);
        int msgType = * cast(int*)(data[4 .. 8].ptr);
        logInfo("mars - S<--%s - message id %d of type %d", clientId, messageId, msgType);
        if( msgType < MsgType.min || msgType > MsgType.max ){
            logError("mars - S<--%s - received message of type %d, unknown; closing connection.", clientId, msgType);
            return MsgType.aborting;
        }

        binary = data[8 .. $];
        return cast(MsgType)msgType;
    }
    
    private {
        S socket;
        ubyte[] binary;
        int messageId;
        string clientId;
    }
}
