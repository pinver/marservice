

module mars.protomars;

import vibe.core.log;

import mars.client;
import mars.server;
import mars.msg;

import mars.protoauth;
import mars.protocallmethod;
import mars.protoinsertvaluerequest;
import mars.protodeleterecordrequest;

void protoMars(S)(MarsClient* client, S socket_)
{
    // ... we are switching to binary msgpack ...
    socket_.switchToBinaryMode();
    auto socket = MarsProxy!S(socket_, client.id);
    
    // ... client must be wired to this socket, to be able to 'broadcast' or 'push' message to the browser ...
    client.wireSocket(socket);

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

            case syncOperationReply:
                logInfo("mars - S<--%s - received a syncOperationReply", client.id);
                break;

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

            default:
                logInfo("mars - S<--%s - received a message of type %s, skipping!", client.id, msgType);
                assert(false);
        }
    }

    // ... cleanup the client
    client.wireSocket( typeof(socket).init );
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
        immutable(ubyte)[8] prefix = (cast(immutable(ubyte)*)(&(req.messageId)))[0 .. 4] 
                                   ~ (cast(immutable(ubyte)*)(&(rep.type)))[0 .. 4];
        immutable(ubyte)[] packed = rep.pack!true().idup;
        logInfo("mars - S-->%s - sending message %d of type %s with a payload of %d bytes", clientId, req.messageId, rep.type, packed.length);
        socket.send(prefix ~ packed);
    }

    void sendRequest(A)(int messageId, A req){
        immutable(ubyte)[8] prefix = (cast(immutable(ubyte)*)(&messageId))[0 .. 4] 
                                   ~ (cast(immutable(ubyte)*)(&(req.type)))[0 .. 4];
        immutable(ubyte)[] packed = req.pack!true().idup;
        logInfo("mars - S-->%s - sending message request %d of type %s with a payload of %d bytes", clientId, messageId, req.type, packed.length);
        socket.send(prefix ~ packed);
    }

    ReceivedMessage!M receiveMsg(M)(){
        auto msgType = receiveType();
        if( msgType != M.type ) return ReceivedMessage!M(true);
        auto rm = ReceivedMessage!M(false, messageId, binaryAs!M);
        return rm;
    }

    ReceivedMessage!M binaryAs(M)(){
        import std.experimental.logger;
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
        immutable(ubyte)[] binary;
        int messageId;
        string clientId;
    }
}
