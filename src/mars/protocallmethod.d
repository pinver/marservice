

module mars.protocallmethod;

import mars.client;
import mars.msg;

void protoCallServerMathod(S)(MarsClient* client, S socket)
{
    import msgpack : unpack, pack;

    auto callServerMethodRequest = socket.binaryAs!CallServerMethodRequest;
    import std.stdio; writeln("mars - protoCallServerMathod decoded it as :", callServerMethodRequest);
    string reply = client.callServerMethod(callServerMethodRequest.method, callServerMethodRequest.parameters);

    auto replyMsg = CallServerMethodReply(reply);
    socket.sendReply(callServerMethodRequest, replyMsg);
}
