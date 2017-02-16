

module mars.protocallmethod;

import mars.client;
import mars.msg;

import vibe.data.json;
import msgpack;

void protoCallServerMathod(S)(MarsClient* client, S socket)
{
    import msgpack : unpack, pack;

    auto callServerMethodRequest = socket.binaryAs!CallServerMethodRequest;
    import std.stdio; writeln("mars - protoCallServerMathod decoded it as :", callServerMethodRequest);
    string jsonString = cast(string)(callServerMethodRequest.parameters);
    writeln("string:", jsonString);
    Json parameters = jsonString.parseJsonString;
    string reply = client.callServerMethod(callServerMethodRequest.method, parameters);

    auto replyMsg = CallServerMethodReply(reply);
    socket.sendReply(callServerMethodRequest, replyMsg);
}
