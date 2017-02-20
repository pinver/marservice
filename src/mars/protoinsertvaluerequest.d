module mars.protoinsertvaluerequest;


import std.conv;

import mars.client;
import mars.msg;
import mars.server : indexStatementFor;

void protoInsertValueRequest(S)(MarsClient* client, S socket)
{
    import msgpack : unpack, pack;

    auto insertValueRequest = socket.binaryAs!InsertValuesRequest;
    import std.stdio; writeln("mars - protoInsertValueRequest decoded it as :", insertValueRequest);

    int tableIndex = insertValueRequest.statementIndex;

    InsertError err;
    auto reply = client.vueInsertRecord(insertValueRequest.statementIndex, insertValueRequest.bytes, err);
    
    auto replyMsg = InsertValuesReply(
        cast(int)(err), 
        reply[0], 
        reply[1], 
        err == InsertError.inserted? indexStatementFor(tableIndex, "update").to!int : indexStatementFor(tableIndex, "delete").to!int, 
    );
    socket.sendReply(insertValueRequest, replyMsg);
}
