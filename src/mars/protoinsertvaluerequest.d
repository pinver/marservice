module mars.protoinsertvaluerequest;


import std.conv;

import mars.client;
import mars.msg;
import mars.server : indexStatementFor;

void protoInsertValueRequest(S)(MarsClient* client, S socket)
{
    import msgpack : unpack, pack;

    auto insertValueRequest = socket.binaryAs!InsertValuesRequest;

    int tableIndex = insertValueRequest.statementIndex;

    InsertError err;
    auto reply = client.vueInsertRecord(insertValueRequest.statementIndex, insertValueRequest.bytes, err);
    
    auto replyMsg = InsertValuesReply(
        cast(int)(err), 
        reply[0], 
        reply[1], 
        tableIndex,
    );
    if( err == InsertError.inserted ){
        replyMsg.statementIndex = indexStatementFor(tableIndex, "update").to!int;
        replyMsg.statementIndex2 = indexStatementFor(tableIndex, "updateDecorations").to!int;
    }
    else {
        replyMsg.statementIndex = indexStatementFor(tableIndex, "updateDecorations").to!int;
        replyMsg.statementIndex2 = indexStatementFor(tableIndex, "delete").to!int;
    }
    socket.sendReply(insertValueRequest, replyMsg);
}
