module mars.protoinsertvaluerequest;


import mars.client;
import mars.msg;

void protoInsertValueRequest(S)(MarsClient* client, S socket)
{
    import msgpack : unpack, pack;

    auto insertValueRequest = socket.binaryAs!InsertValuesRequest;
    import std.stdio; writeln("mars - protoInsertValueRequest decoded it as :", insertValueRequest);
    auto reply = client.vueInsertRecord(insertValueRequest.statementIndex, insertValueRequest.bytes);
    
    int clientStatementToUse = insertValueRequest.statementIndex +1; // XXX refactor
    
    // ... if where was an error return statement -1, right now, to easily signal it
    if( reply[0].length == 0 && reply[1].length == 0 ) clientStatementToUse = -1;
    
    auto replyMsg = InsertValuesReply(0, reply[0], reply[1], clientStatementToUse);
    socket.sendReply(insertValueRequest, replyMsg);
}
