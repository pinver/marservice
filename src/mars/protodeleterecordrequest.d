
module mars.protodeleterecordrequest;

import std.conv;

import mars.client;
import mars.msg;
import mars.server : indexStatementFor;

void protoDeleteRecordRequest(S)(MarsClient* client, S socket)
{
    import msgpack : unpack, pack;

    auto req = socket.binaryAs!DeleteRecordRequest;
    int tableIndex = req.statementIndex;

    DeleteError err;
    auto deleted = client.vueDeleteRecord(req.statementIndex, req.bytes, err);

    auto rep = DeleteRecordReply(
        cast(int)err,
        deleted,

        tableIndex,
        indexStatementFor(tableIndex, "delete").to!int
        );
    socket.sendReply(req, rep);
}
