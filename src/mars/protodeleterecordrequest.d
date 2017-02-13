
module mars.protodeleterecordrequest;


import mars.client;
import mars.msg;

void protoDeleteRecordRequest(S)(MarsClient* client, S socket)
{
    import msgpack : unpack, pack;

    auto req = socket.binaryAs!DeleteRecordRequest;
    auto clientRep = client.vueDeleteRecord(req.statementIndex, req.bytes);

    auto rep = DeleteRecordReply(0);
    socket.sendReply(req, rep);
}
