

module mars.protoupd;

import mars.client;
import mars.msg;
import mars.server;

/**
Handle a request for update coming form the client, originating from on optimistic update.

Params:
    client = the mars client, the sender.
    socket = socket like object, as a transport.
*/
void protoOptUpdate(S)(MarsClient* client, S socket) 
{
    auto state = RequestState.executed;

    auto req = socket.binaryAs!OptUpdateReq;
    if( req.tableIndex < 0 || req.tableIndex > marsServer.tables.length ){
        state = RequestState.rejectedAsWrongParameter;
    }

    if( state == state.init ){
        client.vueUpdateRecord(req.tableIndex, req.keys, req.record, state);
    }

    final switch(state) with(RequestState){
        case rejectedAsWrongParameter: assert(false);
        case internalServerError:
        case rejectedAsDecodingFailed:
        case rejectedAsNotAuthorised:
        case rejectedAsPGSqlError:
            socket.sendReply(req, OptUpdateRep(state));
            break;
        case executed:
            socket.sendReply(req, OptUpdateRep(state));
            break;
    }
}