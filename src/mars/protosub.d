module mars.protosub;

import mars.client;
import mars.msg;

void protoSubscribe(S)(MarsClient* client, S socket) {
    auto state = RequestState.executed;

    auto req = socket.binaryAs!SubscribeReq;
    //if( req.tableIndex < 0 || req.tableIndex > marsServer.tables.length ){
     //   state = RequestState.rejectedAsWrongParameter;
    //}

    if( state == state.init ){
        //client.vueUpdateRecord(req.tableIndex, req.keys, req.record, state);
    }

    final switch(state) with(RequestState){
        case rejectedAsWrongParameter: assert(false);
        case internalServerError:
        case rejectedAsDecodingFailed:
        case rejectedAsPGSqlError:
            socket.sendReply(req, SubscribeRep(state));
            break;
        case executed:
            socket.sendReply(req, SubscribeRep(state));
            break;
    }

}