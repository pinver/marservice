module mars.protosub;

import std.variant;
import std.experimental.logger;
import vibe.data.json;

import mars.client;
import mars.msg;



 
void protoSubscribe(S)(MarsClient* client, S socket) {
    auto state = RequestState.executed;


    auto req = socket.binaryAs!SubscribeReq;
    // XXX handle wrong parameters
    //if( req.tableIndex < 0 || req.tableIndex > marsServer.tables.length ){
    //    state = RequestState.rejectedAsWrongParameter;
    //}

    infof("===> %s ", req.parameters);

    // ... let's put the parameters that are inside the json in a variant AA
    //     in that way we are isolating the encoding, vibe json, from the rest ...
    Variant[string] parameters;
    if(req.parameters){
        auto json = parseJsonString(req.parameters);
        foreach (string name, value; json){
            if( value.type == Json.Type.string ){ parameters[name] = value.get!string; }
            // ... vibe doc says Json int is a '64bit integer value'
            else if(value.type == Json.Type.int_){ parameters[name] = value.get!long; } 
            else if(value.type == Json.Type.bool_){ parameters[name] = value.get!bool; }

            else if(value.type == Json.Type.null_){
                state = RequestState.rejectedAsWrongParameter;
                errorf("mars - C ... S - something wrong with query parameter, null json data type");
            }
            else assert(false, value.type.to!string); // XXX reply back error
        }
    }

    string stringified = "[]";
    if( state == state.init ){ // XXX 
        auto json = client.vueSubscribe(req.select, parameters, state);
        stringified = json.toString();
    }

    final switch(state) with(RequestState){
        case rejectedAsWrongParameter:
        case internalServerError:
        case rejectedAsDecodingFailed:
        case rejectedAsNotAuthorised:
        case rejectedAsPGSqlError:
            socket.sendReply(req, SubscribeRep(state, stringified));
            break;
        case executed:
            socket.sendReply(req, SubscribeRep(state, stringified));
            break;
    }

}