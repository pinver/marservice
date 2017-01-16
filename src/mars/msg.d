module mars.msg;


enum MsgType {
    authenticationRequest, authenticationReply,
    authenticateRequest, authenticateReply,
    discardAuthenticationRequest, discardAuthenticationReply,

    syncOperationRequest = 20, syncOperationReply,
    importValuesRequest  = 22, importValuesReply,
    insertValuesRequest  = 24, insertValuesReply,
    updateValuesRequest  = 26, updateValuesReply,

    welcomeBroadcast = 100, goodbyBroadcast,

    callServerMethodRequest = 150, callServerMethodReply,

    disconnectRequest = 200,
    aborting
}

struct AuthenticationRequest {
    static immutable type = MsgType.authenticationRequest;
    string username;
}

struct AuthenticationReply {
    static immutable type = MsgType.authenticationReply;
    string seed;
}

struct AuthenticateRequest {
    static immutable type = MsgType.authenticateRequest;
    string hash;
}

struct AuthenticateReply {
    static immutable type = MsgType.authenticateReply;
    int status;
    string sqlCreateDatabase;
    immutable(string)[] sqlStatements;
}

struct DiscardAuthenticationRequest {
    static immutable type = MsgType.discardAuthenticationRequest;
}

struct DiscardAuthenticationReply {
    static immutable type = MsgType.discardAuthenticationReply;
}

struct ImportValuesRequest {
    static immutable type = MsgType.importValuesRequest;
    int statementIndex;
    immutable(ubyte)[] bytes;
}

struct ImportValuesReply {
    static immutable type = MsgType.importValuesReply;
    int donno;
}

struct SyncOperationRequest {
    static immutable type = MsgType.syncOperationRequest;
    int syncOperation; // 0 = initial sync start 1 = initial sync finished
}
struct SyncOperationReply {
    static immutable type = MsgType.syncOperationReply;
    int donno;
}

struct InsertValuesRequest {
    static immutable type = MsgType.insertValuesRequest;
    int statementIndex;
    immutable(ubyte)[] bytes;
}

struct InsertValuesReply {
    static immutable type = MsgType.insertValuesReply;
    int donno;
}

struct UpdateValuesRequest {
    static immutable type = MsgType.updateValuesRequest;
    int statementIndex;
    immutable(ubyte)[] bytes;
}

struct UpdateValuesReply {
    static immutable type = MsgType.updateValuesReply;
    int donno;
}

struct CallServerMethodRequest {
    static immutable type = MsgType.callServerMethodRequest;
    string method; string parameters;
}

struct CallServerMethodReply {
    static immutable type = MsgType.callServerMethodReply;
    string returns;
}
