module mars.msg;

enum RequestState 
{
    executed,

    // ... client side bugs or tampering of the request
    rejectedAsDecodingFailed, /// the deconding of the data is failed
    rejectedAsWrongParameter, /// the value of one of the request parameters is wrong.

    rejectedAsNotAuthorised,  /// the client is not authorised for the request (ex, subscription before of authentication)
    rejectedAsForeignKeyViolation, /// update or delete on table violates foreign key constraint on another table (ERR 23503)

    rejectedAsPGSqlError,     /// PostgreSQL unhandled error
    internalServerError,
}
enum MsgTypeStoC {
    autologinReq     = 60, autologinRep,
    syncOperationReq = 62, syncOperationRep,
    importRecordsReq = 64, importRecordsRep,
    deleteRecordsReq = 66, deleteRecordsRep,
    insertRecordsReq = 68, insertRecordsRep,
    updateRecordsReq = 70, updateRecordsRep,
    pingReq          = 72, pingRep,
    
    aborting = 201
}



struct AutologinReq
{
    static immutable type = MsgTypeStoC.autologinReq;
    string username;                   /// the username that has performed the autologin.
    string sqlCreateDatabase;          /// sql statements to execute for the creation of client side tables
    immutable(string)[] sqlStatements; /// sql statements to prepare, for further operations on the tables
    immutable(string)[] jsStatements;  /// javascript statements to eval, like constraints, key extraction, etc.
}

struct DeleteRecordsReq 
{
    static immutable type = MsgTypeStoC.deleteRecordsReq;
    ulong tableIndex;
    ulong statementIndex;
    immutable(ubyte)[] encodedRecords = []; 
}

struct DeleteRecordsRep
{
    static immutable type =  MsgTypeStoC.deleteRecordsRep;
}

struct InsertRecordsReq
{
    static immutable type = MsgTypeStoC.insertRecordsReq;
    ulong tableIndex;
    ulong statementIndex;
    immutable(ubyte)[] encodedRecords;
}

struct InsertRecordsRep
{
    static immutable type = MsgTypeStoC.insertRecordsRep;
}

struct ImportRecordsReq {
    static immutable type = MsgTypeStoC.importRecordsReq;
    ulong tableIndex;
    ulong statementIndex;
    immutable(ubyte)[] encodedRecords;
}

struct PingReq {
    static immutable type = MsgTypeStoC.pingReq;
}

struct PingRep {
    static immutable type = MsgTypeStoC.pingRep;
}

struct UpdateRecordsReq {
    static immutable type = MsgTypeStoC.updateRecordsReq;
    ulong tableIndex;
    immutable(ubyte)[] encodedRecords;
}

struct ImportRecordsRep {
    static immutable type = MsgTypeStoC.importRecordsRep;
}

struct SyncOperationReq
{
    enum SyncOperation { starting, completed }

    static immutable type = MsgTypeStoC.syncOperationReq;
    SyncOperation operation; 
}

struct SyncOperationReply
{
    static immutable type = MsgTypeStoC.syncOperationRep;
}

// ----

enum MsgType {
    authenticationRequest, authenticationReply,
    authenticateRequest, authenticateReply,
    discardAuthenticationRequest, discardAuthenticationReply,

    importValuesRequest  = 22, importValuesReply,
    insertValuesRequest  = 24, insertValuesReply,
    updateValuesRequest  = 26, updateValuesReply,
    deleteRecordRequest  = 28, deleteRecordReply,

    optUpdateReq = 50, optUpdateRep = 51, // request the server to perform an update and confirm an optimistic one
    subscribeReq = 52, subscribeRep = 52, // request to subscribe to a query
    pingReq      = 54,                    // request to keep alive the connection


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
    enum { seedProvided, 
        invalidUsername, 
        alreadyAuthorised, /// one user is already logged in, and authorised.
    }
    static immutable type = MsgType.authenticationReply;
    int status;
    string seed;
}

struct AuthenticateRequest {
    static immutable type = MsgType.authenticateRequest;
    string hash;
}

/// Warning, this enum is checked in the client also!
enum AuthoriseError {
    assertCheck,

    authorised,              
    databaseOffline,         /// the database is offline, so can't autorise
    wrongUsernameOrPassword, /// password authentication failed for user "user"
    unknownError,            /// unknown or not handled error code.
}
struct AuthenticateReply {
    static immutable type = MsgType.authenticateReply;
    int status;
    string sqlCreateDatabase;
    immutable(string)[] sqlStatements;
    immutable(string)[] jsStatements;  /// javascript statements to eval, like constraints, key extraction, etc.
}

struct DiscardAuthenticationRequest {
    static immutable type = MsgType.discardAuthenticationRequest;
}

struct DiscardAuthenticationReply {
    static immutable type = MsgType.discardAuthenticationReply;
}

// 
struct SubscribeReq 
{
    static immutable type = MsgType.subscribeReq;
    string select;
    string parameters;
}

struct SubscribeRep 
{
    static immutable type = MsgType.subscribeRep;
    RequestState state;
    string json;
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



// S --> C in the op
struct InsertValuesRequest {
    static immutable type = MsgType.insertValuesRequest;
    int statementIndex;
    immutable(ubyte)[] bytes;
}

enum InsertError {
    assertCheck,
    inserted,
    duplicateKeyViolations,
    unknownError,
}
// sent from server to client validating or rejecting the optimistic update
struct InsertValuesReply {
    static immutable type = MsgType.insertValuesReply;
    int insertStatus; // the insert error
    immutable(ubyte)[] bytes = []; // the server inserted record
    immutable(ubyte)[] clientKeys = [];
    int tableIndex = -1;
    int statementIndex = -1; // the sql statement to use for emending the client with the server data
    int statementIndex2 = -1; // idem. For example, on errors, the first one is used to update the deco, then delete
}

struct DeleteRecordRequest {
    static immutable type = MsgType.deleteRecordRequest;
    int statementIndex;
    immutable(ubyte)[] bytes = []; 
}

enum DeleteError {
    assertCheck,
    deleted,
    unknownError,
}

// the reply is flowing from server to client
struct DeleteRecordReply {
    static immutable type =  MsgType.deleteRecordReply;
    int deleteStatus;
    immutable(ubyte)[] serverRecord = []; // if we can't delete the record, we must re-insert it into the client
    int tableIndex;
    int statementIndex;
}

// request an update of a record to the server, that the client has optimistically updated
struct OptUpdateReq 
{
    static immutable type = MsgType.optUpdateReq;
    ulong tableIndex;           /// the index identifier of the updated table.
    immutable(ubyte)[] keys;    /// the primary keys of the record to update
    immutable(ubyte)[] record;  /// the new values for that record
}

struct OptUpdateRep 
{
    static immutable type = MsgType.optUpdateRep;
    RequestState state;
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
    string method; immutable(ubyte)[] parameters;
}

struct CallServerMethodReply {
    static immutable type = MsgType.callServerMethodReply;
    string returns;
}
