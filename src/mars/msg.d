module mars.msg;

enum MsgTypeStoC {
    autologinReq     = 60, autologinRep,
    syncOperationReq = 62, syncOperationRep,
    importRecordsReq = 64, importRecordsRep,
    deleteRecordsReq = 66, deleteRecordsRep,
    insertRecordsReq = 68, insertRecordsRep,
}

struct AutologinReq
{
    static immutable type = MsgTypeStoC.autologinReq;
    string username;                   /// the username that has performed the autologin.
    string sqlCreateDatabase;          /// sql statements to execute for the creation of client side tables
    immutable(string)[] sqlStatements; /// sql statements to prepare, for further operations on the tables
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
    enum { seedProvided, invalidUsername }
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
struct InsertValuesReply {
    static immutable type = MsgType.insertValuesReply;
    int insertStatus; // the insert error
    immutable(ubyte)[] bytes = []; // the server inserted record
    immutable(ubyte)[] clientKeys = [];
    int tableIndex = -1;
    int statementIndex = -1; // the sql statement to use for emending the client with the server data
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
// flowing from server to client
struct DeleteRecordReply {
    static immutable type =  MsgType.deleteRecordReply;
    int deleteStatus;
    immutable(ubyte)[] serverRecord = []; // if we can't delete the record, we must re-insert it into the client
    int tableIndex;
    int statementIndex;
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
