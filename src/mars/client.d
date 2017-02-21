
module mars.client;

import std.datetime,
       std.experimental.logger;

import vibe.core.log;
import vibe.data.json;

import mars.msg;
import mars.server;
import mars.websocket;
import mars.protomars : MarsProxy; // XXX per instanziare la variabile per il socket... dovrei fare un interfaccia?
                                   // nel momento in cui instanzio il client, non ho ancora il MarsProxy, che invece
                                   // instanzio nel protoMars. Dovrei instanziare il client nel protoMars? Maybe yes
                                   // visto che è li che, dopo aver stabilito che è un client mars, instanzio il socket.

import mars.sync;
import mars.pgsql;

struct MarsClient
{
    void connected() in {
        assert( connectionEvents.length ==0 || connectionEvents[$-1].type == ConnessionEvent.disconnected );
    } body {
        connectionEvents ~= ConnessionEvent(ConnessionEvent.connected, Clock.currTime);
    }
    void disconnected() in {
        assert( connectionEvents[$-1].type == ConnessionEvent.connected );
    } body {
        connectionEvents ~= ConnessionEvent(ConnessionEvent.disconnected, Clock.currTime);
    }

    bool isConnected(){ return connectionEvents.length >0 && connectionEvents[$-1].type == ConnessionEvent.connected; }
    
    SysTime[] reconnections() {
        import std.algorithm : filter, map;
        import std.array : array;

        return connectionEvents
            .filter!( (e) => e.type == ConnessionEvent.connected )
            .map!"a.when"
            .array;
    }
    
    private {
        struct ConnessionEvent {
            enum { connected, disconnected }
            int type;
            SysTime when;
        }
        ConnessionEvent[] connectionEvents;
    }

    this(string id, const DatabaseService databaseService){ 
        this.id_ = id; 
        this.databaseService = databaseService;
    }

    /**
     * Push a forward-only message to the client, a reply is not expected. */
    void sendBroadcast(M)(M msg) in { assert(isConnected); } body 
    {
        socket.sendRequest(0, msg); // ... this is really a proxy to the websocket
    }

    /**
     * Push a new message, from the server to the client. Used by the server to inform clients about events. */
    void sendRequest(M)(M msg) in { assert(isConnected); } body 
    {
        socket.sendRequest(nextId++, msg);
    }
    private int nextId = 1;

    /**
     * The Helo protocol will wire the active socket here, and will set this to null when disconnecting. */
    void wireSocket(MarsProxy!Proxy socket)
    {
        this.socket = socket;
    }

    /**
     * Called by the authentication protocol.
     * 
     * Returns: false if PostgreSQL is offline or user in not authorised, or true. */
    AuthoriseError authoriseUser(string username, string pgpassword) in {
        assert(username && pgpassword);
    } body {
        this.username = username;

        AuthoriseError err;
        if( databaseService.host == "" ){
            logWarn("S --- C | the database host is not specified, we are operating in offline mode");
            err = AuthoriseError.authorised;
        }
        else {
            db = databaseService.connect(username, pgpassword, err );
        }
        return err;
    }
    void discardAuthorisation() { this.username = ""; }

    bool authorised() { return this.username != ""; }
    string id() { return id_; }
    
    string callServerMethod(string method, Json parameters){
        if( serverSideMethods !is null ){
            return serverSideMethods(this, method, parameters);
        }
        assert(false); // catch on server side;
    }

    immutable(ubyte)[][2] vueInsertRecord(int statementIndex, immutable(ubyte)[] record, ref InsertError err){
        immutable(ubyte)[][2] inserted = marsServer.tables[statementIndex].insertRecord(db, record, err);
        return inserted;
    }

    immutable(ubyte)[] vueDeleteRecord(int tableIndex, immutable(ubyte)[] record, ref DeleteError err){
        immutable(ubyte)[] deleted = marsServer.tables[tableIndex].deleteRecord(db, record, err);
        return deleted;
    }

    //ClientSideTable!(typeof(this)*)*[string] tables;
    private {
        string id_;

        string username = "";
        //string password;
        string seed;

        MarsProxy!Proxy socket;
        
        public typeof(MarsServer.serverSideMethods) serverSideMethods;
        DatabaseService databaseService;
        public Database db;
    }
}
