
module mars.client;

import std.datetime;

import vibe.core.log;

import mars.msg;
import mars.server;
import mars.websocket;
import mars.protomars : MarsProxy; // XXX per instanziare la variabile per il socket... dovrei fare un interfaccia?
                                   // nel momento in cui instanzio il client, non ho ancora il MarsProxy, che invece
                                   // instanzio nel protoMars. Dovrei instanziare il client nel protoMars? Maybe yes
                                   // visto che è li che, dopo aver stabilito che è un client mars, instanzio il socket.

import mars.sync;

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

    this(string id){ this.id_ = id; }

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

    void authoriseUser(string username) { this.username = username; }
    void discardAuthorisation() { this.username = ""; }

    bool authorised() { return this.username != ""; }
    string id() { return id_; }
    
    string callServerMethod(string method, string parameters){
        import std.stdio; writeln("client.callServerMethod(method:%s, parameters:%s)", method, parameters);
        if( serverSideMethods !is null ){
            return serverSideMethods(method, parameters);
        }
        assert(false); // catch on server side;
    }
    
    ClientSideTable*[string] tables;
    private {
        string id_;

        string username = "";
        string password;
        string seed;

        MarsProxy!Proxy socket;
        
        public typeof(MarsServer.serverSideMethods) serverSideMethods;

    }
}
