/**
 * the `sync` module operate on a generic database, to support mock. The module is feeded with a concrete db
 * like the `pgsql`.
 */

module mars.server;

import std.experimental.logger;

import mars.client;
import mars.defs;

import mars.sync;
import mars.pgsql;
import mars.msg;

import vibe.core.log;
import vibe.data.json;

void InstantiateTables(alias tables, F...)(MarsServer m, F fixtures) {
    static assert( tables.length > 0 && fixtures.length > 0, "every table must have at least an empty fixtures array"); 
    InstantiateServerSideTable!(tables[0], typeof(fixtures[0]))(m, fixtures[0]);
    static if( tables.length > 1 ){
        InstantiateTables!(tables[1 .. $])(m, fixtures[1 .. $]);
    }
}

void InstantiateServerSideTable(immutable(Table) table, Fixtures)(MarsServer m, Fixtures fixtures){
    auto serverSideTable = new ServerSideTable!(MarsClient*, table)();
    m.tables ~= serverSideTable;
  
    static if( ! is(Fixtures == void[]) ){  // ... no fixtures, empty array of void ...
        foreach(fixture; fixtures){
            serverSideTable.loadFixture(serverSideTable.ColumnsStruct(fixture.expand));
        }
    }
}


auto serverSideTable(immutable(Table) table)(MarsServer m){
    foreach( t; m.tables ){
        if( t.definition.name == table.name ){
            auto c = cast(ServerSideTable!(MarsClient*, table))(t);
            return c;
        }
    }
    assert(false);
}

class MarsServer
{

    MarsClient* engageClient(string clientId)
    {
        import std.experimental.logger; trace("clients? %s", marsClients.keys());
        auto client = clientId in marsClients;
        if( client is null ){
            marsClients[clientId] = MarsClient(clientId, configuration.databaseService);
            client = clientId in marsClients;

            // ... the tables that are exposed in the schema ...
            foreach(ref table; tables){
                /*client.tables[table.definition.name] =*/ table.createClientSideTable(clientId);
            }
            //logInfo("mars - created %d client side tables", client.tables.length);
            // XXX questo è da ripensare con un meccanismo generico
            client.serverSideMethods = this.serverSideMethods;
        }
        assert( client !is null );
        client.connected();
        return client;
    }

    void disposeClient(MarsClient* client) in { 
        assert(client !is null); assert(client.id in marsClients); 
    } body {
        client.disconnected();
    }

    // The mars protocol has completed the handshake and setup, request can be sent and received.
    /// Called by 'protomars'  module.
    void onMarsProtocolReady(MarsClient* client){

        //... connect to the DB if autologin is enabled server side
        if( configuration.pgsqlUser != "" ){
            bool dbAuthorised = client.authoriseUser(configuration.pgsqlUser, configuration.pgsqlPassword);
            if( ! dbAuthorised ) throw new Exception("Server autologin enabled, but can't authorise with psql");
            auto reply = AuthenticateReply(0);
            reply.sqlCreateDatabase = configuration.alasqlCreateDatabase;
            reply.sqlStatements = configuration.alasqlStatements;
            logInfo("S --> C | authenticate reply, autologin for %s", configuration.pgsqlUser);
            client.sendBroadcast(reply);
        }
        startDatabaseHandler();
    }


    void broadcast(M)(M message)
    {
        import std.experimental.logger : trace;

        //trace("tracing...");
        foreach(clientId, marsClient; marsClients)
        {
        
        //trace("tracing...");
        if( marsClient.isConnected() ) marsClient.sendBroadcast(message);
        //trace("tracing...");
        }
    }

    string delegate(MarsClient, string, Json) serverSideMethods;

    // ================
    this(immutable(MarsServerConfiguration) c){
        assert(marsServer is null);
        marsServer = this;
        configuration = c;
    }



    static MarsServerConfiguration ExposeSchema(immutable(Schema) schema)
    {
        import mars.alasql : createDatabase, insertIntoParameter, updateParameter;
        
        immutable(string)[] statements;
        foreach(table; schema.tables){
            statements ~= table.insertIntoParameter;
            statements ~= table.updateParameter;
        }
        return MarsServerConfiguration( schema, createDatabase(schema), statements );    
    }

    MarsServerConfiguration configuration;

    private void startDatabaseHandler(){

        import vibe.core.core : runTask;
        import vibe.core.log : logInfo;
        
        logInfo("mars - database handler starting.");
        
        foreach(t; tables){
            logInfo("mars - exposing table %s to clients", t);
        }
        runTask(&handleDatabase);
    }


    /**
    gestisci le cose se succedono a livello db, come push per i client mars */
    void handleDatabase()
    {
        import std.algorithm : sort;
        import std.datetime : seconds;
        import vibe.core.core : sleep;
        import vibe.core.log : logInfo;

        while(true) {
            sleep(2.seconds);

            foreach(ref client; marsClients ){
               if( client.isConnected && client.authorised && client.db !is null ){
                   bool syncStarted = false;
                   //logInfo("mars - database operations for client %s", client.id);
                   auto req = SyncOperationRequest();
                   req.syncOperation = 0;
                   foreach( table; tables ){
                       auto clientTable = table.clientSideTables[client.id];
                       //logInfo("mars - database operations for client %s table %s", client.id, table.definition.name);
                       foreach(op; clientTable.ops){
                           if( ! syncStarted ){
                               syncStarted = true; 
                               client.sendRequest(req);
                           }
                           //logInfo("mars - executing database operation for client %s", client.id);
                           op.execute(client.db, &client, clientTable, table);
                       }
                       clientTable.ops = []; // XXX gestisci le singole failure...
                   }
                   if( syncStarted ){
                       req.syncOperation = 1;
                       client.sendRequest(req);

                   }
               } 
            }
        }
    }

    private {
        MarsClient[string] marsClients;
    public    BaseServerSideTable!(MarsClient*)[] tables;
    }

}
__gshared MarsServer marsServer;

struct MarsServerConfiguration
{
    immutable(Schema) schemaExposed;
    string alasqlCreateDatabase;
    immutable(string)[] alasqlStatements;
    immutable string[] serverMethods;

    
    immutable DatabaseService databaseService;

    string pgsqlUser, pgsqlPassword; // for autologin
}

static MarsServerConfiguration ExposeServerMethods(MarsServerConfiguration c, const string[] methods){
    return MarsServerConfiguration(c.schemaExposed, c.alasqlCreateDatabase, c.alasqlStatements, methods.idup,
            c.databaseService, c.pgsqlUser, c.pgsqlPassword);
}

MarsServerConfiguration PostgreSQL(MarsServerConfiguration c, const string host, const ushort port, const string db){
    return MarsServerConfiguration(c.schemaExposed, c.alasqlCreateDatabase, c.alasqlStatements, c.serverMethods,
            DatabaseService(host, port, db), c.pgsqlUser, c.pgsqlPassword);
}

MarsServerConfiguration Autologin(MarsServerConfiguration c, const string login, const string password){
    return MarsServerConfiguration(c.schemaExposed, c.alasqlCreateDatabase, c.alasqlStatements, c.serverMethods, c.databaseService,
        login, password);
}
