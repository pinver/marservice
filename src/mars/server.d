/**
 * the `sync` module operate on a generic database, to support mock. The module is feeded with a concrete db
 * like the `pgsql`.
 */

module mars.server;

import std.algorithm.iteration, std.array, std.conv, std.format;
      
       

import std.experimental.logger;

import mars.client;
import mars.defs;

import mars.sync;
import mars.pgsql;
import mars.msg;
version(unittest) import mars.starwars;

import vibe.core.log;
import vibe.data.json;
import vibe.core.task;

void InstantiateTables(alias tables, F...)(MarsServer m, F fixtures) {
    static assert( tables.length > 0 && fixtures.length > 0, "every table must have at least an empty fixtures array"); 
    InstantiateServerSideTable!(tables[0], typeof(fixtures[0]))(m, fixtures[0]);
    static if( tables.length > 1 ){
        InstantiateTables!(tables[1 .. $])(m, fixtures[1 .. $]);
    }
}
unittest {
    MarsServer marsServer_ = new MarsServer(MarsServerConfiguration());
    enum ctTables = starwarsSchema.tables[0 .. 2];
    InstantiateTables!(ctTables)(marsServer_, [], []);
}

private void InstantiateServerSideTable(immutable(Table) table, Fixtures)(MarsServer m, Fixtures fixtures){
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
    /// Register the client between the connected clients
    MarsClient* engageClient(string clientId)
    {
        auto client = clientId in marsClients;
        if( client is null ){
            logInfo("mars S - %s - this is a new client.", clientId);
            marsClients[clientId] = MarsClient(clientId, configuration.databaseService);
            client = clientId in marsClients;
        }
        else {
            // ... the client was already engaged, for safety, wipe the client side tables ...
            logInfo("mars S - %s - this is a reconnection, wiping out the client side tables.", clientId);
            foreach(ref table; tables){
                table.wipeClientSideTable(client.id);
            }

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

    /// Create the server side tables for the client, preparing the initial sync op.
    /// Callee: protoauth.
    void createClientSideTablesFor(MarsClient* client){
        // ... the tables that are exposed in the schema ...
        foreach(ref table; tables){
            table.createClientSideTable(client.id);
        }
        //logInfo("mars - created %d client side tables", client.tables.length);
        // XXX questo è da ripensare con un meccanismo generico
        client.serverSideMethods = this.serverSideMethods;
    }

    void wipeClientSideTablesFor(MarsClient* client) {
        foreach(ref table; tables){
            table.wipeClientSideTable(client.id);
        }
    }

    // The mars protocol has completed the handshake and setup, request can be sent and received.
    /// Called by 'protomars'  module.
    void onMarsProtocolReady(MarsClient* client){

        //... connect to the DB if autologin is enabled server side
        if( configuration.pgsqlUser != "" ){
            auto dbAuthorised = client.authoriseUser(configuration.pgsqlUser, configuration.pgsqlPassword);
            if( dbAuthorised != AuthoriseError.authorised ) 
                throw new Exception("Server autologin enabled, but can't authorise with postgreSQL");
            auto request = AutologinReq(); with(request){
                username = configuration.pgsqlUser;
                sqlCreateDatabase = configuration.alasqlCreateDatabase;
                sqlStatements = configuration.alasqlStatements;
                jsStatements = configuration.jsStatements;
            }
            logInfo("mars - S --> C | autologin request, autologin for %s", configuration.pgsqlUser);
            client.sendRequest(request);
            createClientSideTablesFor(client);
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

    /**
    Returns: a pointer to the mars client with that id, or null if it does not exists. */
    MarsClient* getClient(string clientId){ return clientId in marsClients; }

    string delegate(MarsClient, string, Json) serverSideMethods;

    // ================
    this(immutable(MarsServerConfiguration) c){
        assert(marsServer is null);
        marsServer = this;
        configuration = c;
    }


    static MarsServerConfiguration ExposeSchema(immutable(Schema) schema)
    {
        import mars.alasql : createDatabase, selectFrom, selectFromWhere, insertIntoParameter, updateParameter,
               deleteFromParameter, updateDecorationsParameter, pkValuesJs, pkValuesWhereJs;
        
        immutable(string)[] statements;
        immutable(string)[] jsStatements;
  
        jsStatements ~= jsIndexStatementFor;
        jsStatements ~= `[%s]`.format(schema.tables.map!((t) => t.decorateRows.to!string).join(", ").array);
        jsStatements ~= `[%s]`.format(schema.tables.map!((t) => t.cacheRows.to!string).join(", ").array);
        foreach(table; schema.tables){
            statements ~= table.insertIntoParameter;           // 'insert'
            statements ~= table.updateParameter;               // 'update'
            statements ~= table.deleteFromParameter;           // 'delete'
            statements ~= table.updateDecorationsParameter;    // 'updateDecorations'
            statements ~= table.selectFrom;
            statements ~= table.selectFromWhere;               // 'selectFromWhere'
            // update the 'indexStatementFor' below in this module...
            jsStatements ~= table.pkValuesJs;
            jsStatements ~= table.pkValuesWhereJs;
        }
        return MarsServerConfiguration( schema, createDatabase(schema), statements, jsStatements );
    }

    MarsServerConfiguration configuration;

    private void startDatabaseHandler(){

        import vibe.core.core : runTask;
        import vibe.core.log : logInfo;
        
        logInfo("mars - database handler starting.");
        
        //foreach(t; tables){ logInfo("mars - exposing table %s to clients", t); }
        if( databaseHandler == Task.init ) databaseHandler = runTask(&handleDatabase);
    }
    Task databaseHandler;


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
            logInfo("mars - database handler starting to check for sync...%s", Task.getThis());

            clientLoop: foreach(ref client; marsClients ){
               if( client.isConnected && client.authorised ){
                   bool syncStarted = false;
                   logInfo("mars - database operations for client %s", client.id);
                   auto req = SyncOperationReq(SyncOperationReq.SyncOperation.starting);
                   foreach( table; tables ){
                        if(client.id in table.clientSideTables)
                        {
                            auto clientTable = table.clientSideTables[client.id];
                            //logInfo("mars - database operations for client %s table %s", client.id, table.definition.name);
                            foreach(op; clientTable.ops){
                                if( ! syncStarted ){
                                    syncStarted = true; 
                                    client.sendRequest(req);
                                    logInfo("mars - database operations for client %s sync started", client.id);
                                    if( ! client.isConnected ){
                                        logInfo("mars - the client %s seems disconnected, continuing with another client", client.id);
                                        continue clientLoop;
                                    }
                                }
                                logInfo("mars - executing database operation for client %s", client.id);
                                op.execute(client.db, &client, clientTable, table);
                                if( ! client.isConnected ){
                                    logInfo("mars - the client %s seems disconnected after some operation, continuing with another client", client.id);
                                        continue clientLoop;
                                }
                            }
                            clientTable.ops = []; // XXX gestisci le singole failure...
                        }
                   }
                   if( syncStarted ){
                       req.operation = SyncOperationReq.SyncOperation.completed;
                       client.sendRequest(req);
                       logInfo("mars - database operations for client %s sync completed", client.id);
                   }
               } 
            }
            foreach( table; tables ){ table.unsafeReset(); }
        }
    }

    private {
        MarsClient[string] marsClients;
    public    BaseServerSideTable!(MarsClient*)[] tables;
    }

}
__gshared MarsServer marsServer;

// adjust the same function in mars.ts server
ulong indexStatementFor(ulong table, string op){
    enum ops = 6; // XXX
    if      (op == "insert"){ return table * ops + 0; }
    else  if(op == "update"){ return table * ops + 1; }
    else  if(op == "delete"){ return table * ops + 2; }
    else  if(op == "updateDecorations"){ return table * ops + 3; }
    else  if(op == "select"){ return table * ops + 4; }
    else  if(op == "selectFromWhere"){ return table * ops + 5; }
    assert(false, "unknown ops!");
}
enum jsIndexStatementFor = `(
function a(table, op)
{
    const ops = 6;
    if      (op == "insert"){ return table * ops + 0; }
    else  if(op == "update"){ return table * ops + 1; }
    else  if(op == "delete"){ return table * ops + 2; }
    else  if(op == "updateDecorations"){ return table * ops + 3; }
    else  if(op == "select"){ return table * ops + 4; }
    else  if(op == "selectFromWhere"){ return table * ops + 5; }
    alert("unknown ops!");
})
`;

struct MarsServerConfiguration
{
    immutable(Schema) schemaExposed;
    string alasqlCreateDatabase;
    immutable(string)[] alasqlStatements;
    immutable(string)[] jsStatements;
    immutable string[] serverMethods;

    
    immutable DatabaseService databaseService;

    string pgsqlUser, pgsqlPassword; // for autologin
}

static MarsServerConfiguration ExposeServerMethods(MarsServerConfiguration c, const string[] methods){
    return MarsServerConfiguration(c.schemaExposed, c.alasqlCreateDatabase, c.alasqlStatements, c.jsStatements,
            methods.idup, c.databaseService, c.pgsqlUser, c.pgsqlPassword);
}

MarsServerConfiguration PostgreSQL(MarsServerConfiguration c, const string host, const ushort port, const string db){
    return MarsServerConfiguration(c.schemaExposed, c.alasqlCreateDatabase, c.alasqlStatements, c.jsStatements,
            c.serverMethods, DatabaseService(host, port, db), c.pgsqlUser, c.pgsqlPassword);
}

MarsServerConfiguration Autologin(MarsServerConfiguration c, const string login, const string password){
    return MarsServerConfiguration(c.schemaExposed, c.alasqlCreateDatabase, c.alasqlStatements, c.jsStatements,
            c.serverMethods, c.databaseService, login, password);
}
