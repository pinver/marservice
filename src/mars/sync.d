

/**
  gestiamo la parte di sincronia tra server e le varie tavole associate ai client.


  */
module mars.sync;

enum d =
`
BaseServerSideTable
    clientSideTables[clientId] --- SyncOps[]
`;

import std.algorithm;
import std.conv;
import std.datetime;
import std.format;
import std.meta;
import std.typecons;
import std.variant;

import std.experimental.logger;

import msgpack;

import mars.defs;
import mars.pgsql;
import mars.msg;
import mars.server : indexStatementFor;
version(unittest) import mars.starwars;

alias Bytes = immutable(ubyte)[];

/**
A server side table is instantiated only once per marsServer, and they are stored into the 'tables'
structure of the marsServer.

The instantiation usually is inside the application code: `InstantiateTables!(ctTables)(marsServer, [], [], [], [])`
*/
class BaseServerSideTable(ClientT)
{
    alias ClientType = ClientT;

    this(immutable(Table) definition){
        this.definition = definition;
    }

    auto createClientSideTable(string clientid){
        auto cst = new ClientSideTable!ClientT();
        cst.strategy = definition.cacheRows? Strategy.easilySyncAll : Strategy.easilySyncNone;
        final switch(cst.strategy) with(Strategy) {
            case easilySyncAll:
                cst.ops ~= new ClientImportValues!ClientT();
                break;
            case easilySyncNone:
                break;
        }
        // new client, new client side table
        assert( (clientid in clientSideTables) is null, format("cliendid:%s, clientSideTables:%s",
                    clientid, clientSideTables.keys() ) );
        clientSideTables[clientid] = cst;

        return cst;
    }

    auto wipeClientSideTable(string clientid){
        assert( (clientid in clientSideTables) !is null, clientid );
        clientSideTables.remove(clientid);
    }

    /// execute a sql select statement, and returns a vibe json array with the records as json
    auto selectAsJson(Database db, string sqlSelect, Variant[string] parameters) in {
        assert(db !is null);
    } body {
        import std.math : isNaN;
        import vibe.data.json;

        auto resultSet = db.executeQueryUnsafe(sqlSelect, parameters);
        scope(exit) resultSet.close();

        Json jsonRecords = Json.emptyArray;
        foreach(variantRow; resultSet){
            Json jsonRow = Json.emptyArray;
            foreach(i, variantField; variantRow){
                if(variantField.type == typeid(int)){ jsonRow ~= variantField.get!int; }
                else if(variantField.type == typeid(float)){ jsonRow ~= variantField.get!float; }
                else if(variantField.type == typeid(long)){ jsonRow ~= variantField.get!long; }
                else if(variantField.type == typeid(string)){ jsonRow ~= variantField.get!string; }
                else if(variantField.type == typeid(ubyte[])){
                    // ... if I apply directly the '=~', the arrays are flattened!
                    jsonRow ~= 0;
                    jsonRow[jsonRow.length-1] = variantField.get!(ubyte[]).serializeToJson;
                }
                else {
                    import std.stdio; writeln(variantField.type);
                    assert(false);
                }
            }
            jsonRecords ~= 0; jsonRecords[jsonRecords.length-1] = jsonRow;
        }
        return jsonRecords;
    }
    version(unittest_starwars){
        unittest {
            AuthoriseError err; auto db = DatabaseService("127.0.0.1", 5432, "starwars").connect("jedi", "force", err);
            auto table = new WhiteHole!BaseServerSideTable(Table());
            auto json = table.selectAsJson(db, "select * from people", null);
            //import std.stdio; writeln(json.toPrettyString());
            assert(json[0][0] == "Luke");
        }
        unittest {
            import std.math : isNaN;
            import vibe.data.json : Json;

            AuthoriseError err; auto db = DatabaseService("127.0.0.1", 5432, "starwars").connect("jedi", "force", err);
            auto table = new WhiteHole!BaseServerSideTable(Table());
            auto json = table.selectAsJson(db, "select * from starships", null);
            assert(json[0][0] == "NanShip");
            // here we have a BIG problem: NaN is not encodable in Json! The value is 'null'!
            // assert(json[0][1].get!float().isNaN);
        }
    }

    abstract Bytes packRows(size_t offset = 0, size_t limit = long.max);
    abstract Bytes packRows(Database db, size_t offset = 0, size_t limit = long.max);
    //abstract size_t count() const;
    abstract size_t count(Database) const;
    abstract size_t countRowsToInsert() const;
    abstract size_t countRowsToUpdate() const;
    abstract size_t countRowsToDelete() const;
    abstract size_t index() const;
    abstract Bytes packRowsToInsert();
    abstract Bytes packRowsToUpdate();
    abstract Bytes packRowsToDelete();

    abstract Bytes[2] insertRecord(Database, immutable(ubyte)[], ref InsertError, string, string);
    abstract void updateRecord(Database, Bytes, immutable(ubyte)[], ref RequestState);
    abstract Bytes    deleteRecord(Database, immutable(ubyte)[], ref DeleteError, string, string);

    abstract void unsafeReset();

    immutable Table definition; 
    private {

        /// Every server table has a collection of the linked client side tables. The key element is the identifier of
        /// the client, so that the collection can be kept clean when a client connect/disconnects.
        public ClientSideTable!(ClientT)*[string] clientSideTables;
        
        //public SynOp!ClientT[] ops;

    }
}

class ServerSideTable(ClientT, immutable(Table) table) : BaseServerSideTable!ClientT
{
    enum Definition = table; 
    enum Columns = table.columns;
     
    alias ColumnsType = asD!Columns; /// an AliasSeq of the D types for the table columns...
    alias ColumnsStruct = asStruct!table; 
    alias KeysStruct = asPkStruct!table;

    this() { super(table); } 

    // interface needed to handle the records in a generic way ...

    /// returns the total number of records we are 'talking on' (filters? query?)
    //deprecated override size_t count() const { return fixtures.length; }
    override size_t count(Database db) const {
        import ddb.postgres : ServerErrorException;

        static if( table.durable ){
            try {
                return db.executeScalarUnsafe!size_t("select count(*) from %s".format(table.name));
            }
            catch(ServerErrorException e){
                switch(e.code){
                    case "42501": // permission denied for relation "relation"
                        infof("Permission denied:%s", e.toString());
                        return 0;
                    default:
                        warningf("mars - x ... x - Unhandled PostgreSQL exception during 'count'");
                        info(e.toString());
                        throw e;
                }
            }
        }
        else {
            return fixtures.length;
        }
    }
    //static if( ! table.durable ){ // XXX
        override size_t countRowsToInsert() const { return toInsert.length; }
        override size_t countRowsToUpdate() const { return toUpdate.length; }
        override size_t countRowsToDelete() const { return toDelete.length; }
    //}

    /// return the unique index identifier for this table, that's coming from the table definition in the app.d
    override size_t index() const { return Definition.index; }

    /// returns 'limit' rows starting from 'offset'.
    deprecated auto selectRows(size_t offset = 0, size_t limit = long.max) const  {
        size_t till  = (limit + offset) > count(null) ? count(null) : (limit + offset);
        return fixtures.values()[offset .. till];
    }
    /// returns 'limit' rows starting from 'offset'.
    auto selectRows(Database db, size_t offset = 0, size_t limit = long.max) const {
        static if(table.durable){
            auto resultSet = db.executeQueryUnsafe!(asStruct!table)("select * from %s limit %d offset %d".format(
                table.name, limit, offset)
            );
            static if( Definition.decorateRows ){
                asSyncStruct!table[] rows;
                foreach(vr; resultSet){
                    asStruct!table v = vr;
                    asSyncStruct!table r;
                    assignCommonFields!(typeof(r), typeof(v))(r, v);
                    r.mars_who = "automation@server";
                    r.mars_what = "imported";
                    r.mars_when = Clock.currTime.toString(); 
                    rows ~= r;
                }
            }
            else {
                asStruct!table[] rows;
                foreach(v; resultSet){
                    rows ~= v;
                }
            }
            
            
            resultSet.close();
            return rows;
        }
        else {
            size_t till  = (limit + offset) > count(db) ? count(db) : (limit + offset);
            return fixtures.values()[offset .. till];
        }
    }

    /// insert a new row in the server table, turning clients table out of sync
    deprecated void insertRow(ColumnsStruct fixture){
        KeysStruct keys = pkValues!(table)(fixture);
        fixtures[keys] = fixture;
        static if(table.decorateRows){
            asSyncStruct!table rec;
            assignCommonFields(rec, fixture);
            with(rec){ mars_who = "automation@server"; mars_what = "inserted"; mars_when = Clock.currTime.toString(); }
        }
        else auto rec = fixture;
        toInsert[keys] = rec;
        foreach(ref cst; clientSideTables.values){
            cst.ops ~= new ClientInsertValues!ClientT();
        }
    }

    /// insert a new row in the server table, turning clients table out of sync
    ColumnsStruct insertRecord(Database db, ColumnsStruct record, ref InsertError err, string username,string clientid){
        KeysStruct keys = pkValues!table(record);
        static if(table.durable){
            auto inserted = db.executeInsert!(table, ColumnsStruct)(record, err);
        } else {
            fixtures[keys] = record;
            auto inserted = record;
            err = InsertError.inserted;
        }
        if( err == InsertError.inserted ){
            static if(table.decorateRows){
                asSyncStruct!table rec;
                assignCommonFields(rec, record);
                with(rec){
                    mars_who = username ~ "@" ~ clientid; mars_what = "inserted";
                    mars_when = Clock.currTime.toString();
                }
            }
            else {
                auto rec = record;
            }
            toInsert[keys] = rec;
            // ... don't propagate if not cached, or we are triggering a loot of refresh
            //     stick with manual refresh done on client.
            if(table.cacheRows){
                foreach(ref cst; clientSideTables.values){
                    cst.ops ~= new ClientInsertValues!ClientT();
                }
            }
        }
        return inserted;
    }

    override Bytes[2]
    insertRecord(Database db, Bytes data, ref InsertError err, string username, string clientId){
        import  msgpack : pack, unpack, MessagePackException;
        ColumnsStruct record;
        try {
            record = unpack!(ColumnsStruct, true)(data);
        }
        catch(MessagePackException exc){
            errorf("mars - failed to unpack record to insert in '%s': maybe a wrong type of data in js", table.name);
            errorf(exc.toString);
            err = InsertError.unknownError;
            return [[], []];
        }
        ColumnsStruct inserted = insertRecord(db, record, err, username, clientId);
        return [
            inserted.pack!(true).idup,
            record.pkParamValues!table().pack!(true).idup // clientKeys
        ];
    }

    override void updateRecord(Database db, Bytes encodedKeys, Bytes encodedRecord, ref RequestState state){
        asPkStruct!table keys;
        ColumnsStruct record;
        try { 
            keys = unpack!(asPkStruct!table, true)(encodedKeys); 
            record = unpack!(ColumnsStruct, true)(encodedRecord);
        }
        catch(MessagePackException exc){
            errorf("mars - failed to unpack keys for record to update '%s': maybe a wrong type in js", table.name);
            errorf(exc.toString);
            state = RequestState.rejectedAsDecodingFailed;
            return;
        }
        updateRecord(db, keys, record, state);
    }

    void updateRecord(Database db, asPkStruct!table keys, ColumnsStruct record, ref RequestState state){
        static if(table.durable){
            db.executeUpdate!(table, asPkStruct!table, ColumnsStruct)(keys, record, state);
        }
        else {  }
        if( state == RequestState.executed ){
            static if(table.decorateRows){
                asSyncStruct!table rec;
                assignCommonFields(rec, record);
                with(rec){
                    mars_who = /+username+/"qualeutente" ~ "@" ~ /+clientid+/ "qualeclient";
                    mars_what = "updated";
                    mars_when = Clock.currTime.toString();
                }
            }
            else {
                auto rec = record;
            }
            toUpdate[keys] = rec;
            if(table.cacheRows){
                foreach(ref cst; clientSideTables.values){
                    cst.ops ~= new ClientUpdateValues!ClientT();
                }
            }
        }
    }

    override Bytes deleteRecord(Database db, Bytes data, ref DeleteError err, string username, string clientid){
        import msgpack : pack, unpack, MessagePackException;
        asPkParamStruct!table keys;
        try {
            keys = unpack!(asPkParamStruct!table, true)(data);
        }
        catch(MessagePackException exc){
            errorf("mars - failed to unpack keys for record to delete '%s': maybe a wrong type in js", table.name);
            errorf(exc.toString);
            err = DeleteError.unknownError;
            return data;
        }
        deleteRecord(db, keys, err, username, clientid);
        if( err != DeleteError.deleted ) return data;
        return [];
    }

    asPkParamStruct!table
    deleteRecord(Database db, asPkParamStruct!table keys, ref DeleteError err, string username, string clientid){
        KeysStruct k;
        assignFields(k, keys);
        static if(table.durable){
            db.executeDelete!(table, asPkParamStruct!table)(keys, err);
        }
        else {
            fixtures.remove(k);
            err = DeleteError.deleted;
        }
        if( err == DeleteError.deleted ){
            static if(table.decorateRows){
                toDelete[k] = Sync(username ~ "@" ~ clientid, "deleted", Clock.currTime.toString());
            }
            else {
                toDelete[k] = 0;
            }
            foreach(ref cst; clientSideTables.values){
                cst.ops ~= new ClientDeleteValues!ClientT();
            }
        }
        return keys;
    }

    /// update row in the server table, turning the client tables out of sync
    deprecated void updateRow(KeysStruct keys, ColumnsStruct record){
        //KeysStruct keys = pkValues!table(record);
        auto v = keys in toInsert;
        if( v !is null ){
            static if(table.decorateRows){
                asSyncStruct!table rec;
                assignCommonFields(rec, record);
                with(rec){ mars_who = "who@where"; mars_what = "updated"; mars_when = Clock.currTime.toString(); }
            }
            else {
                auto rec = record;
            }
            *v = rec;
            assert( (keys in toUpdate) is null );
        }
        else {
            auto v2 = keys in toUpdate;
            if( v2 !is null ){
                static if(table.decorateRows){ assert(false); }
                else { *v2 = record; }
            }
            else {
                static if(table.decorateRows){ assert(false); }
                else { toUpdate[keys] = record; }
            }
        }
        fixtures[keys] = record;
        foreach(ref cst; clientSideTables.values){
            cst.ops ~= new ClientUpdateValues!ClientT();
        }
    }

    /// update row in the server table, turning the client tables out of sync
    void updateRow(Database db, KeysStruct keys, ColumnsStruct record){
        static if( table.durable ){
            import msgpack : pack;

            RequestState state;
            db.executeUpdate!(table, KeysStruct, ColumnsStruct)(keys, record, state);
            auto v = keys in toInsert;
            if( v !is null ){
                static if(table.decorateRows){
                    asSyncStruct!table rec;
                    assignCommonFields(rec, record);
                    with(rec){ mars_who = "who@where"; mars_what = "updated"; mars_when = Clock.currTime.toString(); }
                }
                else {
                    auto rec = record;
                }
                *v = rec;
                assert( (keys in toUpdate) is null );
            }
            else {
                auto v2 = keys in toUpdate;
                if( v2 !is null ){
                    static if(table.decorateRows){ assert(false); }
                    else { *v2 = record; }
                }
                else {
                    static if(table.decorateRows){ assert(false); }
                    else { toUpdate[keys] = record; }
                }
            }
        }
        else {
            //KeysStruct keys = pkValues!table(record);
            auto v = keys in toInsert;
            if( v !is null ){
                static if(table.decorateRows){
                    asSyncStruct!table rec;
                    assignCommonFields(rec, record);
                    with(rec){ mars_who = "who@where"; mars_what = "updated"; mars_when = Clock.currTime.toString(); }
                }
                else {
                    auto rec = record;
                }
                *v = record;
                assert( (keys in toUpdate) is null );
            }
            else {
                v = keys in toUpdate;
                if( v !is null ){
                    *v = record;
                }
                else {
                    toUpdate[keys] = record;
                }
            }
            fixtures[keys] = record;
        }
        foreach(ref cst; clientSideTables.values){
            cst.ops ~= new ClientUpdateValues!ClientT();
        }
    }

    /// returns the packet selected rows
    override Bytes packRows(size_t offset = 0, size_t limit = long.max) const {
        import msgpack : pack;
        return pack!(true)(selectRows(null, offset, limit)).idup;
    }
    /// returns the packet selected rows
    override Bytes packRows(Database db, size_t offset = 0, size_t limit = long.max) const {
        import msgpack : pack;
        return pack!(true)(selectRows(db, offset, limit)).idup;
    }

    /// return the packet rows to insert in the client
    override Bytes packRowsToInsert() {
        import msgpack : pack;
        auto packed = pack!(true)(toInsert.values()).idup;
        //toInsert = null; can't reset... this is called for every client
        return packed;
    }

    /// return the packet rows to delete in the client
    override Bytes packRowsToDelete() {
        import msgpack : pack;
        asSyncPkParamStruct!(table)[] whereKeys;
        foreach(key; toDelete.keys()){
            import std.stdio; writeln("+++++++++++++>>>>", key, ":", toDelete[key]);
            asSyncPkParamStruct!table whereKey;
            assignFields(whereKey, key);
            static if(table.decorateRows) assignCommonFields(whereKey, toDelete[key]);
            whereKeys ~= whereKey;
            writeln("<<<=== M<<<<<<<<<<", whereKey);
        }
        auto packed = pack!(true)(whereKeys).idup;
        //toInsert = null; can't reset... this is called for every client
        return packed;
    }

    /// return the packet rows to update in the client
    override Bytes packRowsToUpdate() {
        static struct UpdateRecord {
            KeysStruct keys;
            static if(table.decorateRows){
                asSyncStruct!table record;
            }
            else {
                asStruct!table record;
            }
        }
        UpdateRecord[] records;
        foreach(r; toUpdate.keys){
            records ~= UpdateRecord(r, toUpdate[r]);
        }

        import msgpack : pack;
        auto packed = pack!(true)(records).idup;
        //toUpdate = null; can't reset... this is called for every client
        return packed;
    }

    void loadFixture(ColumnsStruct fixture){
        KeysStruct keys = pkValues!table(fixture);
        fixtures[keys] = fixture;
    }

    override void unsafeReset() {
        //fixtures = null;
        toInsert = null;
        toUpdate = null;
        toDelete = null;
    }

    //static if( ! table.durable ){
        asStruct!(table)[asPkStruct!(table)] fixtures;
        static if(table.decorateRows){
            asSyncStruct!(table)[asPkStruct!(table)] toInsert;
            Sync[asPkStruct!(table)] toDelete;
            asSyncStruct!(table)[asPkStruct!(table)] toUpdate;
        }
        else {
            asStruct!(table)[asPkStruct!(table)] toInsert;
            int[asPkStruct!(table)] toDelete;
            asStruct!(table)[asPkStruct!(table)] toUpdate;
        }

        // ... record inserted client side, already patched and inserted for this client.
        //asStruct!(table)[string] notToInsert;
    //}
}


struct ClientSideTable(ClientT)
{
    private {
        Strategy strategy = Strategy.easilySyncNone;
        public SynOp!ClientT[] ops;
    }
}

private
{
    enum Strategy { easilySyncAll, easilySyncNone }

    class SynOp(MarsClientT) {
        abstract void execute(MarsClientT, ClientSideTable!(MarsClientT)*, BaseServerSideTable!MarsClientT);
        abstract void execute(Database, MarsClientT, ClientSideTable!(MarsClientT)*, BaseServerSideTable!MarsClientT);
    }

    /// take all the rows in the server table and send them on the client table.
    class ClientImportValues(MarsClientT) : SynOp!MarsClientT {

        override void execute(
            Database db, MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst
        ){
            assert(db !is null);

            // ... if the table is empty, simply do nothing ...
            if( sst.count(db) > 0 ){
                auto payload = sst.packRows(db);

                auto req = ImportRecordsReq(); with(req){
                    tableIndex = sst.index;
                    statementIndex = indexStatementFor(sst.index, "insert");
                    encodedRecords = payload;
                }
                marsClient.sendRequest(req);
                if(marsClient.isConnected) auto rep = marsClient.receiveReply!ImportRecordsRep();
            }
        }
        
        override
        void execute(MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst)
        {
            import mars.msg : ImportValuesRequest;
            import std.conv : to;

            // ... if the table is empty, simply do nothing ...
            if( sst.count(null) > 0 ){
                auto payload = sst.packRows();

                auto req = ImportRecordsReq();  with(req){
                    tableIndex =sst.index;
                    statementIndex = indexStatementFor(sst.index, "insert");
                    encodedRecords = payload;
                }
                marsClient.sendRequest(req);
                if(marsClient.isConnected) auto rep = marsClient.receiveReply!ImportRecordsRep();
            }
        }
    }

    class ClientInsertValues(MarsClientT) : SynOp!MarsClientT {

        override
        void execute(MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst)
        {
            if( sst.countRowsToInsert > 0 ){
                auto payload = sst.packRowsToInsert();
                auto req = InsertRecordsReq(); with(req){
                    tableIndex = sst.index;
                    statementIndex = indexStatementFor(sst.index, "insert");
                    encodedRecords = payload;
                }
                marsClient.sendRequest(req);
                if(marsClient.isConnected) auto rep = marsClient.receiveReply!InsertRecordsRep();
            }
        }
        override
        void execute(
            Database db, MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst
        ){
            if( sst.countRowsToInsert > 0 ){
                auto payload = sst.packRowsToInsert();
                auto req = InsertRecordsReq(); with(req){
                    tableIndex = sst.index;
                    statementIndex = indexStatementFor(sst.index, "insert");
                    encodedRecords = payload;
                }
                marsClient.sendRequest(req);
                if(marsClient.isConnected) auto rep = marsClient.receiveReply!InsertRecordsRep();
            }
        }
    }

    class ClientDeleteValues(MarsClientT) : SynOp!MarsClientT {

        override
        void execute(MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst)
        {
            if( sst.countRowsToDelete > 0 ){
                auto payload = sst.packRowsToDelete();
                auto req = DeleteRecordsReq(); with(req){
                    tableIndex = sst.index;
                    statementIndex = indexStatementFor(sst.index, "delete").to!int;
                    encodedRecords = payload;
                }
                marsClient.sendRequest(req);
                if(marsClient.isConnected) auto rep = marsClient.receiveReply!DeleteRecordsRep();
            }
        }
        override
        void execute(
            Database db, MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst
        ){
            if( sst.countRowsToDelete > 0 ){
                auto payload = sst.packRowsToDelete();
                auto req = DeleteRecordsReq(); with(req){
                    tableIndex = sst.index;
                    statementIndex = indexStatementFor(sst.index, "delete").to!int;
                    encodedRecords = payload;
                }
                marsClient.sendRequest(req);
                if(marsClient.isConnected) auto rep = marsClient.receiveReply!DeleteRecordsRep();
            }
        }
    }

    class ClientUpdateValues(MarsClientT) : SynOp!MarsClientT {

        override
        void execute(MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst)
        {
            import mars.msg : UpdateValuesRequest;
            import std.conv :to;

            if( sst.countRowsToUpdate > 0 ){
                auto payload = sst.packRowsToUpdate();
                auto req = UpdateValuesRequest();
                req.statementIndex = indexStatementFor(sst.index, "update").to!int;
                req.bytes = payload;
                marsClient.sendRequest(req);
            }
        }

        override void execute(Database db, MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst,
                BaseServerSideTable!MarsClientT sst)
        {
            import mars.msg : UpdateValuesRequest;
            import std.conv :to;

            if( sst.countRowsToUpdate > 0 ){
                auto payload = sst.packRowsToUpdate();
                auto req = UpdateValuesRequest();
                req.statementIndex = indexStatementFor(sst.index, "update").to!int;
                req.bytes = payload;
                marsClient.sendRequest(req);
            }

        }
    }

    class ServerUpdateValues(MarsClientT) : SynOp!MarsClientT {
        override void execute(Database db, MarsClientT marsClient, ClientSideTable* cst,
                BaseServerSideTable!MarsClientT sst){}
    }
}

version(unittest)
{
    struct MarsClientMock { void sendRequest(R)(R r){} }
}
unittest
{
    /+
    import std.range : zip;

    
    auto t1 = immutable(Table)("t1", [Col("c1", Type.integer, false), Col("c2", Type.text, false)], [0], []);
    auto sst = new ServerSideTable!(MarsClientMock, t1);
    zip([1, 2, 3], ["a", "b", "c"]).each!( f => sst.loadFixture(sst.ColumnsStruct(f.expand)) );
    
    auto cst = sst.createClientSideTable();
    // ... la strategia più semplice è syncronizzare subito TUTTO il contenuto nella client side ...
    assert( cst.strategy == Strategy.easilySyncAll );
    // ... e a questo punto, come minimo deve partire un comando di import di tutti i dati....
    assert( cast(ClientImportValues!MarsClientMock)(sst.ops[$-1]) !is null );
    // ... che eseguito si occupa di gestire il socket, e aggiornare client e server side instances.
    auto op = sst.ops[$-1];
    op.execute(MarsClientMock(), cst, sst);

    // ...posso aggiornare uno dei valori con update, in questo caso la primary key è la colonna c1
    sst.updateRow(sst.KeysStruct(2), sst.ColumnsStruct(2, "z"));
    assert( sst.fixtures[sst.KeysStruct(2)] == sst.ColumnsStruct(2, "z") );
    +/
}
/+
unittest
{
    version(unittest_starwars){
        import mars.starwars;
        enum schema = starwarsSchema();

        auto people = new ServerSideTable!(MarsClientMock, schema.tables[0]);
        auto scores = new ServerSideTable!(MarsClientMock, schema.tables[3]);
        auto databaseService = DatabaseService("127.0.0.1", 5432, "starwars");
        AuthoriseError err;
        auto db = databaseService.connect("jedi", "force", err);
        db.executeUnsafe("begin transaction");
        
        auto rows = people.selectRows(db);
        assert( rows[0] == luke, rows[0].to!string );

        auto paolo = Person("Paolo", "male", [0x00, 0x01, 0x02, 0x03, 0x04], 1.80);
        InsertError ierr;
        auto inserted = people.insertRecord(db, paolo, ierr);
        assert(inserted == paolo);
        

        //import std.stdio;
        //foreach(row; rows) writeln("---->>>>>", row);
        //assert(false);
    }
}
+/

