

/**
  gestiamo la parte di sincronia tra server e le varie tavole associate ai client.
  */

module mars.sync;

import std.algorithm;
import std.meta;
import std.typecons;
import std.experimental.logger;
import std.format;

import mars.defs;
import mars.pgsql;

class BaseServerSideTable(ClientT)
{
    alias ClientType = ClientT;

    this(immutable(Table) definition){
        this.definition = definition;
    }

    auto createClientSideTable(string clientid){
        auto cst = new ClientSideTable!ClientT();
        final switch(cst.strategy) with(Strategy) {
            case easilySyncAll:
                cst.ops ~= new ClientImportValues!ClientT();
        }
        // new client, new client side table
        assert( (clientid in clientSideTables) is null );
        clientSideTables[clientid] = cst;

        return cst;
    }

    abstract immutable(ubyte)[] packRows(size_t offset = 0, size_t limit = long.max);
    abstract immutable(ubyte)[] packRows(Database db, size_t offset = 0, size_t limit = long.max);
    abstract size_t count() const;
    abstract size_t count(Database) const;
    abstract size_t countRowsToInsert() const;
    abstract size_t countRowsToUpdate() const;
    abstract size_t index() const;
    abstract immutable(ubyte)[] packRowsToInsert();
    abstract immutable(ubyte)[] packRowsToUpdate();

    abstract immutable(ubyte)[][2] insertRecord(Database, immutable(ubyte)[]);
    abstract immutable(ubyte)[]    deleteRecord(Database, immutable(ubyte)[]);

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
    deprecated override size_t count() const { return fixtures.length; }
    override size_t count(Database db) const {
        static if( table.durable ){
            return db.executeScalarUnsafe!size_t("select count(*) from %s".format(table.name));
        }
        else {
            return fixtures.length;
        }
    }
    //static if( ! table.durable ){ // XXX
        override size_t countRowsToInsert() const { return toInsert.length; }
        override size_t countRowsToUpdate() const { return toUpdate.length; }
    //}

    /// return the unique index identifier for this table, that's coming from the table definition in the app.d
    override size_t index() const { return Definition.index; }

    /// returns 'limit' rows starting from 'offset'.
    deprecated auto selectRows(size_t offset = 0, size_t limit = long.max) const  {
        size_t till  = (limit + offset) > count ? count : (limit + offset);
        return fixtures.values()[offset .. till];
    }
    /// returns 'limit' rows starting from 'offset'.
    auto selectRows(Database db, size_t offset = 0, size_t limit = long.max) const {
        static if(table.durable){
            auto resultSet = db.executeQueryUnsafe!(asStruct!table)("select * from %s limit %d offset %d".format(
                table.name, limit, offset)
            );
            asStruct!table[] rows;
            foreach(v; resultSet){
                rows ~= v;
                //import std.stdio; writeln("selectRows:", v);
                // XXX
            }
            resultSet.close();
            return rows;
        }
        else {
            size_t till  = (limit + offset) > count ? count : (limit + offset);
            return fixtures.values()[offset .. till];
        }
    }

    /// insert a new row in the server table, turning clients table out of sync
    deprecated void insertRow(ColumnsStruct fixture){
        KeysStruct keys = pkValues!table(fixture);
        fixtures[keys] = fixture;
        toInsert[keys] = fixture;
        foreach(ref cst; clientSideTables.values){
            cst.ops ~= new ClientInsertValues!ClientT();
        }
    }

    /// insert a new row in the server table, turning clients table out of sync
    ColumnsStruct insertRecord(Database db, ColumnsStruct record){
        static if(table.durable){
            auto inserted = db.executeInsert!(table, ColumnsStruct)(record);
            KeysStruct keys = pkValues!table(record);
            toInsert[keys] = record;
        }
        else {
            auto inserted = record;
            KeysStruct keys = pkValues!table(record);
            fixtures[keys] = record;
            toInsert[keys] = record;
        }
        foreach(ref cst; clientSideTables.values){
            cst.ops ~= new ClientInsertValues!ClientT();
        }
        return inserted;
    }

    override immutable(ubyte)[][2] insertRecord(Database db, immutable(ubyte)[] data){
        import  msgpack : pack, unpack, MessagePackException;
        ColumnsStruct record;
        try {
            record = unpack!(ColumnsStruct, true)(data);
        }
        catch(MessagePackException exc){
            errorf("mars - failed to unpack record to insert in '%s': maybe a wrong type of data in js", table.name);
            errorf(exc.toString);
            return [[], []];
        }
        ColumnsStruct inserted = insertRecord(db, record);
        return [inserted.pack!(true).idup, inserted.pkValues!table().pack!(true).idup];
    }

    override immutable(ubyte)[] deleteRecord(Database db, immutable(ubyte)[] data){
        import msgpack : pack, unpack;
        asStruct!table record = unpack!(ColumnsStruct, true)(data);
        KeysStruct keys = record.pkValues!table();
        deleteRecord(db, keys);
        return [];
    }

    immutable(ubyte)[] deleteRecord(Database db, KeysStruct keys){
        static if(table.durable){
            db.executeDelete!(table, KeysStruct)(keys);
        }
        else {
            fixtures.remove(keys);
            //toDelete[keys] = 0;
        }
        return [];
    }

    /// update row in the server table, turning the client tables out of sync
    deprecated void updateRow(KeysStruct keys, ColumnsStruct record){
        //KeysStruct keys = pkValues!table(record);
        auto v = keys in toInsert;
        if( v !is null ){ 
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
        foreach(ref cst; clientSideTables.values){
            cst.ops ~= new ClientUpdateValues!ClientT();
        }
    }

    /// update row in the server table, turning the client tables out of sync
    void updateRow(Database db, KeysStruct keys, ColumnsStruct record){
        static if( table.durable ){
            import msgpack : pack;

            db.executeUpdate!(table, KeysStruct, ColumnsStruct)(keys, record);
            auto v = keys in toInsert;
            if( v !is null ){ 
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
        }
        else {
            //KeysStruct keys = pkValues!table(record);
            auto v = keys in toInsert;
            if( v !is null ){ 
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
    override immutable(ubyte)[] packRows(size_t offset = 0, size_t limit = long.max) const {
        import msgpack : pack;
        return pack!(true)(selectRows(offset, limit)).idup;
    }
    /// returns the packet selected rows
    override immutable(ubyte)[] packRows(Database db, size_t offset = 0, size_t limit = long.max) const {
        import msgpack : pack;
        return pack!(true)(selectRows(db, offset, limit)).idup;
    }

    /// return the packet rows to insert in the client
    override immutable(ubyte)[] packRowsToInsert() {
        import msgpack : pack;
        auto packed = pack!(true)(toInsert.values()).idup;
        toInsert = null;
        return packed;
    }

    /// return the packet rows to update in the client
    override immutable(ubyte)[] packRowsToUpdate() {
        static struct UpdateRecord {
            KeysStruct keys;
            asStruct!table record;
        }
        UpdateRecord[] records;
        foreach(r; toUpdate.keys){
            records ~= UpdateRecord(r, toUpdate[r]);
        }

        import msgpack : pack;
        auto packed = pack!(true)(records).idup;
        toUpdate = null;
        return packed;
    }

    void loadFixture(ColumnsStruct fixture){
        KeysStruct keys = pkValues!table(fixture);
        fixtures[keys] = fixture;
    }

    void unsafeReset() {
        fixtures = null;
        toInsert = null;
        toUpdate = null;
    }

    //static if( ! table.durable ){
        asStruct!(table)[asPkStruct!(table)] fixtures;
        asStruct!(table)[asPkStruct!(table)] toInsert;
        asStruct!(table)[asPkStruct!(table)] toUpdate;
        asStruct!(table)[asPkStruct!(table)] toDelete;
    //}
}


struct ClientSideTable(ClientT)
{
    private {
        Strategy strategy = Strategy.easilySyncAll;
        public SynOp!ClientT[] ops;
    }
}

private
{
    enum Strategy { easilySyncAll }

    class SynOp(MarsClientT) {
        abstract void execute(MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst);
        abstract void execute(Database db, MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst);
    }

    /// take all the rows in the server table and send them on the client table.
    class ClientImportValues(MarsClientT) : SynOp!MarsClientT {

        override void execute(Database db, MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst)
        {
            assert(db !is null);

            import mars.msg : ImportValuesRequest;
            import std.conv : to;

            // ... if the table is empty, simply do nothing ...
            if( sst.count(db) > 0 ){
                auto payload = sst.packRows(db);

                auto req = ImportValuesRequest();
                req.statementIndex = sst.index.to!int *2;
                req.bytes = payload;
                marsClient.sendRequest(req);
            }
        }
        override void execute(MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst)
        {
            import mars.msg : ImportValuesRequest;
            import std.conv : to;

            // ... if the table is empty, simply do nothing ...
            if( sst.count > 0 ){
                auto payload = sst.packRows();

                auto req = ImportValuesRequest();
                req.statementIndex = sst.index.to!int *2;
                req.bytes = payload;
                marsClient.sendRequest(req);
            }
        }
    }

    class ClientInsertValues(MarsClientT) : SynOp!MarsClientT {
        
        override void execute(MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst)
        {
            import mars.msg : InsertValuesRequest;
            import std.conv : to;
            
            if( sst.countRowsToInsert > 0 ){
                auto payload = sst.packRowsToInsert();
                auto req = InsertValuesRequest();
                req.statementIndex = sst.index.to!int *2;
                req.bytes = payload;
                marsClient.sendRequest(req);
            }
        }
        override void execute(Database db, MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst){}
    }

    class ClientUpdateValues(MarsClientT) : SynOp!MarsClientT {

        override void execute(MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst)
        {
            import mars.msg : UpdateValuesRequest;
            import std.conv :to;

            if( sst.countRowsToUpdate > 0 ){
                auto payload = sst.packRowsToUpdate();
                auto req = UpdateValuesRequest();
                req.statementIndex = sst.index.to!int *2 +1;
                req.bytes = payload;
                marsClient.sendRequest(req);
            }
        }
        override void execute(Database db, MarsClientT marsClient, ClientSideTable!(MarsClientT)* cst, BaseServerSideTable!MarsClientT sst){}
    }

    class ServerUpdateValues(MarsClientT) : SynOp!MarsClientT {
        override void execute(Database db, MarsClientT marsClient, ClientSideTable* cst, BaseServerSideTable!MarsClientT sst){
            
        }
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
unittest
{
    version(starwars){
        import mars.starwars;
        enum schema = starwarsSchema();

        auto people = new ServerSideTable!(MarsClientMock, schema.tables[0]);
        auto scores = new ServerSideTable!(MarsClientMock, schema.tables[3]);
        auto databaseService = DatabaseService("127.0.0.1", 5432, "starwars");
        auto db = databaseService.connect("jedi", "force");
        db.executeUnsafe("begin transaction");
        
        auto rows = people.selectRows(db);
        assert( rows[0] == luke );

        auto paolo = Person("Paolo", "male", [0x00, 0x01, 0x02, 0x03, 0x04], 1.80);
        auto inserted = people.insertRecord(db, paolo);
        assert(inserted == paolo);
        

        //import std.stdio;
        //foreach(row; rows) writeln("---->>>>>", row);
        //assert(false);
    }
}


