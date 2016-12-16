

/**
  gestiamo la parte di sincronia tra server e le varie tavole associate ai client.
  */

module mars.sync;

import std.algorithm;
import std.meta;
import std.typecons;

import mars.defs;

class BaseServerSideTable(ClientT)
{
    this(immutable(Table) definition){
        this.definition = definition;
    }

    auto createClientSideTable() {
        auto cst = new ClientSideTable();
        final switch(cst.strategy) with(Strategy) {
            case easilySyncAll:
                ops ~= new ClientImportValues!ClientT();
        }
        return cst;
    }

    abstract immutable(ubyte)[] packRows(size_t offset = 0, size_t limit = size_t.max);
    abstract size_t count() const;
    abstract size_t countRowsToInsert() const;
    abstract size_t index() const;
    abstract immutable(ubyte)[] packRowsToInsert();


    immutable Table definition;   
    private {

        public SynOp!ClientT[] ops;

    }
}

class ServerSideTable(ClientT, immutable(Table) table) : BaseServerSideTable!ClientT
{
    enum Definition = table; 
    enum Columns = table.columns;
     
    alias ColumnsType = asD!Columns; /// an AliasSeq of the D types for the table columns...
    alias ColumnsStruct = asStruct!table; 
    
    this() { super(table); } 

    // interface needed to handle the records in a generic way ...

    /// returns the total number of records we are 'talking on' (filters? query?)
    override size_t count() const { return fixtures.length; }

    override size_t countRowsToInsert() const { return toInsert.length; }

    /// return the unique index identifier for this table, that's coming from the table definition in the app.d
    override size_t index() const { return Definition.index; }

    /// returns 'limit' rows starting from 'offset'.
    auto selectRows(size_t offset = 0, size_t limit = size_t.max) const {
        size_t till  = (limit + offset) > count ? count : (limit + offset);
        return fixtures[offset .. till];
    }

    /// insert a new row in the server table, turning client table out of sync
    auto insertRow(ColumnsStruct fixture){
        fixtures ~= fixture;
        toInsert ~= fixture;
        ops ~= new ClientInsertValues!ClientT();
    }

    /// returns the packet selected rows
    override immutable(ubyte)[] packRows(size_t offset = 0, size_t limit = size_t.max) const {
        import msgpack : pack;
        return pack!(true)(selectRows(offset, limit)).idup;
    }

    /// return the packet rows to insert in the client
    override immutable(ubyte)[] packRowsToInsert() {
        import msgpack : pack;
        auto packed = pack!(true)(toInsert).idup;
        toInsert = [];
        return packed;
    }

    void loadFixture(ColumnsStruct fixture){
        fixtures ~= fixture;
    }

    asStruct!(table)[] fixtures;
    asStruct!(table)[] toInsert;
}

struct ClientSideTable
{
    private {
        Strategy strategy = Strategy.easilySyncAll;
    }
}

private
{
    enum Strategy { easilySyncAll }

    class SynOp(MarsClientT) {
        abstract void execute(MarsClientT marsClient, ClientSideTable* cst, BaseServerSideTable!MarsClientT sst);
    }

    /// take all the rows in the server table and send them on the client table.
    class ClientImportValues(MarsClientT) : SynOp!MarsClientT {
        
        override void execute(MarsClientT marsClient, ClientSideTable* cst, BaseServerSideTable!MarsClientT sst)
        {
            import mars.msg : ImportValuesRequest;
            import std.conv : to;

            // ... if the table is empty, simply do nothing ...
            if( sst.count > 0 ){
                auto payload = sst.packRows();

                auto req = ImportValuesRequest();
                req.statementIndex = sst.index.to!int;
                req.bytes = payload;
                marsClient.sendRequest(req);
            }
        }
    }

    class ClientInsertValues(MarsClientT) : SynOp!MarsClientT {
        
        override void execute(MarsClientT marsClient, ClientSideTable* cst, BaseServerSideTable!MarsClientT sst)
        {
            import mars.msg : InsertValuesRequest;
            import std.conv : to;
            
            if( sst.countRowsToInsert > 0 ){
                auto payload = sst.packRowsToInsert();
                auto req = InsertValuesRequest();
                req.statementIndex = sst.index.to!int;
                req.bytes = payload;
                marsClient.sendRequest(req);
            }
        }
    }
}

unittest
{
    import std.range : zip;

    struct MarsClientMock { void sendRequest(R)(R r){} }
    
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
}

