module mars.sync2;
/+
import std.datetime,
       std.experimental.logger;

import mars.defs;

alias Bytes = immutable(ubyte)[];

struct DeltaOperation {
    Operation operation;
    Bytes key;
    Bytes record;
    string by;
    SysTime when;
}
enum Operation { Ins, Committed, Upd }
alias Delta = DeltaOperation[];

struct Row {
    enum {
        updated, inserted, optUpdated, optInserted
    }
    Bytes keys;
    Bytes record;

    int state;
    SysTime when;
    string by;
}

unittest {

    enum k1 = [0]; enum r1 = [0, 1, 2, 3];
    enum k2 = [4]; enum r2 = [4, 5, 6, 7];
                   enum r3 = [8, 9, 0, 1];

    auto t0 = SysTime(DateTime(2017, 01, 01, 0, 0));
    auto t1 = SysTime(DateTime(2017, 01, 01, 1, 0));
    auto t2 = SysTime(DateTime(2017, 01, 01, 2, 0));

    auto db = new MockDatabase();
    auto sst = ServerSideTable(db);
    auto bob = ClientSideTable("Bob");
    auto alice = ClientSideTable("Alice");

    sst.insertRow(r1, t0);
    assert(bob.count == 0 || alice.count == 0);

    auto delta = sst.syncDeltaFor(bob);
    bob.applyDelta(delta);

    auto row = bob.row(k1);
    assert(row.record == r1 && row.by == "Auto" && row.when == t0 && row.state == Row.inserted);

    // client, arrivano una serie di operazioni da fare. Devo tornare una sequenza di cose da fare lato client.
    bob.updateRow(k1, r3, t1);
    bob.insertRow(k2, r2, t1); // XXX per togliere k2, devo dare a bob un db? naaaa..... o si? e se
    //                            facessi un interfaccia minore per fare questi lavori sui record?
    //                            valuta una terzo oggetto, che si occupa del pack!() del record
    assert(bob.row(k1).state == Row.optUpdated && bob.row(k2).state == Row.optInserted);

    delta = bob.commitOrRollback(sst);
    // noi ci siamo aggiornati, il server ha fatto, non si torna indietro.
    assert(sst.row(k1).record == r3 && sst.row(k2).record == r2);
    assert(bob.row(k1).state == Row.updated && bob.row(k2).state == Row.inserted);
    // ... aggiorniamo alice
    delta = sst.syncDeltaFor(alice);
    alice.applyDelta(delta);
    assert(alice.row(k1).record == r3 && sst.row(k1).by == "Bob");
    
    // ... update a row server side
    sst.updateRow(k1, r1, t2);
    delta = sst.syncDeltaFor(bob);
    bob.applyDelta(delta);
    assert(bob.row(k1).record == r1 && bob.row(k1).by == "Auto");

}

unittest
{
    import mars.swar;
    auto schema = starwarSchema();
    auto db = new MockDatabase2!(schema);

    auto dbPeople = db.table!"people";
    dbPeople.insertRow("luke", "male");
    dbPeople.insertRow("leila", "female");

    auto sst = ServerSideTable(db);
    auto bob = ClientSideTable("Bob");
    auto alice = ClientSideTable("Alice");
}

@safe:

interface Database {
    Bytes keysOf(Bytes) const;
}
class MockDatabase : Database {
    Bytes keysOf(Bytes record) const { return record[0 .. 1]; }
}

class MockDatabase2(immutable(Schema) schema) : Database
{
    enum Schema = schema;
    
    auto table(string name)(){ 
        return DatabaseTable!(typeof(this), name)(this); 
    }

    Bytes keysOf(Bytes record) const { return []; }
}

struct DatabaseTable(D, string N) {
    alias Name = N;
    enum Table = D.Schema.tableNamed(Name);
    enum Columns = Table.columns;
    alias RecordTypes = asD!Columns;

    this(D db){ this.db = db; }
import std.meta;
    void insertRow(RecordTypes values){
        auto record = asStruct!(Table)(values);

    }

    D db;
}


struct ServerSideTable {
    Database db;

    void insertRow(Bytes record, SysTime when) {
        auto keys = db.keysOf(record);
        rows[keys] = Row(keys, record, Row.inserted, when, "Auto"); /// questo deve rappresentare le cose ancora da sincronizzare, va svuotato
    }

    void updateRow(Bytes keys, Bytes record, SysTime when) {
        rows[keys] = Row(keys, record, Row.updated, when, "Auto");
    }

    Delta syncDeltaFor(const ClientSideTable cst) {
        Delta delta;
        foreach(keys, srow; rows){
            auto crow = keys in cst.rows;
            if( crow is null ){
                delta ~= DeltaOperation( Operation.Ins, keys, srow.record, srow.by, srow.when );
            }
            else {
                if( crow.record != srow.record ){
                    delta ~= DeltaOperation( Operation.Upd, keys, srow.record, srow.by, srow.when );
                }
            }
        } 
        return delta;
    }
    Row row(Bytes keys) const {
        assert( (keys in rows) !is null);
        return rows[keys];
    }

    Row[Bytes] rows;
}

struct ClientSideTable {
    this(string by) {
        this.by = by;
        rows[ [0] ] = Row.init; rows.remove([0]);
    }
    long count() const { return 0; }
    void applyDelta(const Delta delta){
        foreach(op; delta){
            if(op.operation == Operation.Ins){
                assert((op.key in rows) is null);
                rows[op.key] = Row(op.key, op.record, Row.inserted, op.when, op.by);
            }
            else {
                auto row = op.key in rows;
                assert(row !is null);
                *row = Row(op.key, op.record, Row.updated, op.when, op.by);
            }
        }
    }
    Row row(Bytes keys) const { return rows[keys]; }
    void updateRow(Bytes keys, Bytes record, SysTime when) {
        auto row = keys in rows;
        assert(row !is null);
        *row = Row(keys, record, Row.optUpdated, when, by);
    }

    void insertRow(Bytes keys, Bytes record, SysTime when) {
        assert( (keys in rows) is null );
        rows[keys] = Row(keys, record, Row.optInserted, when, by);
    }
    Delta commitOrRollback(ref ServerSideTable sst) { 
        foreach(keys, ref crow; rows){
            if(crow.state == Row.optUpdated){
                auto srow = keys in sst.rows; assert(srow !is null);
                *srow = Row(crow.keys, crow.record, Row.updated, crow.when, crow.by);
                crow.state = Row.updated;
            }
            else if(crow.state == Row.optInserted){
                assert( (keys in sst.rows) is null );
                sst.rows[keys] = Row(keys, crow.record, Row.inserted, crow.when, crow.by);
                crow.state = Row.inserted;
            }
        }
        return []; 
    }

    string by;
    Row[Bytes] rows;
}

+/

