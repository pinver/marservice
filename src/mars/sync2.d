module mars.sync2;

import std.algorithm.comparison,
       std.algorithm.iteration,
       std.datetime,
       std.experimental.logger;

import mars.defs;

alias Bytes = immutable(ubyte)[];

enum Operation { Ins, Committed, Upd, ConfirmIns }
struct DeltaOperation {
    Operation operation;
    Bytes key;
    Bytes record;
    string by;
    SysTime when;
    ulong revision;
}
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

    ulong revision;
}

unittest {

    enum k1 = [0]; enum r1 = [0, 1, 2, 3];
    enum k2 = [4]; enum r2 = [4, 5, 6, 7];
                   enum r3 = [8, 9, 0, 1];

    auto t0 = SysTime(DateTime(2017, 01, 01, 0, 0));
    auto t1 = SysTime(DateTime(2017, 01, 01, 1, 0));
    auto t2 = SysTime(DateTime(2017, 01, 01, 2, 0));

    auto db = new MockSimpleDatabaseTable();
    auto sst = ServerSideTable(db);
    auto bob = ClientSideTable(db, "Bob");
    auto alice = ClientSideTable(db, "Alice");

    sst.insertRow(r1, t0);
    assert(bob.count == 0 && alice.count == 0);
    assert(bob.revision == 0 && alice.revision == 0 && sst.revision == 1);

    // ... il delta è qualcosa che noi inviamo al client, via websocket ...`
    auto delta = sst.syncDeltaFor(bob);
    // ... una volta che il client ci ha risposto, possiamo aggiornare la sua rappresentazione qua ...
    bob.applyDelta(delta);
    assert(bob.revision ==1);

    auto row = bob.row(k1);
    assert(row.record == r1 && row.by == "Auto" && row.when == t0 && row.state == Row.inserted);

    // client, arrivano una serie di operazioni da fare. Devo tornare una sequenza di cose da fare lato client.
    bob.updateRow(k1, r3, t1);
    bob.insertRow(    r2, t1);                      
    assert(bob.row(k1).state == Row.optUpdated && bob.row(k2).state == Row.optInserted);
    assert(bob.revision ==3);

    // applichiamo al server quanto abbiamo ricevuto dal web, via websocket: il delta racchiude le operazioni da inviare al client per
    // conferma o aggiornamento ....
    delta = bob.commitOrRollback(sst);
    // noi ci siamo aggiornati, il server ha fatto, non si torna indietro.
    assert(sst.row(k1).record == r3 && sst.row(k2).record == r2 && sst.revision ==3);
    // inviamo il delta a bob... quanto lo ha eseguito, aggiorniamoci 
    bob.applyDelta(delta);
    assert(bob.row(k1).state == Row.updated && bob.row(k2).state == Row.inserted);

    // ... aggiorniamo alice
    delta = sst.syncDeltaFor(alice);
    // ... alice ha eseguito il delta, prendiamone atto ...
    alice.applyDelta(delta);
    assert(alice.row(k1).record == r3 && sst.row(k1).by == "Bob" && alice.revision ==3);
    
    // ... update a row server side
    sst.updateRow(k1, r1, t2);
    delta = sst.syncDeltaFor(bob);
    bob.applyDelta(delta);
    assert(bob.row(k1).record == r1 && bob.row(k1).by == "Auto" && sst.revision ==4 && bob.revision ==4);

    // ... purge records that we have already synced
    sst.purgeRevisions([bob, alice]);
    assert( (cast(immutable(ubyte)[])k2 in sst.rows) is null);
}

unittest
{
    import mars.starwars;
    auto schema = starwarsSchema();
    auto db = new MockDatabase!(schema);

    auto dbPeople = db.table!"people";
    dbPeople.insertRow("Luke", "male", [0xDE, 0xAD, 0xBE, 0xEF], 1.72);
    dbPeople.insertRow("Leila", "female", [0xCA, 0xFE, 0xBA, 0xBE], 1.70);

    auto sst = ServerSideTable(dbPeople);
    auto bob = ClientSideTable(dbPeople, "Bob");
    auto alice = ClientSideTable(dbPeople, "Alice");
}

@safe:

interface DatabaseTable {
    Bytes keysOf(Bytes) const;
}
class MockSimpleDatabaseTable : DatabaseTable {
    Bytes keysOf(Bytes record) const { return record[0 .. 1]; }
}

class MockDatabase(immutable(Schema) schema)
{
    enum Schema = schema;
    
    auto table(string name)(){ 
        return new MockDatabaseTable!(typeof(this), name)(this); 
    }
}

class MockDatabaseTable(D, string N) : DatabaseTable {
    alias Name = N;
    enum Table = D.Schema.tableNamed(Name);
    enum Columns = Table.columns;
    alias RecordTypes = asD!Columns;

    this(D db){ this.db = db; }

    void insertRow(RecordTypes values){
        auto record = asStruct!(Table)(values);
        asPkStruct!Table keys;
        assignCommonFields(keys, record);
        assert((keys in fixtures) is null);
        fixtures[keys] = record;
    }

    Bytes keysOf(Bytes record) const { return record[0 .. 1]; }

    D db;
    asStruct!Table[asPkStruct!Table] fixtures;
}


struct ServerSideTable {
    DatabaseTable db;

    void insertRow(Bytes record, SysTime when) {
        auto keys = db.keysOf(record);
        rows[keys] = Row(keys, record, Row.inserted, when, "Auto", ++revision);
    }

    void updateRow(Bytes keys, Bytes record, SysTime when) {
        rows[keys] = Row(keys, record, Row.updated, when, "Auto", ++revision);
    }

    Delta syncDeltaFor(const ClientSideTable cst) {
        Delta delta;
        foreach(keys, srow; rows){
            auto crow = keys in cst.rows;
            if( crow is null ){
                delta ~= DeltaOperation( Operation.Ins, keys, srow.record, srow.by, srow.when, srow.revision );
            }
            else {
                if( crow.record != srow.record ){
                    delta ~= DeltaOperation( Operation.Upd, keys, srow.record, srow.by, srow.when, srow.revision );
                }
            }
        } 
        return delta;
    }

    void purgeRevisions(ClientSideTable[] csts) @trusted { // byKeyValue is @system
        ulong rev = revision;
        foreach(cst; csts) rev = min(rev, revision);
        Bytes[] toPurge;
        foreach(kv; rows.byKeyValue){
            if( kv.value.revision < rev ) toPurge ~= kv.key;
        }
        foreach(k; toPurge) rows.remove(k);
        foreach(cst; csts){
            toPurge = [];
            foreach(kv; cst.rows.byKeyValue){
                if( kv.value.revision < rev ) toPurge ~= kv.key;
            }
            foreach(k; toPurge) cst.rows.remove(k);
        }
    }


    Row row(Bytes keys) const {
        assert( (keys in rows) !is null);
        return rows[keys];
    }

    Row[Bytes] rows;
    ulong revision;
}

struct ClientSideTable {
    this(DatabaseTable db, string by) {
        this.db = db;
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
                if(op.operation == Operation.ConfirmIns)
                    *row = Row(op.key, op.record, Row.inserted, op.when, op.by);
                else 
                    *row = Row(op.key, op.record, Row.updated, op.when, op.by);
            }
            revision = max(revision, op.revision);
        }
    }
    Row row(Bytes keys) const { return rows[keys]; }

    void updateRow(Bytes keys, Bytes record, SysTime when) {
        auto row = keys in rows;
        assert(row !is null);
        *row = Row(keys, record, Row.optUpdated, when, by, ++revision);
    }

    void insertRow(Bytes record, SysTime when) {
        Bytes keys = this.db.keysOf(record);
        assert( (keys in rows) is null );
        rows[keys] = Row(keys, record, Row.optInserted, when, by, ++revision);
    }

    Delta commitOrRollback(ref ServerSideTable sst) {
        Delta delta;
        foreach(keys, ref crow; rows){
            if(crow.state == Row.optUpdated){
                auto srow = keys in sst.rows; assert(srow !is null);
                *srow = Row(crow.keys, crow.record, Row.updated, crow.when, crow.by, crow.revision);
                delta ~= DeltaOperation(Operation.Upd, crow.keys, crow.record, crow.by, crow.when, crow.revision);
            }
            else if(crow.state == Row.optInserted){
                assert( (keys in sst.rows) is null );
                sst.rows[keys] = Row(keys, crow.record, Row.inserted, crow.when, crow.by, crow.revision);
                delta ~= DeltaOperation(Operation.ConfirmIns, crow.keys, crow.record, crow.by, crow.when, crow.revision);
            }
            sst.revision = max(sst.revision, crow.revision);
        }
        return delta; 
    }

    DatabaseTable db;
    string by;
    Row[Bytes] rows;
    ulong revision;
}
