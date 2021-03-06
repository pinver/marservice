
module mars.pgsql;

import std.algorithm;
import std.conv;
import std.format;
import std.string;
import std.range;
import std.typecons;
import std.variant;

import mars.defs;
import mars.msg : AuthoriseError, InsertError, DeleteError, RequestState;
version(unittest) import mars.starwars;

import ddb.postgres;
import ddb.db;
import vibe.core.log;
import vibe.data.json;

string insertIntoReturningParameter(const(Table) table)
{
    int i = 1;
    return "insert into %s values (%s) returning *".format(
        table.name,
        table.columns.map!( (c) => c.type == Type.serial? "default" : "$" ~ (i++).to!string).join(", "));
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[],[]).insertIntoReturningParameter();
    assert( sql == "insert into bar values ($1, $2) returning *", sql );
    auto sql2 = Table("bar", [Col("w_id", Type.serial), Col("w", Type.text)], [0], []).insertIntoReturningParameter();
    assert( sql2 == "insert into bar values (default, $1) returning *", sql2);
}

string deleteFromParameter(const(Table) table)
{
    return "delete from %s where %s".format(
            table.name, 
            zip(iota(0, table.pkCols.length), table.pkCols)
                .map!( (t) => t[1].name ~ " = $" ~ (t[0]+1).to!string)
                .join(" and "));
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("bar", Type.text, false), Col("baz", Type.text, false)], [0, 1], []).deleteFromParameter();
    assert( sql == "delete from bar where foo = $1 and bar = $2", sql);
}

string updateFromParameters(const(Table) table)
{
    immutable(Col)[] whereCols = table.pkCols.length >0? table.pkCols : table.columns;
    int dollarIndex =1;
    return "update %s set %s where %s".format(
        table.name,
        table.columns.map!( (t) => t.name ~ " = $" ~ (dollarIndex++).to!string).join(", "),
        whereCols.map!( (t) => t.name ~ " = $" ~ (dollarIndex++).to!string).join(" and "));
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("bar", Type.text, false), Col("baz", Type.text, false)], [0], []).updateFromParameters();
    assert( sql == "update bar set foo = $1, bar = $2, baz = $3 where foo = $4", sql );
}

string selectFromParameters(const(Table) table)
{
    return "select * from %s where %s".format(
            table.name, 
            zip(iota(0, table.pkCols.length), table.pkCols)
                .map!( (t) => t[1].name ~ " = $" ~ (t[0]+1).to!string)
                .join(" and "));
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("bar", Type.text, false), Col("baz", Type.text, false)], [0, 1], []).deleteFromParameter();
    assert( sql == "select * from bar where foo = $1 and bar = $2", sql);
}


struct DatabaseService {
    string host;
    ushort port;
    string database;
 
    /**
     * Returns: an instance of `Database` of null if can't connect or authenticate. Errors details in 'err' */
    Database connect(string user, string password, ref AuthoriseError err) in {
        assert(user && password);
    } body {
        Database db;
        try {
            db = new Database(host, database, user, password);
            err = AuthoriseError.authorised;
        }
        catch(ServerErrorException e){
            switch(e.code){
                case "28000": // role "user" does not exist
                    logInfo("PostgreSQL role does not exist");
                    err = AuthoriseError.wrongUsernameOrPassword;
                    break;
                case "28P01": // password authentication failed for user "user"
                    logInfo("PostgreSQL password authentication failed for user");
                    err = AuthoriseError.wrongUsernameOrPassword;
                    break;
                default:
                    logWarn("S --- C | Unhandled PostgreSQL server error during connection!");
                    logInfo("S --- C | PostgreSQL server error: %s", e.toString);
                    err = AuthoriseError.unknownError;
            }
        }
        catch(Exception e){
            logWarn("S --- C | exception connecting to the PostgreSQL!");
            logWarn("S --- C | %s", e);
            err = AuthoriseError.unknownError;
        }
        assert( err != AuthoriseError.assertCheck);
        return db;
    }
}

class Database
{
    private this(string host, string database, string user, string password){
        this.username_ = user;
        if( db is null ){
            db = new PostgresDB(["host": host, "database": database, "user": user, "password": password]);
        }
        conn = db.lockConnection();
    }

    void execute(const Select select)
    {
        string s = `select %s from %s`.format(select.cols[0].name, select.tables[0].name);
        auto q = conn.executeQuery(s); 
    }

    void executeUnsafe(string sql){
        auto q = conn.executeQuery(sql);
        foreach(v; q){
            import std.stdio; writeln("-->", v);
        }
    }
    T executeScalarUnsafe(T)(string sql){
        return conn.executeScalar!T(sql);
    }

    // usato da sync per la sottoscrizione di query complesse
    auto executeQueryUnsafe(string sql){
        return conn.executeQuery(sql);
    }

    // usato da sync per la sottoscrizione di query complesse, con parametri
    auto executeQueryUnsafe(string sql, Variant[string] parameters){
        // ... sort param names, transform names into a sequence of $1, $2
        auto pgargs = xxx(sql, parameters);
        // ... prepare the statement
        auto cmd = new PGCommand(conn, pgargs[0]);
        foreach(j, param; pgargs[1]){
            // ... try to guess the PGType from the Variant typeinfo ...
            auto pgType = toPGType(param.type);
            switch(pgType) with (PGType){
                case TEXT:
                    cmd.parameters.add((j+1).to!short, pgType).value = param.get!string;
                    break;
                case INT2:
                    cmd.parameters.add((j+1).to!short, pgType).value = param.get!short;
                    break;
                case INT4:
                    cmd.parameters.add((j+1).to!short, pgType).value = param.get!int;
                    break;
                default:
                    assert(false, pgType.to!string);
            }
        }
        return cmd.executeQuery();
    }
    version(unittest_starwars){ unittest {
        auto db = new Database("127.0.0.1", "starwars", "jedi", "force");
        auto recordSet = db.executeQueryUnsafe("select * from planets where name = $name", ["name": Variant("Tatooine")]);
        scope(exit) recordSet.close();
        assert(recordSet.front[1].get!long == 120_000);
    }}

    auto executeQueryUnsafe(Row)(string sql){
        return conn.executeQuery!Row(sql);
    }

    auto executeInsert(immutable(Table) table, Row, )(Row record, ref InsertError err){
        enum sql = insertIntoReturningParameter(table);
        auto cmd = new PGCommand(conn, sql);
        addParameters!(table, Row, true)(cmd, record); // skip serial parameters
        Row result;
        try {
            auto querySet = cmd.executeQuery!Row();
            scope(exit) querySet.close();
            result = querySet.front;
            err = InsertError.inserted;
        }
        catch(ServerErrorException e){
            switch(e.code){
                case "23505": //  duplicate key value violates unique constraint "<constraintname>" (for example in primary keys)
                    err = InsertError.duplicateKeyViolations;
                    break;
                default:
                    logWarn("S --- C | Unhandled PostgreSQL server error during insertion!");
                    logInfo("S --- C | PostgreSQL server error: %s", e.toString);
                    err = InsertError.unknownError;
            }
        }
        if( table.journal && err == InsertError.inserted ){
            logWarn("S --- C | Journaling");
            try {
                cmd = new PGCommand(conn, `insert into journal (username, operation, tablename, post) values ($1, 'record_inserted', $2, $3)`);
                cmd.parameters.add(1, PGType.TEXT).value = this.username;
                cmd.parameters.add(2, PGType.TEXT).value = table.name;
                cmd.parameters.add(3, PGType.JSON).value = result.serializeToJson();
                cmd.executeNonQuery();
            }
            catch(Exception e){
                logWarn("Catch Exception during journaling:%s", e.toString());
            }
            logWarn("S --- C | Journaling done");
        }
        return result;
    }

    void executeDelete(immutable(Table) table, Pk)(Pk pk, ref DeleteError err){
        asStruct!table pre;
        if( table.journal ){
            pre = selectFromPk!(table, Pk)(pk);
        }

        enum sql = deleteFromParameter(table);
        auto cmd = new PGCommand(conn, sql);

        addParameters!table(cmd, pk);
        try {
            cmd.executeNonQuery();
            err = DeleteError.deleted;
        }
        catch(ServerErrorException e){
            switch(e.code){
                case "23503": // update or delete on table "<table>" violates foreign key constraint "<constraint>" on table "<othertable>"
                    err = DeleteError.rejectedAsForeignKeyViolation;
                    break;
                default:
                    logWarn("S --- C | Unhandled PostgreSQL server error during deletion!");
                    logInfo("S --- C | PostgreSQL server error: %s", e.toString);
                    err = DeleteError.unknownError;
            }
        }
        if( table.journal && err == DeleteError.deleted ){
            logWarn("S --- C | Journaling");
            try {
                cmd = new PGCommand(conn, `insert into journal (username, operation, tablename, post) values ($1, 'record_deleted', $2, $3)`);
                cmd.parameters.add(1, PGType.TEXT).value = this.username;
                cmd.parameters.add(2, PGType.TEXT).value = table.name;
                cmd.parameters.add(3, PGType.JSON).value = pre.serializeToJson();
                cmd.executeNonQuery();
            }
            catch(Exception e){
                logWarn("Catch Exception during journaling:%s", e.toString());
            }
            logWarn("S --- C | Journaling done");
        }
    }

    void executeUpdate(immutable(Table) table, Pk, Row)(Pk pk, Row record, ref RequestState state)
    {
        
        asStruct!table pre;
        if( table.journal ){
            pre = selectFromPk!(table, Pk)(pk);
        }

        enum sql = updateFromParameters(table);
        auto cmd = new PGCommand(conn, sql);
        addParameters!(table)(cmd, record);
        short i = record.tupleof.length +1;
        addParameters!table(cmd, pk, i);
        try {
            cmd.executeNonQuery();
        }
        catch(ServerErrorException e){
            switch(e.code){
                case "23503":
                    logInfo("S --- C | PostgreSQL can't update the primary key as still referenced (maybe add an update cascade?).");
                    state = RequestState.rejectedAsForeignKeyViolation;
                    break;
                default:
                    logWarn("S --- C | Unhandled PostgreSQL server error during update!");
                    logInfo("S --- C | PostgreSQL server error: %s", e.toString);
                    state = RequestState.rejectedAsPGSqlError;
            }
        }
        if( table.journal && state == RequestState.executed ){
            logWarn("S --- C | Journaling");
            try {
                cmd = new PGCommand(conn, `insert into journal (username, operation, tablename, post, pre) values ($1, 'record_updated', $2, $3, $4)`);
                cmd.parameters.add(1, PGType.TEXT).value = this.username;
                cmd.parameters.add(2, PGType.TEXT).value = table.name;
                cmd.parameters.add(3, PGType.JSON).value = record.serializeToJson();
                cmd.parameters.add(4, PGType.JSON).value = pre.serializeToJson();
                cmd.executeNonQuery();
            }
            catch(Exception e){
                logWarn("Catch Exception during journaling:%s", e.toString());
            }
            logWarn("S --- C | Journaling done");
        }
    }

    PGConnection conn;
    private {

        auto selectFromPk(immutable(Table) table, Pk)(Pk pk)
        {
            logWarn("S --- C | Selecting the record to be updated for the journal");
            enum sql = selectFromParameters(table);
            auto cmd = new PGCommand(conn, sql);
            addParameters!table(cmd, pk);
            //try {
                auto querySet = cmd.executeQuery!(asStruct!table)();
                scope(exit) querySet.close();
                auto pre = querySet.front;
            //}
            //catch(ServerErrorException e){
            //}
            logWarn("S --- C | Select done, record is %s", pre.serializeToJson());
            return pre;
        }

        private PostgresDB db;
        private string username_;
        public @property username() { return username_; }
    }
}


private {
    import mars.lexer;
    import mars.sqldb;

    PGType toPGType(TypeInfo t){
        if(t == typeid(bool)) return PGType.BOOLEAN;
        if(t == typeid(int)) return PGType.INT4;
        if(t == typeid(short)) return PGType.INT2;
        if(t == typeid(string)) return PGType.TEXT;
        if(t == typeid(float)) return PGType.FLOAT4;
        if(t == typeid(double)) return PGType.FLOAT8;
        if(t == typeid(ubyte[])) return PGType.BYTEA;
        assert(false, t.to!string);
    }

    PGType toPGType(Type t){
        final switch(t) with(Type) {
            case boolean: return PGType.BOOLEAN;
            case integer: return PGType.INT4; // XXX check
            case bigint: return PGType.INT8;
            case smallint: return PGType.INT2; // XXX check 
            case text: return PGType.TEXT;
            case real_: return PGType.FLOAT4;
            case doublePrecision: return PGType.FLOAT8;
            case bytea: return PGType.BYTEA;
            case smallserial: return PGType.INT2; // XXX check
            case serial: return PGType.INT4; // there's not really a serial type in postgres
            case date: return PGType.DATE; // XXX temptative

            case unknown:
            case varchar: // varchar(n), tbd as column
                assert(false, t.to!string); // not implemented right now, catch at CT
        }
    }

    void addParameters(immutable(Table) table, Struct, bool noSerials = false, short tupleofIndex =0)(PGCommand cmd, Struct s, short paramIndex =1){
        static if( is(Struct : asStruct!table) || Struct.tupleof.length == asStruct!(table).tupleof.length )
        {
            auto type =  table.columns[tupleofIndex].type;
            static if( noSerials ) auto mustAdd = type != Type.serial && type != Type.smallserial;
            else bool mustAdd = true;
            if( mustAdd ) cmd.parameters.add(paramIndex, table.columns[tupleofIndex].type.toPGType).value = s.tupleof[tupleofIndex];
        }
        else static if( is(Struct : asPkStruct!table) || Struct.tupleof.length == asPkStruct!(table).tupleof.length )
        {
            auto type =  table.columns[tupleofIndex].type;
            static if( noSerials ) auto mustAdd = type != Type.serial && type != Type.smallserial;
            else bool mustAdd = true;
            if( mustAdd ) cmd.parameters.add(paramIndex, table.pkCols[tupleofIndex].type.toPGType).value = s.tupleof[tupleofIndex];
        }
        else static assert(false);

        static if( s.tupleof.length > tupleofIndex+1 ) addParameters!(table, Struct, noSerials, tupleofIndex +1)(cmd, s, ++paramIndex);
    }


    string select(const(Select) stat){
        return `select %s from %s`.format(
            stat.cols.map!((c) => c.name).join(", "),
            stat.tables.map!( (t) => t.name ).join(", "), /// XXX ho bisogno del nome dello schema QUA... refactory necessario
            );
    }
    unittest {
        auto s = starwarsSchema();
        const sql = cast(Select)Parser([s], scan("select name from sw.people")).parse();
        assert(select(sql) == "select name from people", select(sql));
    }
    version(unittest_starwars){
        unittest {
            enum pub = starwarsSchema();
            enum tokens = scan("select name from sw.people");
            static const stat = Parser([pub], tokens).parse();
            auto db = new Database("127.0.0.1", "starwars", "jedi", "force");
            db.execute(cast(Select)stat);
        }
        unittest {
            // check bigint select
            enum pub = starwarsSchema();
            enum tokens = scan("select population from sw.planets");
            static const stat = Parser([pub], tokens).parse();
            auto db = new Database("127.0.0.1", "starwars", "jedi", "force");
            db.execute(cast(Select)stat);
        }
    }
    else {
        version(unittest){
            pragma(msg, "compile with version 'unittest_starwars' to activate postgresql starwars tests.");
        }
    }

    auto xxx(string sql, Variant[string] parameters){
        auto names = sort(parameters.keys);
        Variant[] pgparam;
        foreach(name; names){
            pgparam ~= parameters[name];
            sql = sql.replace("$"~name, "$"~(pgparam.length).to!string); // they are starting from $1, and not from $0
        }
        return tuple(sql, pgparam);
    }
    unittest {
        auto r = xxx("select * from planets where name=$name", ["name": Variant("Tatooine")]);
    }
}

