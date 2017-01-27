
module mars.pgsql;

import std.algorithm;
import std.format;
import std.string;

import mars.defs;
import ddb.postgres;
import ddb.db;
import vibe.core.log;

struct DatabaseService {
    string host;
    ushort port;
    string database;

    /**
     * Returns: an instance of `Database` of null if can't connect or authenticate */
    Database connect(string user, string password){
        Database db;
        try {
            db = new Database(host, database, user, password);
        }
        catch(Exception e){
            logWarn("S --- C | exception connecting to the PostgreSQL!");
            logWarn("S --- C | %s", e);
        }
        return db;
    }
}

class Database
{
    private this(string host, string database, string user, string password){
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
    auto executeQueryUnsafe(string sql){
        return conn.executeQuery(sql);
    }
    auto executeQueryUnsafe(Row)(string sql){
        return conn.executeQuery!Row(sql);
    }

    private {
        PostgresDB db;
        PGConnection conn;
    }
}

private {
    import mars.lexer;
    import mars.sqldb;

    version(unittest){
        auto starwarSchema() pure {
            return immutable(Schema)("sw", [
                immutable(Table)("people", [Col("name", Type.text), Col("gender", Type.text)], [0], []),
                immutable(Table)("species", [Col("name", Type.text)], [0], []),
        ]);
        }
    }
    string select(const(Select) stat){
        return `select %s from %s`.format(
            stat.cols.map!((c) => c.name).join(", "),
            stat.tables.map!( (t) => t.name ).join(", "), /// XXX ho bisogno del nome dello schema QUA... refactory necessario
            );
    }
    unittest {
        auto s = starwarSchema();
        const sql = cast(Select)Parser([s], scan("select name from sw.people")).parse();
        assert(select(sql) == "select name from people", select(sql));
    }

    unittest {
        /+enum pub = starwarSchema();
        enum tokens = scan("select foo from bar");
        static const stat = Parser([pub], tokens).parse();
        auto db = new Database("127.0.0.1", "pinver", "pinver", "");
        db.execute(cast(Select)stat);+/
    }
}

