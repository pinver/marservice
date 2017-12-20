

module mars.alasql;

import std.algorithm;
import std.array;
import std.conv;
import std.format;
import std.range;

import mars.defs;

string selectFrom(const(Table) table)
{
    return "select * from %s".format(table.name);
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[],[]).selectFrom;
    assert( sql == "select * from bar", sql );
}

string selectFromWhere(const(Table) table)
{
    auto cols = table.pkCols.length >0? table.pkCols : table.columns;
    return "select * from %s where %s".format(
        table.name,
        cols.map!( (c) => c.name ~ " = $key" ~ c.name).join(" AND "),
    );
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[0],[]).selectFromWhere;
    assert( sql == "select * from bar where foo = $keyfoo", sql );
}

string insertIntoParameter(const(Table) table)
{
    auto columns = table.decorateRows? table.decoratedCols : table.columns;
    return "insert into %s (%s) values (%s)"
        .format(table.name, columns.map!( (c) => c.name).join(", "), columns.map!( (c) => "$" ~ c.name).join(", "));
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[],[]).insertIntoParameter;
    assert( sql == "insert into bar (foo, baz) values ($foo, $baz)", sql );
}

string updateParameter(const(Table) table)
{
    auto cols = table.pkCols.length >0? table.pkCols : table.columns;
    return "update %s set %s where %s".format(
        table.name,
        table.columns.map!( (c) => c.name ~ " = $" ~ c.name).join(", "),
        cols.map!( (c) => c.name ~ " = $key" ~ c.name).join(" AND "),
    );
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[0],[]).updateParameter;
    assert( sql == "update bar set foo = $foo, baz = $baz where foo = $keyfoo", sql );
    auto sql2 = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[],[]).updateParameter;
    assert( sql2 == "update bar set foo = $foo, baz = $baz where foo = $keyfoo AND baz = $keybaz", sql2 );
}

string updateDecorationsParameter(const(Table) table)
{
    auto cols = table.pkCols.length >0? table.pkCols : table.columns;
    return "update %s set %s where %s".format(
        table.name,
        ["mars_who = $mars_who", "mars_what = $mars_what", "mars_when = $mars_when"].join(", "),
        cols.map!( (c) => c.name ~ " = $key" ~ c.name).join(" AND "),
    );
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false), Col("bak", Type.text)],[0, 1],[]).updateDecorationsParameter;
    assert( sql == "update bar set mars_who = $mars_who, mars_what = $mars_what, mars_when = $mars_when where foo = $keyfoo AND baz = $keybaz", sql );
}

string updateDecoratedRecord(const(Table) table)
{
    auto pkKeys = table.pkCols.length >0? table.pkCols : table.columns;
    auto cols = table.columns.map!"a.name".array ~ ["mars_who", "mars_what", "mars_when"];
    return "update %s set %s where %s".format(
        table.name,
        cols.map!( (c) => c ~ " = $" ~ c).join(", "),
        pkKeys.map!( (c) => c.name ~ " = $key" ~ c.name).join(" AND "),
    );
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false), Col("bak", Type.text)],[0, 1],[]).updateDecoratedRecord;
    assert( sql == "update bar set foo = $foo, baz = $baz, bak = $bak, mars_who = $mars_who, mars_what = $mars_what, mars_when = $mars_when where foo = $keyfoo AND baz = $keybaz", sql );
}

string deleteFromParameter(const(Table) table)
{
    auto cols = table.pkCols.length >0? table.pkCols : table.columns;
    return "delete from %s where %s".format(
        table.name,
        cols.map!( (c) => c.name ~ " = $key" ~ c.name).join(" AND "),
    );
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[0],[]).deleteFromParameter;
    assert( sql == "delete from bar where foo = $keyfoo", sql );
}

string pkValuesJs(const(Table) table)
{
    auto cols = table.pkCols.length >0? table.pkCols : table.columns;
    return "(function a(r){ return { %s }; })".format(
        cols.map!( (c) => c.name ~ ": r." ~ c.name).join(", "),
    );
}
unittest {
    auto js = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[0],[]).pkValuesJs;
    assert( js == "(function a(r){ return { foo: r.foo }; })", js);
}

string pkValuesWhereJs(const(Table) table)
{
    auto cols = table.pkCols.length >0? table.pkCols : table.columns;
    return "(function a(r){ return { %s }; })".format(
        cols.map!( (c) => "key" ~ c.name ~ ": r." ~ c.name).join(", "),
    );
}
unittest {
    auto js = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[0],[]).pkValuesWhereJs;
    assert( js == "(function a(r){ return { keyfoo: r.foo }; })", js);
}

string referenceJs(const(Table) table, const(Schema) schema)
{
    auto references = table.references;
    string[] rrr;
    foreach(reference; references){
        auto referencedTable = schema.tableNamed(reference.referencedTable);
        rrr ~= ["{ referenceCols: [%s], referencedTable: '%s', referencedIndex: %d, referencedCols: [%s] }".format(
            reference.referenceCols.map!((i) => "'" ~ table.columns[i].name ~ "'").join(", "),
            reference.referencedTable,
            referencedTable.index,
            reference.referencedCols.map!((i) => "'" ~ referencedTable.columns[i].name ~ "'").join(", ")
        )];
    }
    string sss = "(function a(c) { return [" ~ rrr.join(", ") ~ "]; })";
    return sss;
}
unittest {
    enum sc = Schema("testschema", [
        immutable Table("bar1", [Col("foo", Type.text, false), Col("poo", Type.text, false)], [0, 1], [] ),
        immutable Table("bar2", [Col("foo", Type.text, false), Col("poo", Type.text, false)], [], [Reference([0,1], "bar1", [0,1])]),
    ]);
    enum eee = referenceJs(sc.tables[1], sc);
    static assert( eee == "(function a(c) { return [{ referenceCols: ['foo', 'poo'], referencedTable: 'bar1', referencedIndex: 0, referencedCols: ['foo', 'poo'] }]; })", eee);
}


string createDatabase(const(Schema) schema)
{
    return schema.tables.map!( t => createTable(schema, t) )().join("; ");
}

/**
 * Passing the schema is necessary for being able to refer to foreign table columns names.
 */
string createTable(const(Schema) schema, const(Table) table)
{
        
    auto primaryKey(T)(T t){
        ulong i = t.index;
        string v = t.value;

        string postfix = "";
        if( canFind(table.primaryKey, i) ) //  table.primaryKey.length ==1 && table.primaryKey[0] == i )
            postfix = " primary key";
        return typeof(t)(i, v ~ postfix);
    }

    string references(T)(T t){
        string r = t.value;
        ulong i = t.index;

        foreach(reference; table.references){
            // ... alasql 0.3.6 does not support references with multiple columns, so skip them.
            if( reference.referenceCols.length > 1) continue;

            assert(reference.referenceCols.length == 1); // only one col in alasql references
            assert(reference.referencedCols.length == 1); // idem
            if( reference.referenceCols[0] == i ){
                r ~= " references " ~ reference.referencedTable ~ "(";
                import std.algorithm : find; import std.range : front;
                auto tbs = schema.tables.find!( t => t.name == reference.referencedTable );
                if( tbs.empty ) assert(false, "table "~ table.name ~ " reference table " ~ reference.referencedTable ~ " that does not exists");
                auto tb = tbs.front;
                r ~= tb.columns[reference.referencedCols[0]].name ~ ")";
            }
        }
        return r;
    }

    string cols = (table.decorateRows? table.decoratedCols : table.columns)
        .map!( (c){ return c.asNameTypeNull; })
        .ctfeEnumerate
        //.map!(primaryKey!(EnumerateResult!(ulong, string)))
        .map!(references!(EnumerateResult!(ulong, string)))
        .join(", ");
    

    string primaryKeys = "";
    if( table.primaryKey.length > 0 ){
        primaryKeys = ", primary key (" ~ table.primaryKey.map!( i => table.columns[i].name ).join(", ") ~ ")";
    }

    string sql = "create table " ~ table.name ~ " (" ~ cols ~ primaryKeys ~ ")";
    return sql;
    
}
unittest {
    auto sc = Schema("testschema", [
        immutable Table("bar1", [ Col("foo", Type.text, false) ], [], [] ),
        immutable Table("bar2", [Col("foo", Type.text, false), Col("poo", Type.text, false)], [1], []),
        immutable Table("bar3", [Col("foo", Type.text, false)], [], [Reference([0], "bar2", [1])]),
        immutable Table("bar4", [Col("foo", Type.text, false), Col("bar", Type.text, false)], [], [Reference([0,1], "bar1", [0,1])]),
        immutable Table("bar5", [Col("foo", Type.text, false), Col("bar", Type.text, false)], [0, 1], []),
        immutable Table("bar6", [Col("foo", Type.text, false), Col("bar", Type.text, false)], [0, 1], [], 6, Yes.durable, Yes.decorateRows),
        ]);
    
        string sql = sc.createTable(sc.tables[0]);
        assert(sql == "create table bar1 (foo text not null)", sql);
        sql = sc.createTable(sc.tables[1]);
        assert(sql == "create table bar2 (foo text not null, poo text not null, primary key (poo))", sql);
        sql = sc.createTable(sc.tables[2]);
        assert(sql == "create table bar3 (foo text not null references bar2(poo))", sql);
        sql = sc.createTable(sc.tables[3]);
        assert(sql == "create table bar4 (foo text not null, bar text not null)", sql);
        sql = sc.createTable(sc.tables[4]);
        assert(sql == "create table bar5 (foo text not null, bar text not null, primary key (foo, bar))", sql);
        sql = sc.createTable(sc.tables[5]);
        assert(sql == "create table bar6 (foo text not null, bar text not null, mars_who text not null, mars_what text not null, mars_when text not null, primary key (foo, bar))", sql);

}

string asNameTypeNull(const(Col) col)
{
    return col.name ~ " " ~ col.type.toSql ~ (col.null_? "" : " not null");
}
unittest {
    assert( Col("foo", Type.text, false).asNameTypeNull == "foo text not null" );
}

/**
 * See_Also: https://github.com/agershun/alasql/wiki/Data%20Types
 */
string toSql(Type t){
    import std.conv : to;

    final switch(t) with(Type) {
        case boolean: return "boolean";
        case integer: return "integer";
        case bigint: return "integer";
        case smallint: return "smallint"; 
        case text: return "text";
        case real_: return "real";
        case doublePrecision: return "double precision";
        // ... this is not really supported by alasql, as in javascript we are using 'Buffer', but actually the javascript code
        //     doesn't perform any check for unknown type, so ...
        case bytea: return "bytea";

        // ... the mars client library is handling directly the 'autoincrement' alasql type, as we need to 
        //     reconcile the optimistic updated serial with the server ...
        case serial: return "smallint";
        case smallserial: return "integer";

        // ... temptative: actually I'm not using date in 'cached' tables, so it's just a matter of avoiding that
        case date: return "text";

        case unknown:
        case varchar: // varchar(n), tbd as column
            assert(false, t.to!string); // not implemented right now, catch at CT
    }
}
unittest {
    assert( Type.text.toSql == "text" );
    assert( Type.integer.toSql == "integer" );
}

private
{
    // BUG see https://issues.dlang.org/show_bug.cgi?id=15064
    struct EnumerateResult(I,V) { I index; V value; }
    struct enumerator(R) {
        import std.range : ElementType;
        R r;
        ulong j = 0;
        @property EnumerateResult!(ulong, ElementType!R) front() { return EnumerateResult!(ulong, ElementType!R)(j, r.front); }
        auto empty() { return r.empty; }
        void popFront() { j++; r.popFront(); }
    }
    auto ctfeEnumerate(R)(R r){ return enumerator!R(r); }
}
