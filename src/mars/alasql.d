

module mars.alasql;

import std.algorithm;
import std.array;
import std.conv;
import std.format;
import std.range;

import mars.defs;


string insertIntoParameter(const(Table) table)
{
    return "insert into %s values (%s)"
        .format(table.name, table.columns.map!( (c) => "$" ~ c.name).join(", "));
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[],[]).insertIntoParameter;
    assert( sql == "insert into bar values ($foo, $baz)", sql );
}

string updateParameter(const(Table) table)
{
    if( table.pkCols.length >0 ){
        return "update %s set %s where %s"
            .format(
                    table.name,
                    table.columns.map!( (c) => c.name ~ " = $" ~ c.name).join(", "),
                    table.pkCols.map!( (c) => c.name ~ " = $key" ~ c.name).join(" AND "),
                   );
    }
    else {
        return "update %s set %s where %s"
            .format(
                    table.name,
                    table.columns.map!( (c) => c.name ~ " = $" ~ c.name).join(", "),
                    table.columns.map!( (c) => c.name ~ " = $key" ~ c.name).join(" AND "),
                   );
    }
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[0],[]).updateParameter;
    assert( sql == "update bar set foo = $foo, baz = $baz where foo = $keyfoo", sql );
}

string deleteFromParameter(const(Table) table)
{
    if( table.pkCols.length >0 ){
        return "delete from %s where %s"
            .format(
                    table.name,
                    table.pkCols.map!( (c) => c.name ~ " = $" ~ c.name).join(" AND "),
                   );
    }
    else {
        return "delete from %s where %s"
            .format(
                    table.name,
                    table.columns.map!( (c) => c.name ~ " = $" ~ c.name).join(" AND "),
                   );
    }
}
unittest {
    auto sql = Table("bar", [Col("foo", Type.text, false), Col("baz", Type.text, false)],[0],[]).deleteParameter;
    assert( sql == "delete from bar where foo = $foo", sql );
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

    string cols = table.columns
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

        // right now handle as a smallint, insert client side not implemented right now
        case smallserial: return "smallint";

        case unknown:
        case date:
        case serial:
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
