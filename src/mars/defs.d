

module mars.defs;

import std.meta;
import std.traits;
import std.algorithm;
import std.array;
import std.meta;
import std.typecons;

import mars.starwars;

/+
struct Schema2 
{
    this(string name, string[] tt) {
        this.name = name;
        foreach(t; tt){
            this.tables ~= Table2(&this, t);
        }
    }

    string name;
    Table2[] tables;
}
struct Table2 {
    Schema2* schema;
    string name;
}
enum s1 = Schema2("s1", ["t1", "t2"]);
static assert(s1.tables.length == 2);
static assert(s1.tables[0].schema is s1);

auto schema(string name){ return Schema2(name, []); }
auto table(Schema2 schema, string name){
    string[] tt; foreach(t; schema.tables){ tt ~= t.name; }
    return Schema2(schema.name, schema.tables.map!"a.name"().array ~ name);
}

enum ss = schema("foo");
enum s2 = schema("foo").table("bar");
+/

struct Schema {
    string name;
    immutable(Table)[] tables;

    immutable(Table) tableNamed(string name) const @safe {
        return tables.find!((t) => t.name == name)[0];
    }
}
/+
Schema table(immutable(Schema) schema, string name, immutable(Col)[] columns, immutable(size_t)[] primaryKey, immutable(Reference)[] references, size_t index){
    auto s = Schema(schema.name, schema.tables ~ immutable(Table)(name, columns, primaryKey, references, index));
    return s;
}
unittest {
    enum s = Schema("foo").table("bar", [immutable(Col)("c1", Type.integer)], [], [], 0);
    static assert( s.tables.length == 1 );
    enum s2 = Schema("foo")
        .table("bar", [immutable(Col)("c1", Type.integer)], [], [], 0)
        .table("bar", [immutable(Col)("c1", Type.integer)], [], [], 1)
        ;
    static assert( s.tables.length == 2 );
}
+/

struct Sync {
    string mars_who, mars_what, mars_when;
}

enum SyncCols = [
    immutable(Col)("mars_who", Type.text, false), 
    immutable(Col)("mars_what", Type.text, false), 
    immutable(Col)("mars_when", Type.text, false),
];

struct Table { 
    string name;
    immutable(Col)[] columns;
    size_t[] primaryKey;
    Reference[] references;
    size_t index; /// the unique index of the table in the system.
    bool durable = true;
    bool decorateRows = false;
    bool cacheRows = true;

    immutable(Schema)* schema;

    @property immutable(Col)[] decoratedCols() const { return columns ~ SyncCols; }

    /*
    If the table primary key is set by the terver, we need to return it to the client. */
    const(Col)[] returningCols() const
    {
        import std.algorithm : canFind, filter;
        import std.array : array;
        import std.range : indexed;
        
        return pkCols
            .filter!( (c) => [Type.smallserial, Type.serial].canFind(c.type) )
            .array;
    }
    
    /**
     * returns the primary keys columns of a table */
    immutable(Col)[] pkCols() const
    {
        import std.array : array;
        import std.range : indexed;

        return columns.indexed(primaryKey).array;
    }
    unittest {
        static assert(Landings.pkCols.length == Landings.primaryKey.length);
    }

}
unittest
{
    immutable static t1 = immutable(Table)("t", [
            Col("c1", Type.integer), Col("c2", Type.text), Col("c3", Type.integer)
    ], [0,2], []);

    static assert( t1.pkCols() == [t1.columns[0], t1.columns[2]] );
    
    enum columns = t1.columns;
    void f1(asD!columns cols){ int i = cols[0]; string s = cols[1]; int j = cols[2]; } 
    f1(1, "foo", 2);
}

struct Col {
    string name;
    Type type;
    bool null_;
}

struct Reference {
    size_t[] referenceCols;
    string referencedTable;
    size_t[] referencedCols;
}

enum Type { 
    unknown, 
    boolean,
    date, 
    real_, doublePrecision, 
    smallint, integer, bigint, // 16, 32, 64 bit, postgresql
    smallserial, serial, 
    text, varchar,
    bytea 
}

/**
 * returns the D type for the passed SQL type. */
template asD(alias t) if( is(Unqual!(typeof(t)) == Type) )
{

    static if( t == Type.integer )              alias asD = int;
    else static if( t == Type.bigint )          alias asD = long;
    else static if( t == Type.text )            alias asD = string;
    else static if( t == Type.serial )          alias asD = int;
    else static if( t == Type.boolean )         alias asD = bool;
    else static if( t == Type.smallint )        alias asD = short;
    else static if( t == Type.smallserial )     alias asD = short;
    else static if( t == Type.real_ )           alias asD = float;
    else static if( t == Type.doublePrecision ) alias asD = double;
    else static if( t == Type.date )            alias asD = Date;
    else static if( t == Type.bytea )           alias asD = ubyte[];
    else static assert(false);
}

/**
 * returns the D type for a column. */
template asD(alias c) if( is(Unqual!(typeof(c)) == Col) )
{
    enum t = c.type;
    alias asD = .asD!(t);
}
static assert( is(asD!( Col("c", Type.integer) ) == int) );
static assert( is(asD!( Col("c", Type.real_) ) == float ) );

/**
 * returns the D type for a sequence of columns. */
template asD(alias c) if( is(Unqual!(typeof(c)) : immutable(Col)[]) || is(Unqual!(typeof(c)) : Col[]) )
{

    enum cols = c;
    static if(cols.length == 1){
        alias asD = AliasSeq!(asD!(cols[0]));
    }
    else static if(cols.length >1){
        alias asD = AliasSeq!(asD!(cols[0]), asD!(cols[1 .. $]));
    }
    else static assert(false);
}
static assert( is(asD!( [Col("c", Type.integer), Col("d", Type.text)] ) == AliasSeq!(int, string)) );

/** 
 * returns the D type and the name of the column. */
private template asStruct_(alias c, string p ="") if( is(Unqual!(typeof(c)) : immutable(Col)[]) || is(Unqual!(typeof(c)) : Col[]) )
{

    enum cols = c;
    enum prefix = p;
    static if(cols.length == 1){
        alias t = asD!(cols[0]);
        enum string n = cols[0].name;
        enum string asStruct_ = t.stringof ~ " " ~ prefix ~ n ~ ";"; //AliasSeq!(asD!(cols[0])););
    }
    else static if(cols.length >1){
        enum string asStruct_ = (asD!(cols[0])).stringof ~ " " ~ prefix ~ cols[0].name ~ "; " ~ asStruct_!(cols[1 .. $], prefix);
    }
    else static assert(false, cols.length);
}



template asStruct(alias t)
{
    enum cols = t.columns;
    enum string structName = t.name ~ "Row";
    enum string def = "struct " ~ structName ~ " {" ~ asStruct_!(cols) ~ "}";
    mixin(def ~"; alias asStruct = " ~ structName ~ ";");
}
static assert(is( asStruct!(Table("t", [immutable(Col)("c1", Type.integer), immutable(Col)("c2", Type.text)], [], [])) == struct )); 

/// I can't use here an 'alias this' composition, as it would be serialised wrongly.
template asSyncStruct(alias t)
{
    enum cols = t.decoratedCols;
    enum string structName = t.name ~ "SyncRow";
    enum string def = "struct " ~ structName ~ " {" ~ asStruct_!(cols) ~ "}";
    mixin(def ~"; alias asSyncStruct = " ~ structName ~ ";");
}

template asPkStruct(alias t)
{
    static if( t.pkCols.length >0 ){
        enum cols = t.pkCols;
    }
    else {
        enum cols = t.columns;
    }
    enum string structName = t.name ~ "PkRow";
    enum string def = "struct " ~ structName ~ " {" ~ asStruct_!(cols) ~ "}";
    mixin(def ~"; alias asPkStruct = " ~ structName ~ ";");
}
unittest {
    static assert(is(asPkStruct!(Table("t", [immutable(Col)("c1", Type.integer), immutable(Col)("c2", Type.text)], [0], [])) == struct ));
    static assert( FieldNameTuple!(asPkStruct!Landings) == AliasSeq!("person_name", "planet_name"));
}

template asPkParamStruct(alias t)
{
    static if( t.pkCols.length >0 ){
        enum cols = t.pkCols;
    }
    else {
        enum cols = t.columns;
    }
    enum string structName = t.name ~ "PkRow";
    enum string def = "struct " ~ structName ~ " {" ~ asStruct_!(cols, "key") ~ "}";
    mixin(def ~"; alias asPkParamStruct = " ~ structName ~ ";");
}
unittest {
    static assert(is(asPkStruct!(Table("t", [immutable(Col)("c1", Type.integer), immutable(Col)("c2", Type.text)], [0], [])) == struct ));
    static assert( FieldNameTuple!(asPkParamStruct!Landings) == AliasSeq!("keyperson_name", "keyplanet_name"));

}

template asSyncPkParamStruct(alias t)
{
    static if( t.pkCols.length >0 ){
        enum cols = t.pkCols ~ SyncCols;
    }
    else {
        enum cols = t.decoratedCols;
    }
    enum string structName = t.name ~ "SyncPkParamRow";
    enum string def = "struct " ~ structName ~ " {" ~ asStruct_!(cols, "key") ~ "}";
    mixin(def ~"; alias asSyncPkParamStruct = " ~ structName ~ ";");
}
static assert(is(asPkParamStruct!(Table("t", [immutable(Col)("c1", Type.integer), immutable(Col)("c2", Type.text)], [0], [])) == struct ));



/**
 * returns the values of the primary keys of this table row. */
auto pkValues(alias table)(const asStruct!table fixture)
{
    asPkStruct!table keys;
    assignCommonFields(keys, fixture);
    return keys;
}
unittest {
    static assert(luke.pkValues!People.name == "Luke");
}

/**
 * returns the values of the primary keys of this table row. */
auto pkParamValues(alias table)(const asStruct!table fixture)
{
    asPkStruct!table keys;
    asPkParamStruct!table paramKeys;
    assignCommonFields(keys, fixture);
    assignFields(paramKeys, keys);
    return paramKeys;
}
unittest {
    static assert(luke.pkParamValues!People.keyname == "Luke");
}

void assignCommonFields(R, V, size_t i = 0)(ref R r, V v)
{
    alias NamesR = FieldNameTuple!R;
    alias NamesV = FieldNameTuple!V;

    enum string Name = NamesV[i];


    enum j = staticIndexOf!(Name, NamesR);
    static if( j != -1 ){
        r.tupleof[j] = v.tupleof[i];
    }
    static if(i+1 < NamesV.length){
        assignCommonFields!(R, V, i+1)(r, v);
    }
}
unittest 
{
    struct V { string a; string b; string c; }
    struct R { string b; string a; }
    struct X { int b; string a; }

    V v = V("a", "b", "c");
    R r;
    X x;

    static assert(!__traits(compiles, assignCommonFields!(X, V)(x, v) ));

    assignCommonFields!(R, V)(r, v);
    assert( r == R("b", "a")) ;

}

void assignFields(R, V, size_t i = 0)(ref R r, V v)
{
    r.tupleof[i] = v.tupleof[i];
    static if(i+1 < v.tupleof.length) assignFields!(R, V, i+1)(r, v);
}
unittest
{
    struct V { string a; int b;}
    struct R { string c; int d;}
    auto v = V("hello", 42);
    R r;
    assignFields(r, v);
    assert( r == R("hello", 42) );
}
