
module mars.defs;

import std.meta;
import std.traits;

struct Schema {
    string name;
    immutable(Table)[] tables;
}

struct Table {
    string name;
    immutable(Col)[] columns;
    size_t[] primaryKey;
    Reference[] references;
    size_t index; /// the unique index of the table in the system.

    /*
    If the table primary key is set by the server, we need to return it to the client. */
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
    const(Col)[] pkCols() const
    {
        import std.array : array;
        import std.range : indexed;

        return columns.indexed(primaryKey).array;
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

enum Type { unknown, date, doublePrecision, integer, real_, smallint, smallserial, serial, text, varchar }

/**
 * returns the D type for the passed SQL type. */
template asD(alias t) if( is(Unqual!(typeof(t)) == Type) )
{

    static if( t == Type.integer )              alias asD = int;
    else static if( t == Type.text )            alias asD = string;
    else static if( t == Type.smallint )        alias asD = short;
    else static if( t == Type.real_ )           alias asD = float;
    else static if( t == Type.doublePrecision ) alias asD = double;
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
private template asStruct_(alias c) if( is(Unqual!(typeof(c)) : immutable(Col)[]) || is(Unqual!(typeof(c)) : Col[]) )
{

    enum cols = c;
    static if(cols.length == 1){
        alias t = asD!(cols[0]);
        enum string n = cols[0].name;
        enum string asStruct_ = t.stringof ~ " " ~ n ~ ";"; //AliasSeq!(asD!(cols[0])););
    }
    else static if(cols.length >1){
        enum string asStruct_ = (asD!(cols[0])).stringof ~ " " ~ cols[0].name ~ "; " ~ asStruct_!(cols[1 .. $]);
    }
    else static assert(false);
}
template asStruct(alias t)
{
    enum cols = t.columns;
    enum string structName = t.name ~ "Row";
    enum string def = "struct " ~ structName ~ " {" ~ asStruct_!(cols) ~ "}";
    mixin(def ~"; alias asStruct = " ~ structName ~ ";");
}
static assert(is( asStruct!(Table("t", [Col("c1", Type.integer), Col("c2", Type.text)], [], [])) == struct )); 

