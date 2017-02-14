module mars.sqldb;

import std.algorithm;
import std.array;
import std.format;
import std.range;

import mars.defs;
import mars.lexer;

class Statement {}
class Select : Statement
{
    Col[] cols;
    immutable(Table)[] tables;

    string createClientTables() const {
        return `create table %s (%s)`.format(
                tables[0].name,
                cols.map!createTableColumn
                .joiner(", ")
                .array
                );
    }

    string insertClientRow() const {
        return `insert into %s (%s) values (%s)`.format(
                tables[0].name,
                cols.map!name().joiner(", ").array,
                cols.map!`"?"`().joiner(", ").array
                );
    }

    string updateClientWithReturning() const {
        auto filtered = cols.filter!( (a) => a.type == Type.serial || a.type == Type.smallserial ).array;
        return `update %s set %s = $%s where %s = $optimistic_%s`.format(
                tables[0].name,
                filtered[0].name,
                filtered[0].name,
                filtered[0].name,
                filtered[0].name);
    }

    string insertServerRow() const {
        // insert statement, without serial primary keys...
        auto filtered = cols.filter!( (a) => a.type != Type.serial && a.type != Type.smallserial ).array;
        auto sql = `insert into quattro_graphql.%s (%s) values (%s)`.format(
                tables[0].name,
                filtered.map!name().joiner(", ").array,
                iota(1, filtered.length+1).map!((a)=> "$%d".format(a))().joiner(", ").array
                );
        auto returning = tables[0].returningCols();
        if(returning){
            sql ~= ` returning %s`.format(
                    returning.map!name().joiner(", ").array
                    );
        }
        return sql;
    }

    Col colNamed(string name) const {
        return cols.filter!((col) => col.name == name)().front;
    }

    override string toString() const { 
        import std.format : format;
        return "select %s from %s".format(cols, tables);
    }
}
class Insert : Statement {
    immutable(Table)[] tables;
    Col[] cols;
    string[] params;
    Col[] returning;
}

void fillFrom(ref Col c, immutable Col f){ c.name = f.name; c.type = f.type; c.null_ = f.null_; }
string name(const Col col) pure { return col.name; }
string createTableColumn(const Col col) pure {
    enum alasqlTypes = ["unknown", "date", "smallint", "smallint", "int", "text", "varchar"];
    return `%s %s%s`.format(col.name, alasqlTypes[col.type], (col.null_)? "": " not null") ;
}
//static assert(createTableColumn(Col("foo", Type.smallint)) == "foo smallint not null");

struct Parser 
{
    immutable Schema[] schemas;
    Token[] tokens;
    Token t;

    Statement parse(){
        advance();
        if(t.t == TType.select){
            Select s = new Select;
            parseCols(s);
            parseFrom(s);
            foreach(ref col; s.cols){
                auto c = s.tables[0].columns.find!"a.name ==b"(col.name);
                if(c.length ==0) throw new Exception("no column named "~col.name~" in table "~s.tables[0].name);
                col.fillFrom(c[0]);
            }
            return s;
        }
        else if(t.t == TType.insert){
            Insert s = new Insert;
            advance(); parseInto(s);
            parseCols(s);
            parseValues(s);
            if(tokens.empty) return s;
            parseReturning(s);
            return s;
        }

        if(!__ctfe){ import std.stdio; writeln(t); }
        assert(false);
    }

    void parseCols(Select s){
        do {
            advance();
            s.cols ~= Col(t.v);
            advance();
        } while([TType.comma, TType.dot].canFind(t.t));
    }

    void parseCols(Insert s){
        if(t.t != TType.lparen) throw new Exception("expected left parentesys after 'into'");
        do {
            advance();
            auto col = s.tables[0].columns.filter!((c)=>c.name==t.v)();
            if(col.empty) throw new Exception("column "~t.v~" does not exists.");
            s.cols ~= col.front;
            advance();
        } while(t.t == TType.comma);
        if(t.t != TType.rparen) throw new Exception("expected right paren after column list");
        advance();
    }

    void parseInto(Insert s){
        assert(t.t == TType.into);
        advance();
        string schemaName = "public";
        if( peek.t == TType.dot){
            schemaName = t.v;
            advance(); advance();
        }
        auto sc = schemas.find!"a.name == b"(schemaName);
        if(sc.length ==0) throw new Exception("no schema named "~schemaName);
        auto ta = sc[0].tables.find!"a.name == b"(t.v);
        if(ta.length ==0) throw new Exception("no table "~t.v~" in schema "~schemaName);
        s.tables ~= ta[0];
        advance();
    }

    void parseValues(Insert s){
        if(t.t != TType.values) throw new Exception("expected 'values' after column list");
        advance();
        if(t.t != TType.lparen) throw new Exception("expected '(' after 'values'");
        do {
            advance();
            if(t.t == TType.dollar){
                advance();
                s.params ~= t.v;
            }
            else {
                throw new Exception("Limitation: insert is supported only via parameter binding");
            }
            advance();
        } while(t.t == TType.comma);
        if(t.t != TType.rparen) throw new Exception("expected ')' after values list");
        advance();
    }

    void parseReturning(Insert s)
    {
        if(t.t != TType.returning) throw new Exception("expected 'returning' after values list");
        advance();
        auto col = s.tables[0].columns.filter!((a)=>a.name == t.v)();
        if(col.empty) throw new Exception("table "~s.tables[0].name~" has no column "~t.v);
        s.returning ~= col.front;
        advance();
    }

    void parseFrom(Select s){
        assert(t.t == TType.from);
        advance();
        string schemaName = "public";
        if( peek.t == TType.dot){ 
            schemaName = t.v;
            advance(); advance();
        }
        auto sc = schemas.find!"a.name == b"(schemaName);
        if(sc.length ==0) throw new Exception("no schema named "~schemaName~". Available schemas are:"~schemas.map!("a.name")().join(" "));
        auto ta = sc[0].tables.find!"a.name == b"(t.v);
        if(ta.length ==0) throw new Exception("no table "~t.v~" in schema "~schemaName);
        s.tables ~= ta[0];
    }
    void advance(){
        //if(! __ctfe){ import std.stdio; writeln(tokens); }
        t = tokens[0]; tokens = tokens[1 .. $]; }
    Token peek() const { return tokens[0]; }
}
