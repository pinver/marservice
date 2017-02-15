module mars.starwars;


version(unittest)
{
    import mars.defs;

    auto starwarsSchema() pure {
        return immutable(Schema)("sw", [
                immutable(Table)("people", [Col("name", Type.text), Col("gender", Type.text), Col("photo", Type.bytea)], [0], [], 0),
                immutable(Table)("species", [Col("name", Type.text)], [0], [], 1),
        ]);
    }

    alias Person = asStruct!(starwarsSchema.tables[0]);
    static luke = Person("Luke", "male", [0xDE, 0xAD, 0xBE, 0xEF]); 
    static leila = Person("Leila", "female", [0xCA, 0xFE, 0xBA, 0xBE]);    
}
