module mars.starwars;


version(unittest)
{
    import mars.defs;

    auto starwarsSchema() pure {
        return immutable(Schema)("sw", [
                immutable(Table)("people", [Col("name", Type.text), Col("gender", Type.text), Col("photo", Type.bytea), Col("height", Type.real_)], [0], [], 0),
                immutable(Table)("species", [Col("name", Type.text)], [0], [], 1),
                immutable(Table)("planets", [Col("name", Type.text), Col("population", Type.bigint)], [0], [], 2),
        ]);
    }

    alias Person = asStruct!(starwarsSchema.tables[0]);
    static luke = Person("Luke", "male", [0xDE, 0xAD, 0xBE, 0xEF], 1.72);
    static leila = Person("Leila", "female", [0xCA, 0xFE, 0xBA, 0xBE], 1.70);

    alias Planet = asStruct!(starwarsSchema.tables[2]);
    static tatooine = Planet("Tatooine", 120_000);
}
