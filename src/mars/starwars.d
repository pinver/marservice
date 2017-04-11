module mars.starwars;


//version(unittest) {  -- nope, the app.d use this schema, to expose it to the mars test client app
    import std.typecons : Yes, No;
    import mars.defs;

    auto starwarsSchema() pure {
        return immutable(Schema)("sw", [
            immutable(Table)("people", [Col("name", Type.text), Col("gender", Type.text), Col("photo", Type.bytea), Col("height", Type.doublePrecision)], [0], [], 0),
            immutable(Table)("species", [Col("name", Type.text), Col("average_lifespan", Type.integer)], [0], [], 1, Yes.durable, No.decorateRows, No.cached),
            immutable(Table)("planets", [Col("name", Type.text), Col("population", Type.bigint)], [0], [], 2, Yes.durable, Yes.decorateRows),

            immutable(Table)("scores", [Col("score", Type.integer)], [], [], 3, No.durable, No.decorateRows, Yes.cached),
            immutable(Table)("landings", [Col("person_name", Type.text), Col("planet_name", Type.text), Col("landings", Type.integer)], [0, 1], [], 4, Yes.durable, Yes.decorateRows, Yes.cached),
        ]);
    }
    auto People(){ return starwarsSchema.tableNamed("people"); };
    alias Person = asStruct!People;
    const static luke = Person("Luke", "male", [0xDE, 0xAD, 0xBE, 0xEF], 1.72);
    static leila = Person("Leila", "female", [0xCA, 0xFE, 0xBA, 0xBE], 1.70);

    alias Planet = asStruct!(starwarsSchema.tableNamed("planets"));
    static tatooine = Planet("Tatooine", 120_000);

    auto Landings(){ return starwarsSchema.tableNamed("landings"); }
    alias Landing = asStruct!(Landings);
    static landingsOfLeilaOnTatooine = Landing("Leila", "Tatooine", 1);
//}
