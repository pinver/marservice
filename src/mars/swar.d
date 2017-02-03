module mars.swar;


version(unittest)
{
    import mars.defs;

    auto starwarSchema() pure {
        return immutable(Schema)("swar", [
                immutable(Table)("people", [Col("name", Type.text), Col("gender", Type.text)], [0], [], 0),
                immutable(Table)("species", [Col("name", Type.text)], [0], [], 1),
        ]);
    }
}
