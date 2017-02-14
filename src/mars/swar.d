module mars.swar;


version(unittest)
{
    import mars.defs;

    auto starwarSchema() pure {
        return immutable(Schema)("sw", [
                immutable(Table)("people", [Col("name", Type.text), Col("gender", Type.text), Col("photo", Type.bytea)], [0], [], 0),
                immutable(Table)("species", [Col("name", Type.text)], [0], [], 1),
        ]);
    }
}
