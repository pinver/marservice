
module mars.log;

import vibe.core.log;

class MarsLogger : Logger
{
    import std.stdio : write, writef, writeln;

    override void beginLine(ref LogLine line_info) @safe {
        writef("[%s:%s %s] ", line_info.func, line_info.line, line_info.fiberID);
    }

    override void put(scope const(char)[] text) @safe {
        write(text);
    }

    override void endLine() @safe {
        writeln();
    }

    override void log(ref LogLine line) @safe {
        writeln("logggggg");

    }
}
shared static this(){
    import vibe.core.log;

    shared static l = new MarsLogger();
    registerLogger(l);
}
