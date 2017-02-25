/**

When the thread exists, it does not wait for tasks. 

*/



__gshared string[] log;
void writeln(string s){
    import std.stdio : writeln_ = writeln;
    writeln_(s);
    log ~= s;
}

void main()
{
    import std.stdio : writefln;
    import vibe.core.core;
    import vibe.core.concurrency;
    import core.time : seconds; 

    setTimer(1.seconds, {
            writeln("timer exitEventLoop");
            exitEventLoop(true); // exit event loops of all threads.
            writeln("timer exitEventLoop done");
            });

    auto t1 = runTask({
            writeln("t1 running");
            sleep(2.seconds);
            writeln("t1 stopping");
            });

    auto t2 = runTask({
            writeln("t2 running");
            while(true) {
                int i = receiveOnly!int();
                writeln("t2 received");
                if( i == 42 ) break;
            }
            writeln("t2 stopping");
            });

    writeln("runEventLoop()");
    runEventLoop();
    writeln("main thread exiting");

    assert(log == ["runEventLoop()", "t1 running", "t2 running", "timer exitEventLoop", "timer exitEventLoop done", 
            "main thread exiting"]);
    //writefln("%s", log);
}
