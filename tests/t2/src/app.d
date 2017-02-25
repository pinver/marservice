/**

When the thread exists, it does not wait for tasks. 

*/


import core.time; 
import vibe.http.websockets;
import vibe.core.core;

__gshared string[] log;
void writeln(string s){
    import std.stdio : writeln_ = writeln;
    writeln_(s);
    log ~= s;
}

void main()
{
    import std.stdio : writefln;
    import vibe.core.concurrency;
    import vibe.http.router;

    auto router = new URLRouter;
    router.get("/ws_ue", handleWebSockets(&handleConn));
    
    auto settings = new HTTPServerSettings;
    settings.port = 8080;
    settings.bindAddresses = ["0.0.0.0"];
    listenHTTP(settings, router);

    /+
    setTimer(1.seconds, {
            writeln("connect");
            auto socket = connectWebSocket(URL("ws://127.0.0.1:8080/ws_ue"));
            writeln("close");
            socket.close();
            });
    +/
    setTimer(30.seconds, {
            writeln("timer exitEventLoop");
            exitEventLoop(true); // exit event loops of all threads.
            });


    writeln("runEventLoop()");
    runEventLoop();
    writeln("main thread exiting");

    writefln("%s", log);
}

void handleConn(scope WebSocket socket){

    auto writer = runTask({
        writeln("writer running");
        while(socket.connected) {
            yield;//sleep(1.seconds);
        }
        writeln("writer stopping");
    });

    while( socket.waitForData) {
        auto m = socket.receiveBinary();
        writeln("socket received");
    }
    writeln("join");
    writer.join();
    writeln("joined exiting");

}
