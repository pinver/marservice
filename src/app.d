



int main()
{
    import std.format;
    import mars.server, mars.client;
    import vibe.core.core : runApplication;

    import vibe.data.json : Json;

    string exposedMethods(MarsClient marsClient, string methodName, Json parameters){
        switch(methodName){
            case "methodA": 
                int i = parameters["i"].get!int;
                return "responseA_%d".format(++i);
            default: return "unknownMethod";
        }
    }

    enum marsConf = MarsServerConfiguration();
    marsServer = new MarsServer(marsConf);
    marsServer.serverSideMethods = &exposedMethods;
    setupWebSocketServer();

    return runApplication();
}

void setupWebSocketServer()
{
    import vibe.http.router : URLRouter;
    import vibe.http.server : HTTPServerSettings, listenHTTP;
    import vibe.http.websockets : handleWebSockets;
    import mars.server;
    import mars.websocket : handleWebSocketConnection;

    auto router = new URLRouter();
    router.get("/ws", handleWebSockets(&handleWebSocketConnection));

    auto settings = new HTTPServerSettings();
    settings.port = 8081;
    settings.bindAddresses = ["::1", "0.0.0.0"];
    listenHTTP(settings, router);
}
