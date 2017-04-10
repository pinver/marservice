



int main()
{
    import std.format;
    import mars.server, mars.client, mars.starwars;
    import vibe.core.core : runApplication;

    import vibe.data.json : Json;
    import mars.sync2;

    string exposedMethods(MarsClient marsClient, string methodName, Json parameters){
        switch(methodName){
            case "methodA": 
                int i = parameters["i"].get!int;
                return "responseA_%d".format(++i);
            default: return "unknownMethod";
        }
    }

    enum marsConf = MarsServer
        .ExposeSchema(starwarsSchema())
        .PostgreSQL("127.0.0.1", 5432, "starwars")
        //.Autologin("jedi", "force")
        ;
    marsServer = new MarsServer(marsConf);
    marsServer.serverSideMethods = &exposedMethods;
    enum ctTables = marsConf.schemaExposed.tables;
    InstantiateTables!(ctTables)(marsServer, [], [], [], []);
    setupWebSocketServer();

    return runApplication();
}

void setupWebSocketServer()
{
    import vibe.http.router : URLRouter;
    import vibe.http.server : HTTPServerSettings, listenHTTP;
    import vibe.http.websockets : handleWebSockets;
    import mars.server;
    import mars.websocket : handleWebSocketConnectionClientToService, handleWebSocketConnectionServiceToClient;

    auto router = new URLRouter();
    router.get("/ws_c2s", handleWebSockets(&handleWebSocketConnectionClientToService));
    router.get("/ws_s2c", handleWebSockets(&handleWebSocketConnectionServiceToClient));

    auto settings = new HTTPServerSettings();
    settings.port = 8082;
    settings.bindAddresses = ["::1", "0.0.0.0"];
    listenHTTP(settings, router);
}
