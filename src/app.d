

import mars;
import mars.sync2; // per compilarlo...

int main()
{
    import vibe.core.core : runApplication;


    setupMars();
    setupWebSocketServer();

    return runApplication();
}

void setupMars()
{
    import std.format;
    import mars.server, mars.client, mars.starwars;
    import vibe.data.json : Json;


    enum marsConf = MarsServer
        .ExposeSchema(starwarsSchema())
        .PostgreSQL("127.0.0.1", 5432, "starwars")
        .Autologin("jedi", "force")
        ;

    string exposedMethods(MarsClient marsClient, string methodName, Json parameters){
        switch(methodName){
            case "methodA": 
                int i = parameters["i"].get!int;
                return "responseA_%d".format(++i);
            default: return "unknownMethod";
        }
    }

    marsServer = new MarsServer(marsConf);
    marsServer.serverSideMethods = &exposedMethods;
    enum ctTables = marsConf.schemaExposed.tables;
    InstantiateTables!(ctTables)(marsServer, [], [], [], [], [], [], []);

}

void setupWebSocketServer()
{
    import vibe.http.router : URLRouter;
    import vibe.http.server : HTTPServerSettings, listenHTTP;
    import vibe.http.websockets : handleWebSockets;

    auto router = new URLRouter();
    registerMarsEndpoints(router);

    auto settings = new HTTPServerSettings();
    settings.port = 8082;
    settings.bindAddresses = ["::1", "0.0.0.0"];
    listenHTTP(settings, router);
}
