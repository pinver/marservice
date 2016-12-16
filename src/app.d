


//import ;

int main()
{
    import vibe.core.core : runApplication;

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
