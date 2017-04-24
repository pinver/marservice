module mars;

import vibe.http.router;

void registerMarsEndpoints(URLRouter router)
{
    import vibe.http.websockets : handleWebSockets;
    import mars.websocket : handleWebSocketConnectionClientToService, handleWebSocketConnectionServiceToClient;

    router.get("/ws_c2s", handleWebSockets(&handleWebSocketConnectionClientToService));
    router.get("/ws_s2c", handleWebSockets(&handleWebSocketConnectionServiceToClient));
}

