/**
 * Verifica che tipo di client si sta connettendo, e attiva un protocollo
 * corretto di conseguenza
 */

module mars.protohelo;

import vibe.core.log;
import mars.server;
import mars.protomars;

/**
 * Handle the HELO with the connected entity on the other side of the websocket, and start handling it.
 * When everything it's done, just return, and the connection will be closed.
 * 
 * Params:
 *      socket : Template, proxied socket type, with Task based sync 'send' and 'receive' methods.
 */
void protoHelo(T)(T socket)
{
    // ... verify that it's a mars client ...
    if(auto helo = socket.receive() != "mars" )
    {
        logError("mars - client connected, but the helo is wrong:%s", helo);
        return;
    }
    socket.send("marsserver0000");

    // ... based on the client id, the server will instantiate or retrieve a client-side structure
    auto clientId = socket.receive();
    assert(marsServer !is null);
    auto marsClient = marsServer.engageClient(clientId);
    auto reply = marsClient.reconnections.length > 1? "marsreconnected" : "marswelcome";
    socket.send(reply);

    // ... we are supporting only one version of the protocol right now, let's use it ...
    protoMars!T(marsClient, socket);

    // ... we have done, inform the server that this client socket is no more active.
    marsServer.disposeClient(marsClient);
} 

