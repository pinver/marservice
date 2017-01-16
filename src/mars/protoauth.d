
module mars.protoauth;

import std.algorithm;
import std.ascii;
import std.random;
import std.range;
import std.digest.sha;
import std.string;
import std.experimental.logger;
import vibe.core.log;

import mars.msg;
import mars.client;
import mars.server;

void protoAuth(S)(MarsClient* client, S socket)
{
    import msgpack : unpack, pack;

    auto authenticationRequest = socket.binaryAs!AuthenticationRequest;
    string username = authenticationRequest.username;

    auto seed = letters.length
        .iota
        .randomSample(10)
        .map!( i => letters[i] )
        .array;
    socket.sendReply(authenticationRequest, AuthenticationReply(seed));

    auto authenticateRequest = socket.receiveMsg!AuthenticateRequest;
    logInfo("S <-- C | authenticate request, hash:%s", authenticateRequest.hash);
    logInfo("password hash:%s", sha256Of("password").toHexString());
    string hash256 = sha256Of(seed ~ sha256Of("password").toHexString()).toHexString();
    bool authorised = authenticateRequest.hash.toUpper() == hash256;
    bool dbAuthorised = client.authoriseUser(username, "postgres");
    //logInfo("client authorised? %s", authorised); 

    auto reply = AuthenticateReply(! authorised);
    reply.sqlCreateDatabase = marsServer.configuration.alasqlCreateDatabase;
    reply.sqlStatements = marsServer.configuration.alasqlStatements;
    logInfo("S --> C | authenticate reply, authorised:%s", authorised);
    socket.sendReply(authenticateRequest, reply);

    // ... try the push from the server, a new client has connected ...
    marsServer.broadcast(WelcomeBroadcast(username));
}

void protoDeauth(S)(MarsClient* client, S socket)
{
    auto request = socket.binaryAs!DiscardAuthenticationRequest;
    client.discardAuthorisation();
    auto reply = DiscardAuthenticationReply();
    socket.sendReply(request, reply);

    marsServer.broadcast(GoodbyBroadcast("theusename"));
}

struct WelcomeBroadcast { static immutable type = MsgType.welcomeBroadcast; string username; }
struct GoodbyBroadcast { static immutable type = MsgType.goodbyBroadcast; string username; }
