
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

    // reject multiple authentication request ...
    if( client.authorised ){
        socket.sendReply(authenticationRequest, AuthenticationReply(AuthenticationReply.alreadyAuthorised));
        return;
    }

    // empty username, not allowed ...
    if( username == "" ){
        socket.sendReply(authenticationRequest, AuthenticationReply(AuthenticationReply.invalidUsername));
        return;
    }


    auto seed = letters.length
        .iota
        .randomSample(10)
        .map!( i => letters[i] )
        .array;
    socket.sendReply(authenticationRequest, AuthenticationReply(AuthenticationReply.seedProvided, seed));
    auto authenticateRequest = socket.receiveMsg!AuthenticateRequest;
    logInfo("S <-- %s | authenticate request, hash:%s", client.id, authenticateRequest.hash);

    // ... right now, we can't pass the hash to postgres, so ...
    string hash256, password = "password";
    if     ( username == "dev"    ){ password = "password"; }
    else if( username == "pinver" ){ password = "arathorn"; }
    else if( username == "elisa"  ){ password = "seta"; }
    else if( username == "chiara" ){ password = "velluto"; }

    bool authorised = authenticateRequest.hash.toUpper() == sha256Of(seed ~ sha256Of(password).toHexString()).toHexString();

    AuthoriseError dbAuthorised = client.authoriseUser(username, password);

    auto reply = AuthenticateReply(cast(int)dbAuthorised, "", []);

    if( dbAuthorised == AuthoriseError.authorised ){
        reply.sqlCreateDatabase = marsServer.configuration.alasqlCreateDatabase;
        reply.sqlStatements = marsServer.configuration.alasqlStatements;
        reply.jsStatements = marsServer.configuration.jsStatements;

        // ... now that the client is authorised, expose the data to it
        marsServer.createClientSideTablesFor(client);
    }

    logInfo("S --> %s | authenticate reply, authorised:%s", client.id, dbAuthorised);
    socket.sendReply(authenticateRequest, reply);

    // ... try the push from the server, a new client has connected ...
    //if(dbAuthorised == AuthoriseError.authorised) marsServer.broadcast(WelcomeBroadcast(username));
}

void protoDeauth(S)(MarsClient* client, S socket)
{
    marsServer.wipeClientSideTablesFor(client);
    auto request = socket.binaryAs!DiscardAuthenticationRequest;
    client.discardAuthorisation();
    auto reply = DiscardAuthenticationReply();
    socket.sendReply(request, reply);

    //marsServer.broadcast(GoodbyBroadcast("theusename"));
}

struct WelcomeBroadcast { static immutable type = MsgType.welcomeBroadcast; string username; }
struct GoodbyBroadcast { static immutable type = MsgType.goodbyBroadcast; string username; }
