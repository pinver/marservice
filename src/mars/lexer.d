
module mars.lexer;

enum TType { identifier, eof,
    comma, dot, dollar,
    lparen, rparen, 
    from, insert, into, returning, select, values, } 

struct Token {
	TType t; string v;
}

Token[] scan(string s){
	import std.algorithm : canFind, startsWith, strip;

	Token[] tokens;
	while(s.length){
		if(s.startsWith("select")){ tokens ~= Token(TType.select); s = s["select".length .. $]; }
                else if(s.startsWith("insert")){ tokens ~= Token(TType.insert); s = s["insert".length .. $]; }
		else if(s.startsWith("from")){ tokens ~= Token(TType.from); s = s["from".length .. $]; }
		else if(s.startsWith("into")){ tokens ~= Token(TType.into); s = s["into".length .. $]; }
		else if(s.startsWith("returning")){ tokens ~= Token(TType.returning); s = s["returning".length .. $]; }
		else if(s.startsWith("values")){ tokens ~= Token(TType.values); s = s["values".length .. $]; }
		else if(s.startsWith(",")){ tokens ~= Token(TType.comma); s = s[1 .. $]; }
		else if(s.startsWith(".")){ tokens ~= Token(TType.dot); s = s[1 .. $]; }
		else if(s.startsWith("(")){ tokens ~= Token(TType.lparen); s = s[1 .. $]; }
		else if(s.startsWith(")")){ tokens ~= Token(TType.rparen); s = s[1 .. $]; }
		else if(s.startsWith("$")){ tokens ~= Token(TType.dollar); s = s[1 .. $]; }
		else {
			auto t = Token(TType.identifier);
			while(s.length && ! ",.()$ ".canFind(s[0]) ){
				t.v ~= s[0]; s = s[1 .. $];
			}
			tokens ~= t;
		}
		s = s.strip(' ');
		//import std.stdio; writeln(s);
	}
        tokens ~= Token(TType.eof);
	return tokens;
}
