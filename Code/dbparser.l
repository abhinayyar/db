%{
#include <stdio.h>
#include "y.tab.h"
extern YYSTYPE yylval;
%}
%%
SELECT {return SELECT;}
CREATE {return CREATE;}
TABLE {return TABLE;}
FROM {return FROM;}
WHERE {return WHERE;}
INT {return INTA;}
OR {return OR;}
AND {return AND;}
ORDER {return ORDER;}
BY {return BY;}
INSERT {return INSERT;}
VALUES {return VALUES;}
DROP {return DROP;}
DELETE {return DELETE;}
INTO {return INTO;}
DISTINCT {return DISTINCT;}
STR20 {return STR20A;}
"="	|
"<>" 	|
"<"	|
">"	|
"<="	|
">="		{yylval.string = strdup(yytext); return COMPARISON; }
\( {return ATTSTART;}
\) {return ATTEND;}
, {return COMA;}
\* { yylval.string = "*"; return ALLVAL;}
\. {return DOT;}
\+ {return PLUS;}
\- {return MINUS;}
[a-z][a-z|0-9]*	{ yylval.string = strdup(yytext); return NAME; }
["A-Za-z0-9]*["] {yylval.string = strdup(yytext); return LITERAL; }
[0-9]+	|
[0-9]+"."[0-9]* |
"."[0-9]*	{ yylval.number=atoi(yytext); return INTEGER; }
'[^'\n]*'	{
		int c = input();

		unput(c);	/* just peeking */
		if(c != '\'') {
			yylval.string=strdup(yytext);
			return STRING;
		} else
			yymore();
	}
[ \t\r]+	;	/* white space */		
%%
