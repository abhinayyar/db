%{
#include <stdio.h>
#include <string.h>
extern FILE *yyin;
extern "C"
{
    int yyparse(void);
    int yylex(void);  
    int yywrap()
    {
       return 1;
    }
	
}
void yyerror(const char *str)
{
        fprintf(stderr,"error: %s\n",str);
}
int main(int ac, char **av)
{
	if(ac > 1 && (yyin = fopen(av[1], "r")) == NULL) {
		perror(av[1]);
		return 1;
	}

	if(!yyparse())
		printf("SQL parse worked\n");
	else
		printf("SQL parse failed\n");

	return 0;
}
%}
%union
{
int number;
char *string;
}
%token SELECT CREATE FROM TABLE NAME 
%token <number>INT 
%token <string> STRING
%%
commands: /* empty */
               | commands command
               ;
       command:
               create_table_statement
               ;
       create_table_statement:
               CREATE TABLE table_name attribute_type_list 
               {
                       printf("\tcreate table statement \n");
               }
	       |
	       CREATE NAME
               {
			printf("No table\n");
	       }
               ;
	attribute_type_list:
		attribute_name data_type
		|
		attribute_name data_type ',' attribute_type_list
		;
	attribute_name:
		NAME
		|
		NAME '.' NAME 
		;
	table_name:
		NAME
		|
		NAME '.' NAME
		;
	data_type:
		INT
		|
		STRING
		; 
%%

