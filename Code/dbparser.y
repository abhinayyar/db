%{
#include <stdio.h>
#include <string.h>
#include "parse_tree.h"
//#define PRINT_PARSE_TREE 1
extern FILE *yyin;
query_operator *ptr_qop = NULL;
static int current_statement =-1;
// 0 -> SELECT , 1-> CREATE , 2 -> DELETE , 3 -> INSERT , 4 -> DROP 
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
	// init tracker
	ptr_qop = new query_operator();	
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
%token SELECT CREATE FROM TABLE ATTSTART ATTEND COMA DOT DISTINCT WHERE OR AND
%token MINUS PLUS ORDER BY INSERT INTO VALUES DROP DELETE
%token <number>INTEGER 
%token <string> STRING
%token <string> NAME
%token <string> LITERAL
%token <string> COMPARISON
%token <string> ALLVAL
%token <string> INTA
%token <string> STR20A

%%

commands: /* empty */
               | commands command
               ;
       command:
               create_table_statement
	       |
	       select_statement
	       |
	       insert_statement
	       |
	       drop_table_statement
	       |
	       delete_statement
               ;
       delete_statement:
       {
       		ptr_qop->tree_parse.push(new query_tree("delete_statement"));
			ptr_qop->root_node = ptr_qop->tree_parse.top();
			current_statement=4;
       }
		DELETE FROM table_name delete_cond
		{
			//printf("\t Delete Query Executed \n");
			#ifdef PRINT_PARSE_TREE
			ptr_qop->print_select_tree();
			#endif
			ptr_qop->clear_select();
			while(ptr_qop->tree_parse.empty()==false ) ptr_qop->tree_parse.pop();
			ptr_qop->exp_parse.clear();
			ptr_qop->where_parse.clear();
			current_statement = -1;
			ptr_qop->get_del_list(ptr_qop->root_node);
			ptr_qop->execute_query(DELETEQ);
			ptr_qop->clear_del();
			ptr_qop->clear_select();


		}

		;
       delete_cond:
		
		|
		WHERE search_condition
		;
       drop_table_statement:
		DROP TABLE table_name
		{
			// ptr_qop->table_name = 
			ptr_qop->execute_query(DROPQ);
			//printf("\t Drop Query Executed \n");
		}
		;
       insert_statement:
		INSERT INTO table_name  ATTSTART attribute_list ATTEND insert_tuples
		{
			ptr_qop->execute_query(INSERTQ);
			//printf("\t Insert Query Executed now \n");
		}
		;
       attribute_list:
		attribute_name
		|
		attribute_name COMA attribute_list
		;
       insert_tuples:
		VALUES ATTSTART value_list ATTEND
		|
		select_statement
		;
       value_list:
		value
		|
		value COMA value_list
		;
       value:
		LITERAL
		{	
			ptr_qop->attr_value_list.push_back($1);
			ptr_qop->attr_type_list.push_back("STR20A");
		}
		|
		INTEGER
		{	
			ptr_qop->attr_value_list.push_back(to_string($1));
			ptr_qop->attr_type_list.push_back("INTA");
		}
		;	
       select_statement:
		{
			current_statement = 0;
			ptr_qop->select_query_track.push(new select_query());	
			ptr_qop->tree_parse.push(new query_tree("select_statement"));
			ptr_qop->root_node = ptr_qop->tree_parse.top();
			
		}
		SELECT
		{
			
			if(ptr_qop->tree_parse.empty()==false) {
				ptr_qop->tree_parse.top()->next.push_back(new query_tree("SELECT"));
			}	
		} 
		select_type
		{
			query_tree *node = new query_tree("select_list");
			if(ptr_qop->tree_parse.size()!=0) {
				ptr_qop->tree_parse.top()->next.push_back(node);
				ptr_qop->tree_parse.push(node);
				query_tree *subnode = new query_tree("select_sublist");
				ptr_qop->tree_parse.top()->next.push_back(subnode);
				ptr_qop->tree_parse.push(subnode);
			}
			
		} 
		select_list
		{
			// pop till we have select_statement
			while(ptr_qop->tree_parse.empty()==false 
				&& ptr_qop->tree_parse.top()->name.compare("select_statement")!=0) {
				ptr_qop->tree_parse.pop();
			}
		} 
		FROM 
		{
			if(ptr_qop->tree_parse.size()!=0) {
			ptr_qop->tree_parse.top()->next.push_back(new query_tree("FROM"));
			query_tree *node = new query_tree("table_list");
			ptr_qop->tree_parse.top()->next.push_back(node);
			ptr_qop->tree_parse.push(node);
			}
		}
		table_list 
		{
			// pop table list values here
			while(ptr_qop->tree_parse.empty()==false 
				&& ptr_qop->tree_parse.top()->name.compare("select_statement")!=0) {
				ptr_qop->tree_parse.pop();
			}	
		} 
		condition_list
		{
			while(ptr_qop->tree_parse.empty()==false ) ptr_qop->tree_parse.pop();
			#ifdef PRINT_PARSE_TREE
			ptr_qop->print_select_tree();
			#endif
			ptr_qop->execute_query(SELECTQ);
		}
		;
       condition_list:
		
		|
		WHERE 
		search_condition
		{
		
		}
		| 
		WHERE
		search_condition order_by 
		{
		
		}
		|
		order_by
		;
       order_by:
		ORDER BY column_name_od;
		;
	   column_name_od:
	   		NAME
	   		{
	   			ptr_qop->is_orderby = true;
	   			ptr_qop->orderby_value.assign($1);
	   		}
	   		|
	   		NAME DOT NAME
	   		{
	   			ptr_qop->is_orderby = true;
	   			string tmp = $1;
				tmp.push_back('.');
				tmp+=$3;
				ptr_qop->orderby_value.assign(tmp);
	   		}
	   		;
       search_condition:
		boolean_term
		{
			if(ptr_qop->tree_parse.size()>0) {
			ptr_qop->tree_parse.top()->next.push_back(new query_tree("WHERE"));
			query_tree *node = new query_tree("search_condition");
			ptr_qop->tree_parse.top()->next.push_back(node);
			ptr_qop->tree_parse.push(node);
			if(ptr_qop->where_parse.size()!=0) {
				query_tree *nd = ptr_qop->where_parse.front();
				ptr_qop->tree_parse.top()->next.push_back(nd);
				ptr_qop->tree_parse.push(nd);
				ptr_qop->where_parse.clear();
			}
			}
		}
		|
		boolean_term
		{
			
			if(ptr_qop->where_parse.size()!=0) {
			
				while(ptr_qop->where_parse.empty()==false &&
						ptr_qop->where_parse.back()->name.compare("b_term")!=0) {
						ptr_qop->where_parse.pop_back();
				}
				
			} 
		} 	
		OR
		{
			query_tree *node = new query_tree("OR");
			ptr_qop->where_parse.back()->next.push_back(node);
		} 
		search_condition
		;
       boolean_term:
		boolean_factor
		{
			query_tree *bterm = new query_tree("b_term");
			if(ptr_qop->where_parse.size()==0) {
				ptr_qop->where_parse.push_back(bterm);
			} else {
				ptr_qop->where_parse.back()->next.push_back(bterm);
				ptr_qop->where_parse.push_back(bterm);
			}
			query_tree *bf = new query_tree("b_facor");
			ptr_qop->where_parse.back()->next.push_back(bf);
			ptr_qop->where_parse.push_back(bf);
			while(ptr_qop->exp_parse.empty()==false) {
				ptr_qop->where_parse.back()->next.push_back(ptr_qop->exp_parse[0]);
				ptr_qop->exp_parse.erase(ptr_qop->exp_parse.begin());
			}
		}
		|
		boolean_factor
		{
			
			query_tree *bterm = new query_tree("b_term");
			if(ptr_qop->where_parse.size()==0) {
				ptr_qop->where_parse.push_back(bterm);
			} else {
				ptr_qop->where_parse.back()->next.push_back(bterm);
				ptr_qop->where_parse.push_back(bterm);
			}
			query_tree *bf = new query_tree("b_facor");
			ptr_qop->where_parse.back()->next.push_back(bf);
			ptr_qop->where_parse.push_back(bf);
			while(ptr_qop->exp_parse.empty()==false) {
				ptr_qop->where_parse.back()->next.push_back(ptr_qop->exp_parse[0]);
				ptr_qop->exp_parse.erase(ptr_qop->exp_parse.begin());
			}
		} 
		AND 
		{
			while(ptr_qop->where_parse.empty()==false &&
						ptr_qop->where_parse.back()->name.compare("b_term")!=0) {
						ptr_qop->where_parse.pop_back();
			}
			query_tree *bf = new query_tree("AND");
			ptr_qop->where_parse.back()->next.push_back(bf);
		} 
		boolean_term
		;
       boolean_factor:
		expression 
		COMPARISON
		{
			ptr_qop->exp_parse.push_back(new query_tree($2));
		} 
		expression
		;
       expression:
		term 
		|
		ATTSTART 
		term
		PLUS
		{
			ptr_qop->exp_parse.push_back(new query_tree("+"));	
		} 
		term
		ATTEND
		|	
		ATTSTART 
		term
		MINUS
		{
			ptr_qop->exp_parse.push_back(new query_tree("-"));	
		} 
		term
		ATTEND
		|	
		ATTSTART 
		term
		ALLVAL
		{
			ptr_qop->exp_parse.push_back(new query_tree("*"));	
		} 
		term
		ATTEND
		;
       term:
		column_name_cond
		|
		INTEGER
		{
			ptr_qop->exp_parse.push_back(new query_tree(to_string($1)));
			ptr_qop->exp_parse.back()->is_val=true;
		}
		|
		LITERAL
		{
			ptr_qop->exp_parse.push_back(new query_tree($1));
			ptr_qop->exp_parse.back()->is_val=true;		
		}		
		;
       column_name_cond:
		NAME
		{
			ptr_qop->exp_parse.push_back(new query_tree($1));
			
		}
		|
		NAME DOT NAME
		{	
			string tmp = $1;
			tmp.push_back('.');
			tmp+=$3;
			ptr_qop->exp_parse.push_back(new query_tree(tmp));
		}
		;
       table_list:
		table_name
		|
		table_name COMA table_list
		|
		ATTSTART select_statement ATTEND 
		;
       select_type:
		
		|	
		DISTINCT
		{	
			ptr_qop->is_distinct = true;
		}
		;
       select_list:
		ALLVAL
		|
		select_sublist 
		;
       select_sublist:
		column_name
		|
		column_name COMA select_sublist
		;
       column_name:
		NAME
		{
			if(current_statement==0 && ptr_qop->tree_parse.size()!=0) {
				ptr_qop->tree_parse.top()->next.push_back(new query_tree($1));
			}
		}
		|
		NAME DOT NAME
		{
			if(current_statement==0 && ptr_qop->tree_parse.size()!=0) {
				string tmp = $1;
				tmp.push_back('.');
				tmp+=$3;
				ptr_qop->tree_parse.top()->next.push_back(new query_tree(tmp));
			}
		}
		;	
    create_table_statement:
       CREATE TABLE table_name ATTSTART attribute_type_list ATTEND
       {
       		   ptr_qop->execute_query(CREATEQ);
       }
       ;
		attribute_type_list:
			attribute_name data_type
			|
			attribute_name data_type COMA attribute_type_list
			;
		attribute_name:
			NAME
			{
				

				ptr_qop->attr_name_list.push_back($1);
			}
			;
		table_name:
			NAME
			{	
				if((current_statement ==0 || current_statement == 4) && ptr_qop->tree_parse.size()!=0)
				ptr_qop->tree_parse.top()->next.push_back(new query_tree($1));
				ptr_qop->table_name = $1;
				
			}
			;
		data_type:
			INTA
			{	
				ptr_qop->attr_type_list.push_back("INTA");
				
			}
			|
			STR20A

			{
				ptr_qop->attr_type_list.push_back("STR20");
				
			}
			; 
%%
