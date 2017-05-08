#ifndef __SELECT_H__
#define __SELECT_H__
#include <iostream>
#include <vector>
#include <string>
#include <stack>
using namespace std;
class search_sub_query
{
	public:
	bool is_sep; // AND / OR node
	string val;
	vector<string> query_str;
	vector<search_sub_query*> nested_list;
	search_sub_query(vector<string> query_str) {
		this->query_str = query_str;
		this->is_sep = false;
	}
	
};

class select_query
{
	public:
	bool is_nested;
	bool is_distinct;
	vector<string> select_list;
	vector<string> table_list;
	// table name for from clause, should have atleast one value	
	vector<string> table_name_from;
	select_query *sub_select_query;
	// attributes name for select , empty-> select all
	vector<string> attr_list_select;
	// condition list for where clause, empty -> no where condition , " exp + exp = exp"
	vector<search_sub_query*> cond_list_where;
	vector<string> where_cond;
	select_query()
	{
		sub_select_query = NULL;
		is_distinct = false;
		is_nested = false;
	}
};
#endif // selct_h
