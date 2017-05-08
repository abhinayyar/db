#ifndef __SQLTREE_H__
#define __SQLTREE_H__
#include<iostream>
#include<vector>
#include<string>
class query_tree {
	public:
	string name;
	bool is_val;
	query_tree(string name) {
		this->name=name;
		is_val=false;
	}
	vector<query_tree*> next;
};
#endif