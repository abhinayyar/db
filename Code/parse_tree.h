#ifndef __PARSE_TREE_H__
#define __PARSE_TREE_H__
#include "select.h"
#include<stack>
#include<cassert>
#include "StorageSingleton.h"
#include<cstdlib>
#include<ctime>
#include<string>
#include<utility>
#include<unordered_map>
#include<unordered_set>
#include<algorithm>
#include<sstream>
#include<fstream>
#include<set>
#include "Block.h"
#include "Config.h"
#include "Disk.h"
#include "Field.h"
#include "MainMemory.h"
#include "Relation.h"
#include "Schema.h"
#include "SchemaManager.h"
#include "Tuple.h"
#include "sqltree.h"


enum query_type {
	SELECTQ=0,
	CREATEQ,
	INSERTQ,
	DELETEQ,
	DROPQ
};
typedef unordered_map<string,pair<vector<vector<string> >,vector<string> > > TABLE_ENTERIES;
typedef pair<vector<vector<string> >,vector<string> > EACH_ENTRY;
typedef vector<vector<string> > D_TABLE;
typedef vector<pair<string,pair<string,bool> > > EXPR_TRACK;
typedef pair<string,pair<string,bool> > IND_EXPR;
typedef unordered_map<int,unordered_set<string> > KEY_TRACK;

class lcl_tuple {
	public:
		string value;
		string field;
		string type;
};
class query_operator {
	public:
		clock_t start_time;
		int max_blocks;
		int cur_block;
		unordered_map<string,int> prec;
		ofstream ofile;
		bool is_distinct;
		bool is_orderby;
		string orderby_value;
	query_operator() {
		// storageManager
  		start_time=clock();
  		max_blocks=0;
  		cur_block=0;
  		set_prec();
  		ofile.open("output.txt"/*,std::ios_base::app*/);
  		is_distinct=false;
  		is_orderby=false;
	}
	
	// result tables
	TABLE_ENTERIES parsed_table_list;

	//Create table 
	string table_name;
	vector<string> attr_name_list;
	vector<string> attr_type_list;
	vector<string> attr_value_list;
	struct Local {
    Local(int paramA) { this->paramA = paramA; }
    bool operator () (vector<string> A, vector<string> B) {
    	if(isdigit(A[paramA][0]) && isdigit(A[paramA][0])) {
    		return stoi(A[paramA]) < stoi(B[paramA]);
    	} 
    	return A[paramA] < B[paramA];
    }

    int paramA;
	};
	// //INSERT INTO
	// vector<string> insert_attr_list;
	unordered_set<string> table_track;
	unordered_map<string,D_TABLE> query_join_tables;
	void appendTupleToRelation(Relation* relation_ptr, MainMemory& mem, int memory_block_index, Tuple& tuple);
	vector<string> get_entire_table(pair<vector<vector<string> >,vector<string> >& table,string table_name);
	void add_to_record();
	pair<D_TABLE,string> execute_where_condition(query_tree *root);
	pair<D_TABLE,string> process_b_term(query_tree *root);
	vector<vector<string> > condition_join(pair<D_TABLE,string> left, pair<D_TABLE,string> right,string condition);
	EXPR_TRACK form_postfix(EXPR_TRACK experssion);
	pair<D_TABLE,string> evaluate_postfix(EXPR_TRACK postfix);
	void extract_col(vector<string>& tmp,int col_index,D_TABLE table);
	string perform_oper(string A,string B,string oper);
	bool check_cond(string A,string B,string oper);
	D_TABLE evaluate_natural_join(EXPR_TRACK postfix);
	D_TABLE perform_and(D_TABLE left, D_TABLE right,string table_a,string table_b);
	D_TABLE perform_or(D_TABLE left,D_TABLE right,string table_a,string table_b);
	void prepare_track(unordered_map<string,pair<vector<string>,int> >& track_left,unordered_map<string,pair<vector<string>,int> >& track_right,D_TABLE left,D_TABLE right);
	void compute_cross_join(TABLE_ENTERIES& parsed_table_list_lcl,string& table_name);
	void append_table_name(vector<string>& field,string table_name);
	D_TABLE two_table_cross_join(D_TABLE A,D_TABLE B);
	bool checked_already_crossjoin(string table_a,string table_b);
	int get_col_index(vector<string> tmp,string name);
	vector<bool> prepare_true_table(D_TABLE org,D_TABLE cur);
	void load_table_from_memory(EACH_ENTRY& table,string table_name);
	void test_insert_query(Relation* relation_ptr);
	D_TABLE join_table(string table_a,string table_b,string& join_table,vector<string>& field);
	vector<string> compute_join_results(string table_a,string table_b,string& join_table,string cola,string colb,string oper);
	stack<select_query*> select_query_track;

	// new add
	void remove_common(D_TABLE& right,set<string> common_table_join,string table_name);
  	void refine_table(vector<int> match_index,D_TABLE& table);
  	void get_common_index(vector<int>& left_match_index,vector<int>& right_match_index,unordered_map<string,vector<int> > left,unordered_map<string,vector<int> > right,string table_name);
	unordered_map<string,vector<int> > form_common_key(D_TABLE table,set<string> common_table_join,string table_name);
	string form_key(vector<string> tuple,vector<int> common_index);
	vector<int> form_common_index(vector<string> field,set<string> common_table_join);
	set<string> find_common_tables(string table_a,string table_b);
	string refine_table_name(string table_name);
	pair<vector<vector<string> >,string> compute_normal_result();
	pair<vector<vector<string> >,string> do_projection(pair<vector<vector<string> >,string> to_project,vector<string>& field);
	set<string> refine_project_cond(); 
	vector<int> extract_col_index(vector<string> field,set<string> mod_project);
	void get_or_common(vector<int>& left_match_index,vector<int>& right_match_index,unordered_map<string,vector<int> > left,unordered_map<string,vector<int> > right);
	void simplify_table_values(KEY_TRACK& key,D_TABLE tab,vector<string> tables,string table_name);
	void fill_track(int index,vector<string> tuple,vector<string> tables,KEY_TRACK& key,string table_name);
	int get_index(vector<string> field,string val);
	// end

	// latest add 
	D_TABLE get_table_keys(D_TABLE left,D_TABLE right,string table_name);
	D_TABLE find_common(KEY_TRACK& left_key,KEY_TRACK& right_key,D_TABLE left);
	bool compare(unordered_set<string> A,unordered_set<string> B);
	D_TABLE get_common(D_TABLE A,D_TABLE B,string common_ls,string table_name);
	D_TABLE extract_complete_col(D_TABLE A,vector<int> c_index);
	vector<int> extra_col_index_val(vector<string> field,set<string> c_value);
	vector<int> and_element(D_TABLE A,D_TABLE B);
	void order_by(D_TABLE& res,string val,string table_name);

	string getNormalCrossJoin(string table_one, string table_two);
	Tuple computeTupleJoin(Tuple& a, Tuple& b, Relation* join_relation_ptr);
	void onePassJoin(Relation* R, Relation* S, Relation* join_relation_ptr);
	void twoPassJoin(Relation* R, Relation* S, Relation* join_relation_ptr);
	void performVectorTupleJoin(vector<Tuple>& r_tuples, 
                                            vector<Tuple>& non_modifying_tuples,
                                            Relation* join_relation_ptr,
                                            int memory_block_index,
                                            Block* modified_block_ptr);
	D_TABLE compute_normal_result_with_cross(string& table_name,vector<string>& field_names);
    void drop_cross_join(string join_table_name); 
    D_TABLE get_entire_table_cross(vector<string>& field_names, string table_name);
    D_TABLE load_table_from_memory_cross(string table_name,vector<string>& field_val);
    D_TABLE compute_where_result_with_cross(string table_name,vector<string>& field,string& cross_join);
	void delete_all(string table_name);
	void delete_specific(D_TABLE cond,string table_name);
	/* ----- new code */
	stack<query_tree*> tree_parse;
	deque<query_tree*> where_parse;
	vector<query_tree*> exp_parse;
	query_tree *root_node = NULL;
	vector<vector<string> > tree_str;
	vector<string> select_list;
	vector<string> table_list;
	query_tree *search_cond_node = NULL;
	void print_sim_track(KEY_TRACK key) {
		for(pair<int,unordered_set<string> > p : key) {
			cout << p.first << "\t";
			for(string s : p.second)
				cout << s << "\t";
			cout << endl;
		}
	}
	bool is_comp(string oper) {
		if(oper.compare("<=") ==0 
          || oper.compare(">=") ==0 
          || oper.compare("<") ==0 
          || oper.compare(">") ==0 
          || oper.compare("=") ==0 ) {
			return true;
		}
		return false;
	}
	string get_table_name(string table) {
		auto it = table.find(".");
		if(it!=string::npos) {
			return table.substr(0,it);
		}
		return "";
	}
	void set_prec() {
		prec.insert(make_pair("=",0));
		prec.insert(make_pair("<=",0));
		prec.insert(make_pair(">=",0));
		prec.insert(make_pair("<",0));
		prec.insert(make_pair(">",0));
		// operator
		prec.insert(make_pair("+",1));
		prec.insert(make_pair("-",1));
		prec.insert(make_pair("*",2));
		prec.insert(make_pair("/",2));
	}
	void print_field(vector<string> field) {
		//for(string s : field) {
		//	cout << s << "\t";
		//}
		//cout << endl;
	}
	string get_key(vector<string> tmp) {
		string res;
		for(string s : tmp) {
			res = res + s+"#";
		}
		res.pop_back();
		return res;
	}
	bool is_sub(string table_a,string table_b,string& common_ls) {
		set<string> split_a = split(table_a,'#');
		set<string> split_b = split(table_b,'#');
		for(string s : split_b) {
			if(split_a.count(s) > 0) {
				split_a.erase(s);
				common_ls+=s+"#";
			}
		}
		
		common_ls.pop_back();
		if(split_a.size()==0) {
			return true;
		} else {
			common_ls.clear();
		}
		return false;
	}
	set<string> split(string input,char del) {
		stringstream ss(input);
		set<string> res;
		string tmp;
		while(getline(ss,tmp,del)) {
			res.insert(tmp);
		}
		return res;
	}
	vector<string> split_v(string input,char del) {
		stringstream ss(input);
		vector<string> res;
		string tmp;
		while(getline(ss,tmp,del)) {
			res.push_back(tmp);
		}
		return res;
	}
	void clear_select() {
		parsed_table_list.clear();
		select_list.clear();
		table_list.clear();
		search_cond_node=NULL;
		is_distinct = false;
		is_orderby = false;
		query_join_tables.clear();
	}
	void get_height(query_tree *root,int& ht,int level) {
		if(!root) return;
		ht= max(ht,level);
		for(int i=0;i<root->next.size();i++) {
			get_height(root->next[i],ht,level+1);
		}
	}
	void traverse(query_tree *root,int level) {
		if(!root) return;
		tree_str[level].push_back(root->name);
		for(int i=0;i<root->next.size();i++) {
			traverse(root->next[i],level+1);
		}
	}
	unordered_map<string,pair<string,string> > map_index(vector<string> attr_name,vector<string> attr_value,vector<string> attr_type) {
		unordered_map<string,pair<string,string> > track;
		assert(attr_name.size() == attr_value.size());
		for(int i=0;i<attr_name.size();i++) {
			track.insert(make_pair(attr_name[i],make_pair(attr_value[i],attr_type[i])));
		}
		return track;
	}
	void print_select_tree() {
		int ht=0;
		get_height(root_node,ht,1);
		tree_str.resize(ht);
		traverse(root_node,0);
		print_tree_structure();
		tree_str.clear();
	}
	void print_tree_structure() {
		int ct =0;
		for(vector<string> str : tree_str) {
			cout << ct << "\t";
			for(string s : str) {
				cout << s << "\t";
			}
			cout << endl;
			ct++;
		}
	}
	void clear_del() {
		table_list.clear();
		search_cond_node=NULL;
		parsed_table_list.clear();
		query_join_tables.clear();
	}
	void execute_delete_query();
	void get_del_list(query_tree *root) {

		if(!root) return;
		table_list.push_back(root->next[0]->name);
		if(root->next.size()==3)
			search_cond_node=root->next[2];
		 
	}
	void get_attr_list(query_tree *root) {
		if(!root) return;
		if(root->name.compare("select_sublist")==0) {
			for(int i=0;i<root->next.size();i++) {
				select_list.push_back(root->next[i]->name);
			}
		} else if(root->name.compare("table_list")==0) {
			for(int i=0;i<root->next.size();i++) {
				table_list.push_back(root->next[i]->name);
			}
		} else if(root->name.compare("search_condition")==0) {
			search_cond_node=root;
		}else {
			for(int i=0;i<root->next.size();i++) {
				get_attr_list(root->next[i]);
			}
		}
	}
	void print_table(vector<vector<string> > table) {
		//for(vector<string> row : table) {
		//	for(string c : row) {
		//		cout << c <<"\t";
		//	}
		//	cout << endl;
		//}
	}
	void order_field(vector<string>& field,string table_name) {
		vector<string> actual = parsed_table_list[table_name].second;
		unordered_set<string> track;
		for(string s : field) track.insert(s);
		vector<string> res;
		for(string s : actual) {
			if(track.count(s)>0) res.push_back(s);
		}
		field.clear();
		field.insert(field.end(),res.begin(),res.end());
	}
	void print_table_to_console(vector<vector<string> > table,string table_name,vector<string> field) {

		order_field(field,table_name);
		//ofile << parsed_table_list.find(table_name)!=parsed_table_list.end() ? "1" : 0 << endl;
		table_name.assign(refine_table_name(table_name));
		cout  << "==============="<<table_name<<"================\n";
		
			for(string s : field)
				cout <<s << "\t";
			cout << endl;
		
		for(vector<string> row : table) {
			for(string c : row) {
				cout << c <<"\t";
			}
			cout << endl;
		}
		cout  << "===============================\n";
	}
	void print_table_to_file(vector<vector<string> > table,string table_name,vector<string> field) {

		order_field(field,table_name);
		//ofile << parsed_table_list.find(table_name)!=parsed_table_list.end() ? "1" : 0 << endl;
		table_name.assign(refine_table_name(table_name));
		ofile  << "==============="<<table_name<<"================\n";
		
			for(string s : field)
				ofile <<s << "\t";
			ofile << endl;
		
		for(vector<string> row : table) {
			for(string c : row) {
				ofile << c <<"\t";
			}
			ofile << endl;
		}
		ofile  << "===============================\n";
	}
	/*----------*/

	void execute_query(enum query_type qty);
	void clear_all_values();
	void empty_memory_block();

	//Select query specific
	void process_select_query();
	void execute_select_query();

	//Create query specific
	void process_create_table_query();
	void create_schema(std::vector<string>, std::vector<enum FIELD_TYPE>);
	
	//Insert query specific
	void process_insert_query();
	void insert_into(std::vector<string> attr_list, std::vector<string> value_list);
	void execute_insert_query();

	//Drop query specific
	void execute_drop_query();
	
	


	void print_attr_list(vector<string> attr) {
		//cout << "Attribute List  : ";
		//for(string s : attr) {
		//	cout << s << "\t";
		//}
		//cout << endl;
	}
	void print_table_list(vector<string> table) {
		//cout << "Table List  : ";
		//for(string s : table) {
		//	cout << s << "\t";
		//}
		//cout << endl;
	}
	void return_value_to_print(vector<search_sub_query*> nested_list) {
		for (int i=0;i<nested_list.size();i++) {
			if(nested_list[i]->nested_list.size()==0) {
				if(nested_list[i]->val.empty()==false) {
					cout << nested_list[i]->val << "\t";
				} else {
					for(string s : nested_list[i]->query_str) cout << s <<"\t";
				}
			} else {
				return_value_to_print(nested_list[i]->nested_list);
			}
		}	
	} 
	void print_cond_list(vector<search_sub_query*> cond_list_where) {
		for(int i=0;i<cond_list_where.size();i++) {
			if(cond_list_where[i]->nested_list.size()==0) {
				//cout << cond_list_where[i]->val << "\t";
			} else {
				return_value_to_print(cond_list_where[i]->nested_list);
			}
		}
		//cout << endl;
	}
	void print_where_list(vector<string> list) {
		for(string s : list){
			cout << s << "\t";
		}
		cout << endl;
	}
	void print_nested_query_str_select(select_query *root) {
		// in case of nested select query, this behaves as single linked list
		if(!root) return;
		cout << "================"<< endl;
		//print_attr_list(root->attr_list_select);	
		//print_table_list(root->table_name_from);
		print_where_list(root->where_cond);
		//print_cond_list(root->cond_list_where);
		print_nested_query_str_select(root->sub_select_query);	
	}		

};
#endif // parse tree
