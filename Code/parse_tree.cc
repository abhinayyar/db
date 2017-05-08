#include <iostream>
#include <iterator>
#include <string.h>
#include "parse_tree.h"
// uncommenet them for getting results in file and getting stats for time and disk IO
//#define PRINT_TO_FILE 1
#define SHOW_STATS 1
//#define CHECK_DROP_ACC 1
void query_operator::execute_query(enum query_type qry) {
	//cout << "INFO : " <<  __FUNCTION__ << endl;
	if(qry == SELECTQ) {
    //cout << "Going to execute select query\n";
		execute_select_query();
	}	
  if (qry == CREATEQ) {
    //cout << "Going to execute create query";
    process_create_table_query();
  }
  if (qry == INSERTQ) {
    //cout << "Going to execute insert query";
    execute_insert_query();
  }
  if (qry == DROPQ) {
   // cout << "Going to execute drop query";
    execute_drop_query();
  }
  if( qry ==  DELETEQ) {
    // cout << "Going to execute delete query";
      execute_delete_query();
  }
}

void query_operator::execute_delete_query() {
  pair<vector<vector<string> >,string> where_result;
   for(string table_name : table_list) {
    EACH_ENTRY tmp;
    parsed_table_list.insert(make_pair(table_name,tmp));
  }
  if(search_cond_node) {
     where_result = execute_where_condition(search_cond_node->next[0]);
     //cout << "Delete Specific tuples\n";
     //print_table(where_result.first);
     // excute code for where here
     delete_specific(where_result.first,where_result.second);
  } else {
    //cout << "Delete All tuples of relation\n";
    // execute the code for delete all tuples here
    assert(table_list.size()!=0);
    delete_all(table_list[0]);
  }
  clear_del();
}
// function for nested select -> not used to delete
void query_operator::process_select_query() {
	assert(select_query_track.size()!=0);
	select_query *cur = select_query_track.top();
	select_query_track.pop();
	if(select_query_track.size()>0) {
		// nested query condition true
		select_query_track.top()->sub_select_query = cur;
		//FIXME : Just tmp put here, need to put value we get from cur in this 
		select_query_track.top()->table_name_from.push_back("SUB QRY");
	} else {
		// last node to give final result
		print_nested_query_str_select(cur);
	}			
}	
void query_operator::empty_memory_block() {
  for(int i=0;i<max_blocks;i++) {
    Block* block_ptr=StorageSingleton::getInstance().mem.getBlock(i); //access to memory block 0
    block_ptr->clear();
  }
}
EXPR_TRACK query_operator::form_postfix(EXPR_TRACK experssion) {
  EXPR_TRACK postfix;
  stack<IND_EXPR> oper;
  for(IND_EXPR p : experssion) {
    //cout << p.first << "\t" << p.second.first << "\t" << p.second.second << endl;
    if(prec.find(p.second.first)!=prec.end()) {
      // operator found
      while(oper.empty()==false && prec[oper.top().second.first]>=prec[p.second.first]) {
        postfix.push_back(oper.top());
        oper.pop();
      }
      oper.push(p);
    } else {
      postfix.push_back(p);
    }
  }
  while(oper.empty()==false) {
      postfix.push_back(oper.top());
      oper.pop();
  }
  return postfix;
}
void query_operator::extract_col(vector<string>& tmp,int col_index,D_TABLE table) {
    for(vector<string> tuple : table) {
      tmp.push_back(tuple[col_index]);
    }
}
string query_operator::perform_oper(string A,string B,string oper) {
  string res;
  if(oper.compare("+")==0) {
    res.assign(to_string(stoi(A)+stoi(B)));
  } else if(oper.compare("-")==0) {
    res.assign(to_string(stoi(A)-stoi(B)));
  } else if(oper.compare("*")==0) {
    res.assign(to_string(stoi(A)*stoi(B)));
  } else if(oper.compare("/")==0) {
    res.assign(to_string(stoi(A)/stoi(B)==0?1:stoi(B)));
  }
  return res;
}
bool query_operator::check_cond(string A,string B,string oper) {
  if(oper.compare("<=") ==0 ) {
    return stoi(A) <= stoi(B);
  } else if(oper.compare(">=") ==0 ) {
    return stoi(A) >= stoi(B);
  } else if(oper.compare("=") ==0 ) {
    return A.compare(B)==0;
  } else if(oper.compare("<") ==0 ) {
    return stoi(A) < stoi(B);
  } else if(oper.compare(">") ==0 ) {
    return stoi(A) > stoi(B);
  }
  return false;
}
int query_operator::get_col_index(vector<string> tmp,string name) {
  int col_index_one = 0;
  auto it = find(tmp.begin(),tmp.end(),name);
  col_index_one = distance(tmp.begin(),it);
  return col_index_one;
}
D_TABLE query_operator::evaluate_natural_join(EXPR_TRACK postfix) {
  D_TABLE res;
  return res;
}
bool query_operator::checked_already_crossjoin(string table_a,string table_b) {
  set<string> split_res = split(table_a,'#');
  set<string> split_res_b = split(table_b,'#');
  if(split_res.find(table_b)!=split_res.end()) {
      return true;
  } 
  if(split_res_b.find(table_a)!=split_res_b.end()) {
      return true;
  } 
  return false;
}
D_TABLE query_operator::join_table(string table_a,string table_b,string& table_name,vector<string>& field) {
  vector<string> split_a = split_v(table_a,'#');
  vector<string> split_b = split_v(table_b,'#');
// new code
  for(string s : split_a) {
    auto it = find(split_b.begin(),split_b.end(),s);
    if(it!=split_b.end()) {
      split_b.erase(it);
    }
  }
  split_a.insert(split_a.end(),split_b.begin(),split_b.end());
  D_TABLE res;
  if(split_a.size()==0) return res;
  res = parsed_table_list[split_a[0]].first;
  vector<string> tmp = parsed_table_list[split_a[0]].second;
  append_table_name(tmp,split_a[0]);
  field = tmp;
  table_name = split_a[0];
  for(int i=1;i<split_a.size();i++) {
    res = two_table_cross_join(res,parsed_table_list[split_a[i]].first);
    tmp = parsed_table_list[split_a[i]].second;
    append_table_name(tmp,split_a[i]);
    field.insert(field.end(),tmp.begin(),tmp.end());
    table_name += "#" + split_a[i];
  }
  return res;
}
vector<string> query_operator::compute_join_results(string table_a,string table_b,string& table_name,string col1,string col2,string oper) {
  vector<string> field;
  D_TABLE res = join_table(table_a,table_b,table_name,field);
  table_name.assign(refine_table_name(table_name));
  parsed_table_list.insert(make_pair(table_name,make_pair(res,field)));
  if(query_join_tables.find(table_name)==query_join_tables.end())
    query_join_tables.insert(make_pair(table_name,res)); 
  int ca = get_col_index(field,col1);
  int cb = get_col_index(field,col2);
  vector<string> left;
  extract_col(left,ca,parsed_table_list[table_name].first);
  vector<string> right;
  extract_col(right,cb,parsed_table_list[table_name].first);
  assert(left.size()==right.size());
  vector<string> tmp;
  for(int i=0;i<left.size();i++) {
    if(is_comp(oper))
      tmp.push_back(check_cond(left[i],right[i],oper) ? "1" : "0");
    else 
      tmp.push_back(perform_oper(left[i],right[i],oper));
  }
  return tmp;
}
pair<D_TABLE,string> query_operator::evaluate_postfix(EXPR_TRACK postfix) {
  D_TABLE res;
  stack<pair<vector<string>,pair<string,pair<string,bool> > > > track;
  for(IND_EXPR p : postfix) {
     if(parsed_table_list[p.first].first.size()==0) {
      #ifndef PERFORM_CROSS_JOIN
      string lcl_table_name;
      vector<string> field_lcl;
       //load_table_from_memory(parsed_table_list[p.first],p.first);
       D_TABLE res_lcl = compute_where_result_with_cross(p.first,field_lcl,lcl_table_name);
       parsed_table_list.erase(lcl_table_name);
       parsed_table_list.insert(make_pair(lcl_table_name,make_pair(res_lcl,field_lcl)));
      #else
       load_cross_table_from_memory(parsed_table_list[p.first],p.first);
      #endif
     }
     if(parsed_table_list.find(p.first)==parsed_table_list.end()) {
      assert(0);
    }
     D_TABLE  act_table  = parsed_table_list[p.first].first;
     vector<string> field = parsed_table_list[p.first].second;
     
    if(prec.find(p.second.first)==prec.end()) {
      if(query_join_tables.find(p.first)==query_join_tables.end())
                    query_join_tables.insert(make_pair(p.first,act_table)); 
      if(!p.second.second) {
        // colum name
        int col_index = 0;
        if(p.second.first.find(".")==string::npos)
            p.second.first= p.first+"."+p.second.first;
        append_table_name(field,p.first);
        auto it = find(field.begin(),field.end(),p.second.first);
        if(it!=field.end()) {
          col_index = distance(field.begin(),it);
        } else {
          assert(0);        
        } 
        vector<string> tmp;
        extract_col(tmp,col_index,act_table);
        track.push(make_pair(tmp,make_pair(p.first,make_pair(p.second.first,false))));
      } else {
        // value
        int sz = act_table.size()==0 ? 1 : act_table.size();
        vector<string> tmp(sz,p.second.first);
        track.push(make_pair(tmp,make_pair("Value",make_pair(p.second.first,true))));
      }
    } else {
      // operator
      assert(track.size()>=2);
      pair<vector<string>,pair<string,pair<string,bool> > > B = track.top();
      track.pop();
      pair<vector<string>,pair<string,pair<string,bool> > > A = track.top();
      track.pop();
      vector<string> tmp;
      string table_name;
      if(B.second.second.second==false && A.second.second.second==false && A.second.first.compare(B.second.first)!=0) {
          tmp = compute_join_results(A.second.first,B.second.first,table_name,A.second.second.first,B.second.second.first,p.second.first);
      } else {
        table_name.assign(A.second.first);
        if(B.first.size()==1 && B.second.second.second==true) {
          string val = B.first[0];
          B.first.clear();
          B.first.resize(A.first.size(),val);
        }
        if(B.second.first.compare("Value")==0) {
          string val = B.first[0];
          B.first.clear();
          B.first.resize(A.first.size(),val);
        }
        if(query_join_tables.find(table_name)==query_join_tables.end())
          query_join_tables.insert(make_pair(table_name,act_table)); 
        assert(A.first.size()==B.first.size());
        for(int i=0;i<A.first.size();i++) {
          if(is_comp(p.second.first))
            tmp.push_back(check_cond(A.first[i],B.first[i],p.second.first) ? "1" : "0");
          else 
            tmp.push_back(perform_oper(A.first[i],B.first[i],p.second.first));
        }
            
    }
    track.push(make_pair(tmp,make_pair(table_name,make_pair(p.second.first,false))));
  }
}
  // etract result here
  assert(track.empty()==false);
  vector<string> bool_check = track.top().first;
  string table_name = track.top().second.first;
  track.pop();
  D_TABLE join_table = query_join_tables[table_name];
  for(int i=0;i<bool_check.size();i++) {
    if(bool_check[i].compare("1")==0) {
      // valid entry, push it
      res.push_back(join_table[i]);
    }
  }
  return make_pair(res,table_name);
}
pair<D_TABLE,string> query_operator::process_b_term(query_tree *root) {
  pair<D_TABLE,string> res;
  if(!root) return res;
  // table name , col name
  EXPR_TRACK experssion;
  string last_saved;
  for(int i=0;i<root->next.size();i++) {
    string table_name,col_name;
    bool is_val=false;
#ifndef PERFORM_CROSS_JOIN
    auto ptr = root->next[i]->name.find(".");
    if(ptr!=string::npos) {
      // table name embbeded
      table_name.assign(root->next[i]->name.substr(0,ptr));
      col_name.assign(root->next[i]->name);
      table_track.insert(table_name);
      last_saved.assign(table_name);
    } else {
      if(last_saved.empty()) {
        for(auto it=parsed_table_list.begin();it!=parsed_table_list.end();it++) {
          pair<string,EACH_ENTRY> p = *it;
          table_name.assign(p.first);
          table_track.insert(table_name);
          last_saved.assign(table_name);
          break;
        }
      } else {
        table_name.assign(last_saved);
      }
      col_name.assign(root->next[i]->name);
    }
#else
      for(auto it=parsed_table_list.begin();it!=parsed_table_list.end();it++) {
          pair<string,EACH_ENTRY> p = *it;
          table_name.assign(p.first);
          table_track.insert(table_name);
          break;
      }
      col_name.assign(root->next[i]->name);

#endif
    is_val = root->next[i]->is_val;
    experssion.push_back(make_pair(table_name,make_pair(col_name,is_val)));
  }
  EXPR_TRACK postfix = form_postfix(experssion);
  return evaluate_postfix(postfix);
}
// new code
void query_operator::fill_track(int index,vector<string> tuple,vector<string> tables,KEY_TRACK& key,string table_name) {
  vector<string> field = parsed_table_list[table_name].second;
  /* -> ordering code
  unordered_map<string,vector<string> > field_track;
  for(string s : tables) {
    field_track.insert(make_pair(s,vector<string>()));
  }
  for(int i=0;i<field.size();i++) {
    string cur_table_name = get_table_name(field[i]);
    field_track[cur_table_name].push_back(field[i]);
  }
  field.clear();
  for(string s : tables)  {
    field.insert(field.end(),field_track[s].begin(),field_track[s].end());
  }
  */
  assert(field.size()==tuple.size());
  unordered_map<string,string> lcl_track;
  for(string s : tables) {
    lcl_track.insert(make_pair(s,""));
  }
  unordered_set<string> tmp;
  key.insert(make_pair(index,tmp));
  for(int i=0;i<tuple.size();i++) {
      string cur_table_name = get_table_name(field[i]);
      assert(cur_table_name.empty()==false);
      lcl_track[cur_table_name]+= tuple[i]+"#";
  }
  for(pair<string,string> p : lcl_track) {
    key[index].insert(p.second);
  }
}
void query_operator::simplify_table_values(KEY_TRACK& key,D_TABLE tab,vector<string> tables,string table_name) {
  int i=0;
  for(vector<string> tuple : tab) {
    fill_track(i,tuple,tables,key,table_name);
    i++;
  }
}
bool query_operator::compare(unordered_set<string> A,unordered_set<string> B) {
  assert(A.size()==B.size());
  int ct=0;
  for(string s : A) {
    if(B.count(s)>0) ct++;
  }
  return ct == A.size() ? true : false;
}
D_TABLE query_operator::find_common(KEY_TRACK& left_key,KEY_TRACK& right_key,D_TABLE left) {
  D_TABLE res;
  int to_erase=-1;
  for(pair<int,unordered_set<string> > p : left_key) {
    for(pair<int,unordered_set<string> > rt : right_key) {
      if(compare(p.second,rt.second)) {
        to_erase=rt.first;
        res.push_back(left[p.first]);
        break;
      }
    }
    if(to_erase!=-1) {
      right_key.erase(to_erase);
      to_erase=-1;
    }
  }
  return res;
}
D_TABLE query_operator::get_table_keys(D_TABLE left,D_TABLE right,string table_name) {
  vector<string> tables = split_v(table_name,'#');
  KEY_TRACK left_key;
  simplify_table_values(left_key,left,tables,table_name);
  KEY_TRACK right_key;
  simplify_table_values(right_key,right,tables,table_name);
  return find_common(left_key,right_key,left);
}
///
void query_operator::prepare_track(unordered_map<string,pair<vector<string>,int> >& track_left,unordered_map<string,pair<vector<string>,int> >& track_right,D_TABLE left,D_TABLE right) {
  for(vector<string> tuple : left) {
    string key = get_key(tuple);
    if(track_left.find(key)!=track_left.end()) {
      track_left[key].second++;
    } else {
      track_left.insert(make_pair(key,make_pair(tuple,1)));
    }
  }
  for (vector<string> tuple : right)
  {
    string key = get_key(tuple);
    if(track_right.find(key)!=track_right.end()) {
      track_right[key].second++;
    } else {
      track_right.insert(make_pair(key,make_pair(tuple,1)));
    }
  }

}
set<string> query_operator::find_common_tables(string table_a,string table_b) {
  // 1. in any join calc the tables which are common in join so that they may not be repeated
  set<string> res;
  set<string> left = split(table_a,'#');
  set<string> right = split(table_b,'#');
  for(string s : right) {
    if(left.count(s) > 0) res.insert(s);
  }
  return res;
}
vector<int> query_operator::form_common_index(vector<string> field,set<string> common_table_join) {
  vector<int> res;
  for(int i=0;i<field.size();i++) {
    string s = field[i];
    auto it = s.find(".");
    string table_name = s.substr(0,it);
    if(common_table_join.count(table_name)>0) res.push_back(i);
  }
  return res;
}
string query_operator::form_key(vector<string> tuple,vector<int> common_index) {
  string key;
  for(int i : common_index) {
    assert(i<tuple.size());
    key+= tuple[i];
  }
  return key;
}
unordered_map<string,vector<int> > query_operator::form_common_key(D_TABLE table,set<string> common_table_join,string table_name) {
  unordered_map<string,vector<int> > res;
  vector<string> field = parsed_table_list[table_name].second;
  vector<int> common_index = form_common_index(field,common_table_join);
  int i=0;
  for(vector<string> tuple : table) {
    string key = form_key(tuple,common_index);
    if(res.find(key)==res.end())
      res.insert(make_pair(key,vector<int>(1,i)));
    else {}
      //res[key].push_back(i);
    i++;
  }
  return res;
}
void query_operator::get_or_common(vector<int>& left_match_index,vector<int>& right_match_index,unordered_map<string,vector<int> > left,unordered_map<string,vector<int> > right) { 
  for(pair<string,vector<int> > str : left) {
    string s = str.first;
    if(right.find(s)!=right.end()) {
      for(int i=0;i<str.second.size();i++) {
          left_match_index.push_back(left[s][i]);
          //right.erase(s);
      }
    } else {
        for(int i=0;i<str.second.size();i++) {
          left_match_index.push_back(left[s][i]);
        }
    }
  }
  for(pair<string,vector<int> > str : right) {
    string s = str.first;
    for(int i=0;i<str.second.size();i++)
      right_match_index.push_back(right[s][i]);
  }
}
void query_operator::get_common_index(vector<int>& left_match_index,vector<int>& right_match_index,unordered_map<string,vector<int> > left,unordered_map<string,vector<int> > right,string table_name) {
  // extra value here as table strings
  for(pair<string,vector<int> > str : left) {
    string s = str.first;
    if(right.find(s)!=right.end()) {
        for(int i=0;i<str.second.size();i++)
          left_match_index.push_back(left[s][i]);
        for(int i=0;i<right[s].size();i++)
          right_match_index.push_back(right[s][i]);
     
    }
  }
} 
void query_operator::refine_table(vector<int> match_index,D_TABLE& table) {
  D_TABLE res;
  for(int i : match_index) {
    res.push_back(table[i]);
  }
  table.clear();
  for(vector<string> str : res) {
    table.push_back(str);
  }
}
string query_operator::refine_table_name(string table_name) {
  set<string> res = split(table_name,'#');
  string tmp;
  for(string s : res) {
    tmp+= s+"#";
  }
  tmp.pop_back();
  return tmp;
}
void query_operator::remove_common(D_TABLE& right,set<string> common_table_join,string table_name) {
  D_TABLE res;
  unordered_set<int> search;
  vector<string> field = parsed_table_list[table_name].second;
  vector<int> common_index = form_common_index(field,common_table_join);
  for(int i : common_index) {
    search.insert(i);
  }
  for(vector<string> tuple : right) {
    vector<string> tmp;
    for(int i=0;i<tuple.size();i++) {
      if(search.find(i)==search.end()) {
        tmp.push_back(tuple[i]);
      }
    }
    res.push_back(tmp);
  }
  right.clear();
  for(vector<string> str : res) {
    right.push_back(str);
  }
}
D_TABLE query_operator::extract_complete_col(D_TABLE A,vector<int> c_index) {
  D_TABLE res;
  for(vector<string> tuple : A) {
    vector<string> tmp;
    for(int i : c_index) {
      tmp.push_back(tuple[i]);
    }
    res.push_back(tmp);
  }
  return res;
}
vector<int> query_operator::extra_col_index_val(vector<string> field,set<string> c_value) {
  int i=0;
  vector<int> res;
  for(string s : field) {
    string table_name;
    auto it = s.find(".");
    if(it!=string::npos) {
      table_name = s.substr(0,it);
    } else {
      table_name=s;
    }
    if(c_value.count(table_name) > 0) {
      res.push_back(i);
    }
    i++;
  }
  return res;
}
vector<int> query_operator::and_element(D_TABLE A,D_TABLE B){
  vector<int> res;
  unordered_map<string,vector<int> > track;
  int i=0;
  for(vector<string> tuple : A){
    string key = get_key(tuple);
    if(track.find(key)!=track.end()) {
      track.insert(make_pair(key,vector<int>(1,i)));
    } else {
      track[key].push_back(i);
    }
    i++;
  }
  for(vector<string> tuple : B) {
    string key = get_key(tuple);
     if(track.find(key)!=track.end()) {
        res.push_back(track[key][0]);
        track[key].erase(track[key].begin());
        if(track[key].size()==0) {
          track.erase(key);
        }   
     } 
  }
  return res;
}
D_TABLE query_operator::get_common(D_TABLE A,D_TABLE B,string common_ls,string table_name) {
  set<string> c_value  = split(common_ls,'#');
  vector<string> field = parsed_table_list[table_name].second;
  vector<int> c_index = extra_col_index_val(field,c_value);
  D_TABLE ex_val = extract_complete_col(A,c_index);
  vector<int> index =  and_element(ex_val,B);
  D_TABLE res;
  for(int i : index) {
    assert(i<A.size());
    res.push_back(A[i]);
  }
  return res;

}
D_TABLE query_operator::perform_and(D_TABLE left,D_TABLE right,string table_a,string table_b) {
 unordered_map<string,pair<vector<string>,int> > track_left;
 unordered_map<string,pair<vector<string>,int> > track_right;
  table_a.assign(refine_table_name(table_a));
  table_b.assign(refine_table_name(table_b));
  if(table_a.compare(table_b)!=0) {
    string table_name;
    vector<string> field;
    D_TABLE res = join_table(table_a,table_b,table_name,field);
    parsed_table_list.insert(make_pair(table_name,make_pair(res,field)));
    set<string> common_table_join = find_common_tables(table_a,table_b);
    if(common_table_join.size()!=0) {
      string common_ls;
      if(is_sub(table_a,table_b,common_ls)) {
        return get_common(right,left,common_ls,table_b);
      } else if(is_sub(table_b,table_a,common_ls)) {
        return get_common(left,right,common_ls,table_a);
      } else {
        unordered_map<string,vector<int> > left_key = form_common_key(left,common_table_join,table_a);
        unordered_map<string,vector<int> > right_key = form_common_key(right,common_table_join,table_b);
        vector<int> left_match_index;
        vector<int> right_match_index;
        get_common_index(left_match_index,right_match_index,left_key,right_key,table_name);
        refine_table(left_match_index,left);
        refine_table(right_match_index,right);
        remove_common(right,common_table_join,table_b);
      }
    }
    return two_table_cross_join(left,right);
  } 
  
  return get_table_keys(left,right,table_a);
 
}
vector<bool> query_operator::prepare_true_table(D_TABLE org,D_TABLE cur) {
  vector<bool> res;
  unordered_set<string> track;
  for(vector<string> s : cur) {
    string key= get_key(s);
    if(track.count(key)==0) track.insert(key);
  }
  for(vector<string> s : org) {
    string key = get_key(s);
    if(track.count(key) > 0)
      res.push_back(true);
    else 
      res.push_back(false);
  }
  return res;
}
D_TABLE query_operator::perform_or(D_TABLE left,D_TABLE right,string table_a,string table_b) {
  D_TABLE res;
  unordered_map<string,pair<vector<string>,int> > track_left;
  unordered_map<string,pair<vector<string>,int> > track_right;
  table_a.assign(refine_table_name(table_a));
  table_b.assign(refine_table_name(table_b));
  if(table_a.compare(table_b)!=0) {
    string table_name;
    vector<string> field;
    D_TABLE res = join_table(table_a,table_b,table_name,field);
    parsed_table_list.insert(make_pair(table_name,make_pair(res,field)));
    set<string> common_table_join = find_common_tables(table_a,table_b);
    unordered_map<string,vector<int> > left_key = form_common_key(left,common_table_join,table_a);
    unordered_map<string,vector<int> > right_key = form_common_key(right,common_table_join,table_b);
    vector<int> left_match_index;
    vector<int> right_match_index;
    get_or_common(left_match_index,right_match_index,left_key,right_key);
    refine_table(left_match_index,left);
    refine_table(right_match_index,right);
    remove_common(right,common_table_join,table_b);
    return two_table_cross_join(left,right);

  } 
  prepare_track(track_left,track_right,left,right);
  for(pair<string,pair<vector<string>,int> > p : track_left) {
      if(track_right.find(p.first)!=track_right.end()) {
        int ct = max(p.second.second,track_right[p.first].second);
        while(ct > 0) {
          res.push_back(p.second.first);
          ct--;
        }
        track_right.erase(p.first);
      } else {
        int ct = p.second.second;
        while(ct > 0) {
          res.push_back(p.second.first);
            ct--;
        }
      }
  }
  for(pair<string,pair<vector<string>,int> > p : track_right) {
    int ct = p.second.second;
    while(ct > 0) {
      res.push_back(p.second.first);
      ct--;
    }
  }
  return res;
}
vector<vector<string> > query_operator::condition_join(pair<D_TABLE,string> left, pair<D_TABLE,string> right,string condition) {
  D_TABLE res;
  if(condition.compare("AND")==0) {
    return perform_and(left.first,right.first,left.second,right.second);
  } else if(condition.compare("OR")==0) {
    return perform_or(left.first,right.first,left.second,right.second);
  }
  return res;
}
pair<D_TABLE,string> query_operator::execute_where_condition(query_tree *root) {
      //cout << __FUNCTION__ << endl;
      pair<D_TABLE,string> tmp;
      if(!root) return tmp;
      if(root->next.size()!=1) {
        assert(root->next.size()==3);
       pair<D_TABLE,string> left = root->next[0]->name.compare("b_term")
                        !=0 ? process_b_term(root->next[0]) : execute_where_condition(root->next[0]);
       pair<D_TABLE,string> right = root->next[2]->name.compare("b_term")
                        !=0 ? process_b_term(root->next[2]) : execute_where_condition(root->next[2]);
        string table_name = left.second.compare(right.second)!=0 ? left.second+"#"+right.second : left.second;
        return make_pair(condition_join(left,right,root->next[1]->name),table_name);
      }
      return process_b_term(root->next[0]);
}
void query_operator::append_table_name(vector<string>& field,string table_name) {
  for(string &s : field) {
    if(s.find(".")==string::npos)
      s = table_name+"."+s;
  }

}
D_TABLE query_operator::two_table_cross_join(D_TABLE A,D_TABLE B) {
  D_TABLE res;
  for(vector<string> tuple : A) {
    for(vector<string> loc : B) {
      vector<string> tmp = tuple;
      tmp.insert(tmp.end(),loc.begin(),loc.end());
      res.push_back(tmp);
    }
  }
  return res;

}
void query_operator::compute_cross_join(TABLE_ENTERIES& parsed_table_list_lcl,string& table_name) {
  pair<string,EACH_ENTRY> res;
  for(pair<string,EACH_ENTRY> p : parsed_table_list_lcl) {
    if(res.first.size()==0) {
      append_table_name(p.second.second,p.first);
      res = p;
      continue;
    }
    append_table_name(p.second.second,p.first);
    res.first = res.first + "#" + p.first;
    res.second.second.insert(res.second.second.end(),p.second.second.begin(),p.second.second.end());
    D_TABLE tmp = res.second.first;
    res.second.first.clear();
    for(vector<string> tuple : tmp) {
      for(vector<string> sec : p.second.first) {
        vector<string> nes = tuple;
        nes.insert(nes.end(),sec.begin(),sec.end());
        res.second.first.push_back(nes);
      }
    }
    tmp.clear();
  }
  res.first.assign(refine_table_name(res.first));
  table_name.assign(res.first);
  parsed_table_list_lcl.clear();
  parsed_table_list_lcl.insert(res);
}
pair<vector<vector<string> >,string> query_operator::compute_normal_result() {
  string table_name;
  for(string s : table_list) {
    if(parsed_table_list[s].first.size()==0) {
       load_table_from_memory(parsed_table_list[s],s);
     }
  }  
  compute_cross_join(parsed_table_list,table_name);
  table_name.assign(refine_table_name(table_name));
  parsed_table_list.insert(make_pair(table_name,make_pair(parsed_table_list[table_name].first,parsed_table_list[table_name].second)));
  return make_pair(parsed_table_list[table_name].first,table_name);
}
set<string> query_operator::refine_project_cond() {
  set<string> res;
  for(string cur : select_list) {
    if(cur.find(".")==string::npos) {
      for(string tab : table_list) {
        res.insert(tab+"."+cur);
      }
    } else {
      res.insert(cur);
    }
  }
  return res;
}
vector<int> query_operator::extract_col_index(vector<string> field,set<string> mod_project) {
  vector<int> res;
  for(int i=0;i<field.size();i++) {
    if(mod_project.find(field[i])!=mod_project.end()) res.push_back(i);
  }
  return res;
}
int query_operator::get_index(vector<string> field,string val) {
  auto ptr = val.find(".");
  bool no_split = false;
  if(ptr!=string::npos) {
    no_split=true;
  }
  for(int i=0;i<field.size();i++) {
    if(no_split) {
      if(val.compare(field[i])==0) return i;
      continue;
    }
    auto it = field[i].find(".");
    string cur;
    if(it==string::npos) {
      cur = field[i];
    } else {
      cur = field[i].substr(it+1);
    }
    if(val.compare(cur)==0) return i;
  }
  return -1;
}
void query_operator::order_by(D_TABLE& res,string val,string table_name) {
  vector<string> field = parsed_table_list[table_name].second;
  int index = get_index(field,val);
  assert(index!=-1);
  //cout << "This is index  : " << index << endl;
  sort(res.begin(),res.end(),Local(index));
}
pair<vector<vector<string> >,string> query_operator::do_projection(pair<vector<vector<string> >,string> to_project,vector<string>& field_fnl) {
  set<string> mod_project = refine_project_cond();
  if(mod_project.size()==0) {
    for(string s : parsed_table_list[to_project.second].second) {
      mod_project.insert(s);
    }
  }
  if(to_project.first.size()==0) return to_project;
  assert(to_project.first[0].size()==  parsed_table_list[to_project.second].second.size());
  D_TABLE table = to_project.first;
  vector<string> field =   parsed_table_list[to_project.second].second;
  vector<int> col_index = extract_col_index(field,mod_project);
  D_TABLE res;
  unordered_set<string> track;
  for(vector<string> tuple : table) {
      vector<string> tmp;
      string key;
      for(int i=0;i<col_index.size();i++) {
        tmp.push_back(tuple[col_index[i]]);
        key += tmp.back() + "#";
      }
      key.pop_back();
      if(is_distinct==true) {
        if(track.count(key) == 0) {
          res.push_back(tmp);
          track.insert(key);
        }
        continue;
      }
      res.push_back(tmp);
   }
  for(string s : mod_project) field_fnl.push_back(s);
  if(is_orderby) {
    order_by(res,orderby_value,to_project.second);
  }
  return make_pair(res,to_project.second);
}
void query_operator::add_to_record() {
  // get the value based on select_list
    // need to join the table
    //1. In case of join clear the parse_table_list and make it as single entry
  if(parsed_table_list.size()==0) return;

  #ifdef PERFORM_CROSS_JOIN
      // 1. table_name, 1.1 table 1.2 field
      string table_name;
      for(string s : table_list) {
        table_name += s +"#";
      }
      table_name.pop_back();
      parsed_table_list.clear();
      D_TABLE res;
      parsed_table_list.insert(make_pair(table_name,make_pair(res,vector<string>())));
  #endif
  /*
    Two method to perform where clause
    1. on join
    2. on single
  */
  // 2. perform where clause on single
  pair<vector<vector<string> >,string> where_result;
  vector<string> field;
  D_TABLE tmp;
  string table_cl;
  if(search_cond_node) {
      where_result = execute_where_condition(search_cond_node->next[0]);
      // get result here for where clause
  } else {
      //where_result = compute_normal_result();
      tmp = compute_normal_result_with_cross(table_cl,field);
      where_result.first.insert(where_result.first.end(),tmp.begin(),tmp.end());
      where_result.second.assign(table_cl);
      parsed_table_list.erase(table_cl);
      parsed_table_list.insert(make_pair(table_cl,make_pair(tmp,field)));
  }
  where_result.second.assign(refine_table_name(where_result.second));
  where_result = do_projection(where_result,field);
  #ifdef PRINT_TO_FILE
  print_table_to_file(where_result.first,where_result.second,field);
  #else
  print_table_to_console(where_result.first,where_result.second,field);
  #endif
  #ifdef SHOW_STATS 
  cout << "Real elapse time = " << ((double)(clock()-start_time)/CLOCKS_PER_SEC*1000) << " ms" << endl;
  cout << "Calculated elapse time = " << StorageSingleton::getInstance().disk.getDiskTimer() << " ms" << endl;
  cout << "Calculated Disk I/Os = " << StorageSingleton::getInstance().disk.getDiskIOs() << endl;
  #endif
  clear_select();
}
D_TABLE query_operator::compute_normal_result_with_cross(string& table_name,vector<string>& field_name) {
  // string table_name;
  string join_result;
  D_TABLE res;
  if (table_list.size() == 1) { 
    res  = load_table_from_memory_cross(table_list[0],field_name); 
    join_result.assign(table_list[0]);
  }
  else {
    string join_result = table_list[0];
    string prev;
    for (int i = 1; i < table_list.size(); i++){
      string table_one = table_list[i];
      // string table_two = table_list[i+1];
      join_result.assign(refine_table_name(join_result));  
      join_result = getNormalCrossJoin(join_result, table_one);
      if(!prev.empty()) {
        //del prev
        drop_cross_join(prev);
      }
      prev = join_result;
    }
    res = load_table_from_memory_cross(join_result,field_name);
  }
  table_name.assign(join_result);
  return res;
}
D_TABLE 
query_operator::compute_where_result_with_cross(string table_name,vector<string>& field,string& cross_table) {
  vector<string> subset_table_list = split_v(table_name, '#');
  D_TABLE res;
  assert(subset_table_list.size()!=0);
  if (subset_table_list.size() == 1) {
    res =load_table_from_memory_cross(subset_table_list[0],field);
    cross_table.assign(subset_table_list[0]);  
  } else {
    string join_result = subset_table_list[0];
    string prev;
    for (int i = 1; i < subset_table_list.size(); i++){
      string table_one = subset_table_list[i];
      join_result.assign(refine_table_name(join_result));  
      join_result = getNormalCrossJoin(join_result, table_one);
      if(!prev.empty()) {
        //del prev
        drop_cross_join(prev);
      }
      prev = join_result;
    }

    res = load_table_from_memory_cross(join_result,field);
    cross_table.assign(join_result);
  }
  return res;
}

D_TABLE query_operator::get_entire_table_cross(vector<string>& field_names, string table_name) {
  // This part of code, in place of cout , we can add to vector
  vector<vector<string> > enteries;
  for(int i=0;i<max_blocks;i++) {
     Block* block_ptr=StorageSingleton::getInstance().mem.getBlock(i);
     if(block_ptr->getNumTuples()!=0) {
        vector<Tuple> tuples=block_ptr->getTuples();
        for(Tuple tuple : tuples) {
          Schema tuple_schema = tuple.getSchema();
          vector<string> tmp;
          if(field_names.empty())
          field_names = tuple_schema.getFieldNames();
          for (int i=0; i<tuple.getNumOfFields(); i++) {
            if (tuple_schema.getFieldType(i)==INT)
              tmp.push_back(to_string(tuple.getField(i).integer));
            else
              tmp.push_back(*(tuple.getField(i).str));
            }
            enteries.push_back(tmp);
        }
     }
  }
  return enteries;
}
vector<string> query_operator::get_entire_table(EACH_ENTRY& table, string table_name) {
  // This part of code, in place of cout , we can add to vector
  vector<string> field_names;
  vector<vector<string> > enteries;
  for(int i=0;i<max_blocks;i++) {
     Block* block_ptr=StorageSingleton::getInstance().mem.getBlock(i);
     if(block_ptr->getNumTuples()!=0) {
        vector<Tuple> tuples=block_ptr->getTuples();
        for(Tuple tuple : tuples) {
          Schema tuple_schema = tuple.getSchema();
          vector<string> tmp;
          if(field_names.empty())
          field_names = tuple_schema.getFieldNames();
          for (int i=0; i<tuple.getNumOfFields(); i++) {
            if (tuple_schema.getFieldType(i)==INT)
              tmp.push_back(to_string(tuple.getField(i).integer));
            else
              tmp.push_back(*(tuple.getField(i).str));
            }
            enteries.push_back(tmp);
        }
     }
  }
 // table.second=field_names;
  for(vector<string> tuple : enteries) {
    table.first.push_back(tuple);
  }
 // table.first=enteries;
  return field_names;
}
D_TABLE query_operator::load_table_from_memory_cross(string table_name,vector<string>& field_val) {
    D_TABLE res;
    if(StorageSingleton::getInstance().schema_manager->relationExists(table_name)) {
        Relation* relation_ptr = StorageSingleton::getInstance().schema_manager->getRelation(table_name);
        if(relation_ptr->getNumOfBlocks() > max_blocks) {
          // retrive max block by block
         // cout << "Total Blocks :"<< relation_ptr->getNumOfBlocks() << endl;
          int max_relation_block = relation_ptr->getNumOfBlocks();
          for(int i=0;i<max_relation_block;i+=max_blocks) {
            int limit = i+max_blocks <= max_relation_block ? max_blocks : (max_relation_block-i);
            relation_ptr->getBlocks(i,0,limit);
            D_TABLE tmp = get_entire_table_cross(field_val,table_name);
            res.insert(res.begin(),tmp.begin(),tmp.end());
            append_table_name(field_val,table_name);
            empty_memory_block();
          }
        } else {
        relation_ptr->getBlocks(0,0,relation_ptr->getNumOfBlocks());
        //cout << "Now the memory contains: " << endl;
        //cout << StorageSingleton::getInstance().mem << endl;
        res = get_entire_table_cross(field_val,table_name);
        empty_memory_block();
      }
    }
    return res;
}
void query_operator::delete_specific(D_TABLE cond,string table_name) {
  D_TABLE cur_table;
  vector<string> field;
  if(parsed_table_list[table_name].first.size()==0) {
    cur_table = load_table_from_memory_cross(table_name,field);
  } else {
    cur_table = parsed_table_list[table_name].first;
  }
  D_TABLE to_keep;
  unordered_map<string,int> track;
  for(vector<string> tuple : cond) {
    string key = get_key(tuple);
    track[key]++;
  }
  for(vector<string> tuple : cur_table) {
    string key = get_key(tuple);
    if(track.find(key)==track.end()) {
      to_keep.push_back(tuple);
    } else {
      track[key]--;
      if(track[key]==0) 
        track.erase(key);
      
    }
  }
  delete_all(table_name);
  Relation* relation_ptr = StorageSingleton::getInstance().schema_manager->getRelation(table_name);
  for(vector<string> cur : to_keep) {
    Tuple tuple = relation_ptr->createTuple();
    Schema tuple_schema = tuple.getSchema();
    vector<string> field_names = tuple_schema.getFieldNames();
    vector<enum FIELD_TYPE> field_types = tuple_schema.getFieldTypes();
    assert(cur.size()==field_names.size());
    for (int i = 0; i < field_names.size(); i++) { 
      if (field_types[i] == FIELD_TYPE::STR20){
        tuple.setField(field_names[i], cur[i]); 
      }
      else {
        tuple.setField(field_names[i], stoi(cur[i]));
      }
    }
    Block* block_ptr=StorageSingleton::getInstance().mem.getBlock(0);
    block_ptr->clear();
    block_ptr->setTuple(0,tuple);
    appendTupleToRelation(relation_ptr,StorageSingleton::getInstance().mem,0,tuple);
  }
  
}
void query_operator::delete_all(string table_name) {
   if(StorageSingleton::getInstance().schema_manager->relationExists(table_name)) {
       Relation* relation_ptr = StorageSingleton::getInstance().schema_manager->getRelation(table_name);
       for(int i=0;i<relation_ptr->getNumOfBlocks();i++) {
          relation_ptr->deleteBlocks(i);
       }
   } else {
    cout << "Error : Trying to delete table which is not present\n";
   } 
}
void query_operator::load_table_from_memory(EACH_ENTRY& table,string table_name) {
    if(StorageSingleton::getInstance().schema_manager->relationExists(table_name)) {
        Relation* relation_ptr = StorageSingleton::getInstance().schema_manager->getRelation(table_name);
        if(relation_ptr->getNumOfBlocks() > max_blocks) {
          // retrive max block by block
         // cout << "Total Blocks :"<< relation_ptr->getNumOfBlocks() << endl;
          int max_relation_block = relation_ptr->getNumOfBlocks();
          for(int i=0;i<max_relation_block;i+=max_blocks) {
            int limit = i+max_blocks <= max_relation_block ? max_blocks : (max_relation_block-i);
            relation_ptr->getBlocks(i,0,limit);
            EACH_ENTRY tmp;
            table.second = get_entire_table(tmp,table_name);
            append_table_name(table.second,table_name);
            for(vector<string> val : tmp.first) {
              table.first.push_back(val);
            }
            empty_memory_block();
          }
        } else {
        relation_ptr->getBlocks(0,0,relation_ptr->getNumOfBlocks());
        //cout << "Now the memory contains: " << endl;
        //cout << StorageSingleton::getInstance().mem << endl;
        table.second = get_entire_table(table,table_name);
        empty_memory_block();
      }
    }
}
void query_operator::execute_select_query() {
  get_attr_list(root_node);
  // need to handle where condition also
  for(string table_name : table_list) {
    EACH_ENTRY tmp;
    parsed_table_list.insert(make_pair(table_name,tmp));
  }
  // execute the other parts of select statement, where and slect list
  add_to_record();
}
void query_operator::process_create_table_query() {

  vector<enum FIELD_TYPE> create_attr_type_list;
  string int_string = "INTA";
  for (string attr_type: attr_type_list) {
    if (attr_type.compare(int_string) == 0) {
      create_attr_type_list.push_back(INT);
    }
    else {
      create_attr_type_list.push_back(STR20);
    }
  }
	create_schema(attr_name_list, create_attr_type_list);

}

void query_operator::create_schema(vector<string> field_names, vector<enum FIELD_TYPE> field_types) {
	// Create a schema
  //cout << "Creating a schema" << endl;
  Schema schema(field_names,field_types);

  // Print the information about the schema
  //cout << schema << endl;
  //cout << "The schema has " << schema.getNumOfFields() << " fields" << endl;
  //cout << "The schema allows " << schema.getTuplesPerBlock() << " tuples per block" << endl;
  //cout << "The schema has field names: " << endl;
  field_names=schema.getFieldNames();
  //copy(field_names.begin(),field_names.end(),ostream_iterator<string,char>(cout," "));
  //cout << endl;
  //cout << "The schema has field types: " << endl;
  field_types=schema.getFieldTypes();
  //for (int i=0;i<schema.getNumOfFields();i++) {
    //cout << (field_types[i]==0?"INT":"STR20") << "\t";
  //}
  
  // Create a relation with the created schema through the schema manager
  // string relation_name="ExampleTable1";

  //cout << "Creating table " << table_name << endl;  
  Relation* relation_ptr=StorageSingleton::getInstance().schema_manager->createRelation(table_name,schema);
  max_blocks = StorageSingleton::getInstance().mem.getMemorySize();
  // Print the information about the Relation
  //cout << "The table has name " << relation_ptr->getRelationName() << endl;
  //cout << "The table has schema:" << endl;
  //cout << relation_ptr->getSchema() << endl;
  //cout << "The table currently have " << relation_ptr->getNumOfBlocks() << " blocks" << endl;
  //cout << "The table currently have " << relation_ptr->getNumOfTuples() << " tuples" << endl << endl;


  //cout << "The first field is of name " << schema.getFieldName(0) << endl;
  //cout << "The second field is of type " << (schema.getFieldType(0)==0?"INT":"STR20") << endl;
  //cout << "From the schema manager, the table " << table_name << " exists: "
   //    << (StorageSingleton::getInstance().schema_manager->relationExists(table_name)?"TRUE":"FALSE") << endl;
  //cout << "From the schema manager, the table " << table_name << " has schema:" << endl;
  //cout << StorageSingleton::getInstance().schema_manager->getSchema(table_name) << endl;
  //cout << "From the schema manager, the table " << table_name << " has schema:" << endl;
  //cout << StorageSingleton::getInstance().schema_manager->getRelation(table_name)->getSchema() << endl;

  clear_all_values();
}

void query_operator::appendTupleToRelation(Relation* relation_ptr, MainMemory& mem, int memory_block_index, Tuple& tuple) {
  Block* block_ptr;
  if (relation_ptr->getNumOfBlocks()==0) {
   // cout << "The relation is empty" << endl;
    //cout << "Get the handle to the memory block " << memory_block_index << " and clear it" << endl;
    block_ptr=mem.getBlock(memory_block_index);
    block_ptr->clear(); //clear the block
    block_ptr->appendTuple(tuple); // append the tuple
    //cout << "Write to the first block of the relation" << endl;
    relation_ptr->setBlock(relation_ptr->getNumOfBlocks(),memory_block_index);
  } else {
    //cout << "Read the last block of the relation into memory block 0:" << endl;
    relation_ptr->getBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index);
    block_ptr=mem.getBlock(memory_block_index);

    if (block_ptr->isFull()) {
      //cout << "(The block is full: Clear the memory block and append the tuple)" << endl;
      block_ptr->clear(); //clear the block
      block_ptr->appendTuple(tuple); // append the tuple
      //cout << "Write to a new block at the end of the relation" << endl;
      relation_ptr->setBlock(relation_ptr->getNumOfBlocks(),memory_block_index); //write back to the relation
    } else {
      //cout << "(The block is not full: Append it directly)" << endl;
      block_ptr->appendTuple(tuple); // append the tuple
      //cout << "Write to the last block of the relation" << endl;
      relation_ptr->setBlock(relation_ptr->getNumOfBlocks()-1,memory_block_index); //write back to the relation
    }
  }  
}
void query_operator::execute_insert_query() {
  
  Relation* relation_ptr = StorageSingleton::getInstance().schema_manager->getRelation(table_name);
  Tuple tuple = relation_ptr->createTuple();

  // Print the information about the tuple
  //cout << "Created a tuple " << tuple << " through the relation" << endl;
  //cout << "The tuple is invalid? " << (tuple.isNull()?"TRUE":"FALSE") << endl;
  Schema tuple_schema = tuple.getSchema();
  vector<string> field_names = tuple_schema.getFieldNames();
  string key_act = get_key(field_names);
  string key_cur = get_key(attr_name_list);
  if(key_act.compare(key_cur)!=0) {
    unordered_map<string,pair<string,string> > track = map_index(attr_name_list,attr_value_list,attr_type_list);
    attr_name_list.clear();
    attr_value_list.clear();
    attr_type_list.clear();
    for(string s : field_names) {
      attr_name_list.push_back(s);
      attr_value_list.push_back(track[s].first);
      attr_type_list.push_back(track[s].second);
    }
  }
  for (int i = 0; i < attr_name_list.size(); i++) {
    // tuple.setField(attr_name_list[i], attr_value_list[i]); 
    if (attr_type_list[i].compare("STR20A") ==  0){
      //cout << "Attribute name is: " << attr_name_list[i] << "Value is: " << attr_value_list[i] << endl;
      tuple.setField(attr_name_list[i], attr_value_list[i]); 
    }
    else {
      //cout << "Attribute name is: " << attr_name_list[i] << "Value is: " << attr_value_list[i] << endl;
      tuple.setField(attr_name_list[i], stoi(attr_value_list[i]));
    }
  }

  Block* block_ptr=StorageSingleton::getInstance().mem.getBlock(0);
  block_ptr->clear();
  //cout << "Set the tuple at offset 0 of the memory block 0" << endl;
  block_ptr->setTuple(0,tuple);
  appendTupleToRelation(relation_ptr,StorageSingleton::getInstance().mem,0,tuple);
 #ifdef SHOW_STATS 
  cout << "Insert : Real elapse time = " << ((double)(clock()-start_time)/CLOCKS_PER_SEC*1000) << " ms" << endl;
  cout << "Insert : Calculated elapse time = " << StorageSingleton::getInstance().disk.getDiskTimer() << " ms" << endl;
  cout << "Insert : Calculated Disk I/Os = " << StorageSingleton::getInstance().disk.getDiskIOs() << endl;
 #endif
  clear_all_values();

}
void query_operator::drop_cross_join(string join_table_name) {
  //cout << "Deleting : "<< join_table_name;
  StorageSingleton::getInstance().schema_manager->deleteRelation(join_table_name);
  StorageSingleton::getInstance().schema_manager->getRelation(join_table_name);
  clear_all_values();
}

void query_operator::performVectorTupleJoin(vector<Tuple>& r_tuples, 
                                            vector<Tuple>& non_modifying_tuples,
                                            Relation* join_relation_ptr,
                                            int memory_block_index,
                                            Block* modified_block_ptr) {
 // cout << __FUNCTION__ << endl;
  for (Tuple t1: r_tuples) {
        vector<Tuple> joined_tuples;
        for (Tuple t2: non_modifying_tuples) {
          joined_tuples.push_back(computeTupleJoin(t1, t2, join_relation_ptr));
        }
        modified_block_ptr->clear();
        for (Tuple t: joined_tuples) {
          t.getTuplesPerBlock();
          appendTupleToRelation(join_relation_ptr, 
                                StorageSingleton::getInstance().mem, 
                                memory_block_index, 
                                t);
        }
      } 
}

void query_operator::twoPassJoin(Relation* R, Relation* S, Relation* join_relation_ptr){
  //cout << __FUNCTION__ << endl;
  empty_memory_block();
  for (int i = 0; i < R->getNumOfBlocks(); i++) {
    R->getBlock(i, 0);
    Block* r_block_ptr=StorageSingleton::getInstance().mem.getBlock(0);
    vector<Tuple> r_tuples = r_block_ptr->getTuples();
    for (int j = 0; j < S->getNumOfBlocks(); j++) {
      S->getBlock(j, 1);
      Block* s_block_ptr=StorageSingleton::getInstance().mem.getBlock(1);
      std::vector<Tuple> s_tuples = s_block_ptr->getTuples();
      performVectorTupleJoin(r_tuples, s_tuples, join_relation_ptr, 2, s_block_ptr);
    }
  }
}


void query_operator::onePassJoin(Relation* R, Relation* S, Relation* join_relation_ptr){
  // fetch all blocks of R to mem
  // fetch single block of S to mem
  // store a vector 
  // modify and write back to disk
  empty_memory_block();

  // cout << __FUNCTION__ << endl;
  R->getBlocks(0, 0, R->getNumOfBlocks());
  int memory_block_index = R->getNumOfBlocks();
  for (int i =0 ; i < S->getNumOfBlocks(); i++) {
    S->getBlock(i, memory_block_index);
    Block* block_ptr=StorageSingleton::getInstance().mem.getBlock(memory_block_index);
    vector<Tuple> non_modifying_tuples = block_ptr->getTuples();
    for (int j = 0; j < memory_block_index; j++) {
      Block* r_block_ptr=StorageSingleton::getInstance().mem.getBlock(j);
      vector<Tuple> r_tuples = r_block_ptr->getTuples();
      performVectorTupleJoin(r_tuples, non_modifying_tuples, join_relation_ptr, memory_block_index, block_ptr);
    }
  }
}

Tuple query_operator::computeTupleJoin(Tuple& a, Tuple& b, Relation* join_relation_ptr) {
  // cout << __FUNCTION__ << endl;
  Tuple tuple = join_relation_ptr->createTuple();
  for (int i = 0; i < a.getNumOfFields(); i++) {
    if (FIELD_TYPE::INT == a.getSchema().getFieldType(i)) {
      tuple.setField(i, a.getField(i).integer);  
    } else {
      tuple.setField(i, *a.getField(i).str);  
    }
    
  }
  for (int i = 0; i < b.getNumOfFields(); i++) {
    int k = i + a.getNumOfFields();
    if (FIELD_TYPE::INT == b.getSchema().getFieldType(i)) {
      tuple.setField(k, b.getField(i).integer);
    } else {
      tuple.setField(k, *b.getField(i).str);
      
    }
    
  }

  return tuple;
}
string query_operator::getNormalCrossJoin(string table_one, string table_two) {
  // Create a schema
  //cout << "Creating a schema" << endl;
  
  vector<Tuple> non_modifying_tuples;
  vector<string> first_field_names;
  vector<string> second_field_names;
  vector<FIELD_TYPE> first_field_types;
  vector<FIELD_TYPE> second_field_types;



  // Get field names and types for the two relations
  first_field_names=StorageSingleton::getInstance().schema_manager->getSchema(table_one).getFieldNames();
  first_field_types=StorageSingleton::getInstance().schema_manager->getSchema(table_one).getFieldTypes();
  
  second_field_names=StorageSingleton::getInstance().schema_manager->getSchema(table_two).getFieldNames();
  second_field_types=StorageSingleton::getInstance().schema_manager->getSchema(table_two).getFieldTypes();
  
  print_field(first_field_names);
  print_field(second_field_names);
    
  append_table_name(first_field_names, table_one);
  append_table_name(second_field_names, table_two);

  // first_field_names.insert( first_field_names.end(), second_field_names.begin(), second_field_names.end());
  // first_field_types.insert( first_field_types.end(), second_field_types.begin(), second_field_types.end());

  print_field(first_field_names);
  print_field(second_field_names);
  
  // Schema join_schema(first_field_names, first_field_types);

  // string join_table_name = table_one + "#" + table_two;

  Relation* R = StorageSingleton::getInstance().schema_manager->getRelation(table_one);
  Relation* S = StorageSingleton::getInstance().schema_manager->getRelation(table_two);

  string join_table_name;
  Relation* join_relation_ptr;
  //Get the smaller relation
  // Relation* join_relation_ptr=StorageSingleton::getInstance().schema_manager->createRelation(join_table_name, join_schema);
  if (R->getNumOfBlocks() < max_blocks) {
    first_field_names.insert( first_field_names.end(), second_field_names.begin(), second_field_names.end());
    first_field_types.insert( first_field_types.end(), second_field_types.begin(), second_field_types.end());

    print_field(first_field_names);
    print_field(second_field_names);
    
    Schema join_schema(first_field_names, first_field_types);

    join_table_name = table_one + "#" + table_two;
    join_relation_ptr=StorageSingleton::getInstance().schema_manager->createRelation(join_table_name, join_schema);
    onePassJoin(R, S, join_relation_ptr);

  }
  else if (S->getNumOfBlocks() < max_blocks) {
    second_field_names.insert( second_field_names.end(), first_field_names.begin(), first_field_names.end());
    second_field_types.insert( second_field_types.end(), first_field_types.begin(), first_field_types.end());

    print_field(first_field_names);
    print_field(second_field_names);
    
    Schema join_schema(second_field_names, second_field_types);

    join_table_name = table_one + "#" + table_two;
    join_relation_ptr=StorageSingleton::getInstance().schema_manager->createRelation(join_table_name, join_schema);
    
    onePassJoin(S, R, join_relation_ptr);
  }
  else {
    first_field_names.insert( first_field_names.end(), second_field_names.begin(), second_field_names.end());
    first_field_types.insert( first_field_types.end(), second_field_types.begin(), second_field_types.end());

    print_field(first_field_names);
    print_field(second_field_names);
    
    Schema join_schema(first_field_names, first_field_types);

    join_table_name = table_one + "#" + table_two;
    join_relation_ptr=StorageSingleton::getInstance().schema_manager->createRelation(join_table_name, join_schema);
    
    twoPassJoin(R, S, join_relation_ptr);
  }
  return join_table_name;
}
void query_operator::test_insert_query(Relation* relation_ptr) {
  //cout << __FUNCTION__ << endl;
  relation_ptr->getBlocks(0,0,1);
  //cout << "Now the memory contains: " << endl;
  //cout << StorageSingleton::getInstance().mem << endl;

}
void query_operator::execute_drop_query() {
  //cout << "Deleting : "<< table_name;
  StorageSingleton::getInstance().schema_manager->deleteRelation(table_name);
  #ifdef CHECK_DROP_ACC
  StorageSingleton::getInstance().schema_manager->getRelation(table_name);
  #endif
  clear_all_values();
}




void query_operator::clear_all_values() {
  table_name.clear();
  attr_name_list.clear();
  attr_type_list.clear();
  attr_value_list.clear();
}
