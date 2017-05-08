# DB_Project2

STEP for compilor
1. lex dbparser.l
2.  yacc -d dbparser.y
3. yacc --verbose --debug -d dbparser.y -o dbparser.cc
4.  cc -c lex.yy.c -o lex.yy.o
5.  g++ lex.yy.o dbparser.cc -o dbparser
6. ./dbparser sqltestfile

