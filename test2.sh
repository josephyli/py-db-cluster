./test2-josephyl-2.pre # creates databases and tables
#./run3.sh test2-josephyl-2.cfg test2-josephyl-2.sql
./test2-josephyl-2.post | sort > test2-josephyl-2.post.out
diff ./test2-josephyl-2.post.out ./test2-josephyl-2.post.exp
