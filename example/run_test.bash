#!/bin/bash

PREFIX=${1:-"../build/example"} 

## purpose : sanity tests

s3select_calc() 
{
l="$*"  
res=$( echo 1 | "$PREFIX"/s3select_example -q  "select ${l} from stdin;" ) 
echo $res | sed 's/.$//'
}

# for running expressions , later to be compare with s3select
awk_calc()
{
cat << @@ > awk.tmp.bash
#!/bin/bash

echo 1 | awk '{print $*;}'
@@

chmod +x ./awk.tmp.bash

./awk.tmp.bash

}

# create c file with expression , compile it and run it.
c_calc()
{
cat << @@ > tmp.c

#include <stdio.h>
int main()
{
printf("%f\n",$*);
}
@@
gcc tmp.c
./a.out
}

python_generator()
{
##generate arithmetic expression of depth X ( nested expresion )
## TODO same for logical expression
cat << @@ > tmp.py
import random
import sys

def expr(depth):
    if depth==1 or random.random()<1.0/(2**depth-1):
        return str(int(random.random() * 100) + 1)+".0"
    return '(' + expr(depth-1) + random.choice(['+','-','*','/']) + expr(depth-1) + ')'

print expr( int(sys.argv[1]) )
@@
}

expr_test()
{
## test the arithmetic evaluation of s3select against C program 
for i in {1..100}
do
	e=$(python ./expr_genrator.py 5)
	echo expression=$e
	r1=$(s3select_calc "$e")
	r2=$(c_calc "$e")

	## should be zero or very close to zero; ( s3select is C compile program )
	echo 1 | awk -v e=$e -v r1=$r1 -v r2=$r2 'function abs(n){if(n<0) return -n; else return n;}{if ( abs(r1-r2)> 0.00001 ) {print "MISSMATCH result for expression",e;}}'
done
}

aggregate_test()
{
## generate_rand_csv is generating with the same seed 


echo check sum 
echo $("$PREFIX"/generate_rand_csv 10 10 | "$PREFIX"/s3select_example -q 'select sum(int(_1)) from stdin;') $("$PREFIX"/generate_rand_csv 10 10 | awk 'BEGIN{FS=",";} {s+=$1;} END{print s;}')
echo check min 
echo $("$PREFIX"/generate_rand_csv 10 10 | "$PREFIX"/s3select_example -q 'select min(int(_1)) from stdin;') $("$PREFIX"/generate_rand_csv 10 10 | awk 'BEGIN{FS=",";min=100000;} {if(min>$1) min=$1;} END{print min;}' )
echo check max 
echo $("$PREFIX"/generate_rand_csv 10 10 | "$PREFIX"/s3select_example -q 'select max(int(_1)) from stdin;') $("$PREFIX"/generate_rand_csv 10 10 | awk 'BEGIN{FS=",";max=0;} {if(max<$1) max=$1;} END{print max;}' )
echo check substr and count 
echo  $("$PREFIX"/generate_rand_csv 10000 10 | "$PREFIX"/s3select_example -q 'select count(int(_1)) from stdin where int(_1)>200 and int(_1)<250;') \
$("$PREFIX"/generate_rand_csv 10000 10 | "$PREFIX"/s3select_example -q 'select substr(_1,1,1) from stdin where int(_1)>200 and int(_1)<250;' | uniq -c | awk '{print $1;}')

}

###############################################################

expr_test
aggregate_test

rm tmp.c a.out
