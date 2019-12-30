#!/bin/bash


echo 1 | awk '{for(y=0;y<10000000;y++){ for(i=0;i<8;i++){printf("%d,",int(rand()*1000));}print""; } }'


