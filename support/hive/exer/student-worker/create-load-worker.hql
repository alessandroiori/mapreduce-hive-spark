drop table if exists worker;
create table worker (id int, name string, mat int, age int) row format delimited fields terminated by ',';
load data local inpath '/home/ambpl/Desktop/worker.csv' overwrite into table worker;

