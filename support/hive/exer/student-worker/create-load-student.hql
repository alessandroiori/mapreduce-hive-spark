drop table if exists student;
create table student (id int, name string, mat int, age int) row format delimited fields terminated by ',';
load data local inpath '/home/ambpl/Desktop/student.csv' overwrite into table student;

