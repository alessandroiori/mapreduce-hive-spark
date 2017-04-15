drop table if exists beatles;
create table beatles (line string);
load data local inpath '/home/ambpl/eclipse/workspace/bigdata/hive-files/query/exer/every-beatles-lyrics.txt' overwrite into table beatles;

