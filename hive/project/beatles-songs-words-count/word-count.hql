create table word_count as
select w.word, count(1) as count
from 
(select explode(split(line, '\s')) as word from student) w
group by w.word
order by w.word;
