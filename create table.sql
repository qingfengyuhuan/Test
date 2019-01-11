
--创建地址表
create table if not exists index(
index_id int,
index_name string
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile;
--加载数据
load data local inpath '/hadoop2/index.csv' into table work.index;


--创建普通表
create table if not exists aa(
id int,
name string,
index_id int,
Hospitalgrade string,
equalorder string
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile;
--加载数据
load data local inpath '/hadoop2/details.csv' into table work.aa;



--创建分区表
create table if not exists details(
id int,
name string,
Hospitalgrade string,
equalorder string
)
partitioned by(index_id int)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile;
--插入数据
insert into table work.details partition(index_id)
select id,name,index_id,Hospitalgrade,equalorder from work.aa DISTRIBUTE BY index_id; 


insert into table work.details partition(index_id=110101)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110101;
insert into table work.details partition(index_id=110102)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110102;
insert into table work.details partition(index_id=110105)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110105;
insert into table work.details partition(index_id=110106)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110106;
insert into table work.details partition(index_id=110107)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110107;
insert into table work.details partition(index_id=110108)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110108;
insert into table work.details partition(index_id=110109)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110109;
insert into table work.details partition(index_id=110111)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110111;
insert into table work.details partition(index_id=110112)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110112;
insert into table work.details partition(index_id=110113)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110113;
insert into table work.details partition(index_id=110114)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110114;
insert into table work.details partition(index_id=110115)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110115;
insert into table work.details partition(index_id=110116)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110116;
insert into table work.details partition(index_id=110117)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110117;
insert into table work.details partition(index_id=110228)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110228;
insert into table work.details partition(index_id=110229)
select id,name,Hospitalgrade,equalorder
from work.aa
where index_id=110229;






--创建简略表
create table if not exists brief(
name string,
generalclass string,
classification string,
organization string
)
row format delimited terminated by ','
lines terminated by '\n'
stored as textfile;

load data local inpath '/hadoop2/brief.csv' into table work.brief;


select * from work.brief where organization="非营利性医疗机构";





index表字段 index_id int, index_name string
			地址id		  地址
details表字段id int,name string,Hospitalgrade string,equalorder string,index_id int
			医院编号 医院名称  医院级别	 医院等次  地址id
brief表字段name string,generalclass string,classification string,organization string
		  医院名称  大类  中类  机构管理名称
select b.id,b.name,c.generalclass,c.classification,b.Hospitalgrade,b.equalorder,a.index_name
from work.index as a join work.details as b
on a.index_id=b.index_id join work.brief as c 
on b.name =c.name;
编写需求：
2、找出医院等级为三级，医院等次为甲等 机构管理名称为非营利性医疗机构的所有医院


select a.id,a.name,a.Hospitalgrade,a.equalorder,b.organization
from work.details as a join work.brief as b
on a.name=b.name where a.Hospitalgrade="三级" and a.equalorder="甲等" and b.organization="非营利性医疗机构";


3、找出各区中医院等次为合格 机构管理名称为营利性医疗机构的所有医院

select b.id,b.name,b.equalorder,c.organization,a.index_name
from work.index as a join work.details as b
on a.index_id=b.index_id join work.brief as c 
on b.name =c.name 
where b.equalorder="合格" and c.organization="营利性医疗机构";

4、找出在房山区和大兴区，医院等次为未评 医院等级为未评的所有医院


select b.id,b.name,b.equalorder,b.Hospitalgrade,a.index_name
from work.index as a join work.details as b
on a.index_id=b.index_id
where a.index_id in(110111,110115)and
b.equalorder='未评'and b.Hospitalgrade='未评';


5、随机选出十个医院 如果医院等次为甲等 则显示达标 反之显示不达标
--select b.id as id,b.name as name,b.equalorder as equalorder,
--row_number() over (partition by a.index_name order by a.index_id desc) as rank
--from work.index as a join work.details as b on 
--a.index_id=b.index_id order by rand() limit 1;

select a.id,a.name,a.index_name,a.equalorder,
case when a.equalorder="甲等" then '达标' else '不达标' end
from 
(select b.id as id,b.name as name,a.index_name as index_name,b.equalorder as equalorder,
row_number() over (partition by a.index_name order by a.index_id desc) as rank
from work.index as a join work.details as b on 
a.index_id=b.index_id order by rand() limit 10) as a;



6、分析海淀区医院中中医院的数量与东城区的医院中中医院的数量那个多
select b.id as id,b.name as name,a.index_name as index_name
from work.index as a join work.details as b on
a.index_id=b.index_id where a.index_id in(110108,110101);

select a.index_name,count(b.classification)
from
(select b.id as id,b.name as name,a.index_name as index_name
from work.index as a join work.details as b on
a.index_id=b.index_id where a.index_id in(110108,110101)) as a join work.brief as b on
a.name=b.name where b.classification=reflect('java.net.URLDecoder', 'decode','A2.中医医院' , 'UTF-8') group by index_name; 


index表字段 index_id int, index_name string
			地址id		  地址
details表字段id int,name string,Hospitalgrade string,equalorder string,index_id int
			医院编号 医院名称  医院级别	 医院等次  地址id
brief表字段name string,generalclass string,classification string,organization string
		  医院名称  大类  中类  机构管理名称
7、计算出北京市各个类型医院的总数 并算出每个类型医院占总医院的比率

A1.综合医院	318
A2.中医医院	158
A3.中西医结合医院	26
A4.民族医院	3
A5.专科医院	170
A7.护理院(站)	7
--求出小数
select a.classification as classification,count(a.classification) as cc,count(a.classification)/682 as dd
from 
(select b.classification as classification
from work.details as a join work.brief as b on
a.name=b.name join work.index as c on a.index_id=c.index_id)as a group by a.classification;

--把小数转化为百分数
select a.classification ,a.cc,CONCAT(cast(a.dd *100 as decimal(10,2)),'', '%' )
from 
(select a.classification as classification,count(a.classification) as cc,count(a.classification)/682 as dd
from 
(select b.classification as classification
from work.details as a join work.brief as b on
a.name=b.name join work.index as c on a.index_id=c.index_id)as a group by a.classification) as a;




8、








