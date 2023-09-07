

with diseasecte as (select disease_name,
ROW_NUMBER() OVER (PARTITION BY disease_name ORDER BY disease_name)cnt
from test.claims)
, diseasecte2 as (select max(cnt) cnt from diseasecte)

select b.disease_name, b.cnt from diseasecte2 a
inner join diseasecte b
on a.cnt = b.cnt




select birth_date, first_name, last_name from test.subscriber
where birth_date > DATEADD(year, -30, GETDATE())
and sub_id is not null




with sgcte as (select grp_id,ROW_NUMBER() OVER (PARTITION BY grp_id ORDER BY grp_id)cnt
from test.groupsubgroup) , sgcte1 as
(select max(cnt) cnt from sgcte)

select a.grp_id, a.cnt from sgcte a
inner join sgcte1 b
on a.cnt=b.cnt




with hospcte as (select h.hospital_name,
rank() OVER (PARTITION BY p.hospital_id
oRDER BY p.patient_id
)cnt
from test.patient p
join test.hospital h
on p.hospital_id=h.hospital_id), hospcte1 as
(select max(cnt) cnt from hospcte)

select a.hospital_name, a.cnt from hospcte a
inner join hospcte1 b
on a.cnt = b.cnt;




select count(*) from test.claims where claim_or_rejected='N'




with CLAIMS as (select city, ROW_NUMBER() OVER (PARTITION BY city ORDER BY claim_id)cnt
from test.claims c
join test.patient p on c.patient_id=p.patient_id), CLAIMS1 as
(select max(cnt) cnt
from CLAIMS)
select a.city, b.cnt
from CLAIMS a
inner join CLAIMS1 b
on a.cnt = b.cnt




select grp_type,count(grp_id) from test.group group by grp_type




SELECT sg.subgrp_id, Avg(monthly_premium)
FROM test.subscriber s
INNER JOIN test.subgroup sg ON s.subgrp_id = sg.subgrp_id
GROUP BY sg.subgrp_id




with cte as (
select max(premium_written) maxprofit from test.group
)
select g.grp_name,premium_written from cte c
inner join test.group g
on g.premium_written=c.maxprofit




select * from test.patient
where disease_name like '%cancer%'
and patient_birth_date > DATEADD(year, -18, GETDATE())




select * from test.patient p
join test.claims c
on p.patient_id=c.patient_id
where c.claim_amount>=50000




select * from test.patient p
where
patient_gender = 'Female'
and patient_birth_date < DATEADD(year, -40, GETDATE())
and disease_name like '%surgery%'