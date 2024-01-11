--elliptic dec 2022 data
with base_ellip as (
	select
	id--,	customer
	,(replace(customer, '''', '"')::json ->> 'reference')::numeric as ap_account_id
	--,subject
	,replace(subject, '''', '"')::json ->> 'asset' asset
	,replace(subject, '''', '"')::json ->> 'protocol' protocol
	,replace(subject, '''', '"')::json ->> 'hash' hash
	,replace(subject, '''', '"')::json ->> 'output_address' output_address
--	,blockchain_info 
--	,replace(replace (replace(blockchain_info, '''', '"'),'True','true'),'False','false')::json
--	,strpos(blockchain_info, '\') 	,SUBSTRING ( blockchain_info, strpos(blockchain_info, '\'),12)
	--->'transaction'->>'time' tx_datetime
	,SUBSTRING ( blockchain_info, strpos(blockchain_info, 'time')+8	, strpos(blockchain_info, 'input') - (strpos(blockchain_info, 'time')+12)	)::timestamp  "tx_datetime"
--	,contri_0
--	,replace(replace(replace (replace(contri_0, '''', '"'),'True','true'),'False','false'),'None','abc')::json ->> 'name' name0
	,case when length(contri_0)>0 then SUBSTRING ( contri_0, strpos(contri_0, 'name')+8, strpos(contri_0, 'category')-3 
				- (strpos(contri_0, 'name')+9)) end as "name0"
	,case when length(contri_0)>0 then SUBSTRING ( contri_0, strpos(contri_0, 'category')+12, strpos(contri_0, 'actor_id')-3 - (strpos(contri_0, 'category')+13)) end as "category0"
	,case when length(contri_0)>0 then SUBSTRING ( contri_0, strpos(contri_0, 'is_primary_entity')+19, 5 )::boolean end as is_primary_entity0
	,trim(case when length(contri_0)>0 then
		SUBSTRING ( contri_0, strpos(contri_0, 'contribution_percentage')+25, strpos(contri_0, 'contribution_value') - (strpos(contri_0, 'contribution_percentage')+28)
		) end)::dec(65,2) as "contribution_percentage0"
--	,contri_1
	,case when length(contri_1)>0 then SUBSTRING ( contri_1, strpos(contri_1, 'name')+8, strpos(contri_1, 'category')-3 - (strpos(contri_1, 'name')+9)) end as "name1"
	,case when length(contri_1)>0 then SUBSTRING ( contri_1, strpos(contri_1, 'category')+12, strpos(contri_1, 'actor_id')-3 - (strpos(contri_1, 'category')+13)) end as "category1"
	,case when length(contri_1)>0 then SUBSTRING ( contri_1, strpos(contri_1, 'is_primary_entity')+19, 5 )::boolean end as is_primary_entity1
	,trim(case when length(contri_1)>0 then
		SUBSTRING ( contri_1, strpos(contri_1, 'contribution_percentage')+25, strpos(contri_1, 'contribution_value') - (strpos(contri_1, 'contribution_percentage')+28)
		) end)::dec(65,2) as "contribution_percentage1"
	--,contri_2
	,case when length(contri_2)>0 then SUBSTRING ( contri_2, strpos(contri_2, 'name')+8, strpos(contri_2, 'category')-3 - (strpos(contri_2, 'name')+9)) end as "name2"
	,case when length(contri_2)>0 then SUBSTRING ( contri_2, strpos(contri_2, 'category')+12, strpos(contri_2, 'actor_id')-3 - (strpos(contri_2, 'category')+13)) end as "category2"
	,case when length(contri_2)>0 then SUBSTRING ( contri_2, strpos(contri_2, 'is_primary_entity')+19, 5 )::boolean end as is_primary_entity2
	,trim(case when length(contri_2)>0 then
		SUBSTRING ( contri_2, strpos(contri_2, 'contribution_percentage')+25, strpos(contri_2, 'contribution_value') - (strpos(contri_2, 'contribution_percentage')+28)
		) end)::dec(65,2) as "contribution_percentage2"
	from data_team_staging.elliptic_merged_df_contri_drop_csv
	order by length(contri_0) desc
)select * from base_ellip ;where ap_account_id=15 ;
,prep0 as (
	select date_trunc('month',tx_datetime) tx_datetime, ap_account_id, 
	name0, is_primary_entity0, category0
	, count(*) cnt
	,avg(contribution_percentage0) contribution_percentage0
	,row_number () over (partition by ap_account_id) dup_check
	from base_ellip
	where is_primary_entity0 is true
	group by 1,2,3,4,5 order by 1
) --select * from prep0 where ap_account_id in (46641,1483659,121121) order by ap_account_id;
,prep1 as (
	select date_trunc('month',tx_datetime) tx_datetime, ap_account_id, 
	name1, is_primary_entity1, category1
	, count(*) cnt
	,avg(contribution_percentage1) contribution_percentage1
	,row_number () over (partition by ap_account_id) dup_check
	from base_ellip
	where is_primary_entity1 is true
	group by 1,2,3,4,5 order by 1
) --select count(*) from prep1;
,cross_j as (select a.*, b.*
	from (select distinct ap_account_id from base_ellip) a
	cross join (select distinct date_trunc('month',tx_datetime) tx_datetime from base_ellip) b
)
,ready as (
	select a.*
	, b.name0, b.category0, b.cnt, b.contribution_percentage0
	, c.name1, c.category1, c.cnt, c.contribution_percentage1
	from cross_j a
	left join prep0 b on b.ap_account_id = a.ap_account_id and b.tx_datetime = a.tx_datetime 
		and b.dup_check = 1
	left join prep1 c on c.ap_account_id = a.ap_account_id and c.tx_datetime = a.tx_datetime 
		and c.dup_check = 1
	--where a.ap_account_id in (1141869, 151468, 446655, 920967)
	--group by 1 
	order by 1
) --select*  from ready;
,good as (select date_trunc('month',tx_datetime) tx_datetime
,ap_account_id
, name0 , category0 , contribution_percentage0
from ready
where name0 is not null
) select * from good where ap_account_id in (11)