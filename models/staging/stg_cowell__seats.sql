with seats as (
    
    select GRUP_CD, COUNT(*) as ORDERED_SEATS
	from TRGPAX
	where GRUP_ST between 1 and 5
    and JOIN_TP <> '2'
	group by GRUP_CD

)

select * from seats