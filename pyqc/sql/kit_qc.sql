-- Staging table
create schema kitqc;
drop table kitqc.metrics_stg ;
create table kitqc.metrics_stg (
    family varchar(20) null,
    sequencer varchar(20) null,
    "description" varchar(100) null,
    metric varchar(100) null,
    "value" numeric null,
    "filename" varchar(255) null,
    "date" varchar(30) null,
    qc_by varchar(30) null,
    pn_descrip varchar(100) null,
    pn varchar(100) null,
    "ln" varchar(50) null,
    site varchar(10) null
);

-- Final tables
drop table kitqc.file_m;
create table kitqc.file_m (
    filefk serial primary key,
    "filename" varchar(255) not null,
    --run
    qc_date date not null,
    qc_by varchar(30) not null,
    unique("filename")
);

drop table kitqc.product_m;
create table kitqc.product_m (
    pnfk serial primary key,
    pn varchar(100) not null,
    pn_descrip varchar(100) not null,
    "ln" varchar(50) not null,
    family varchar(20) not null,
    site varchar(10) not null,
    unique(pn, pn_descrip, ln, family, site)
);

--drop table kitqc.lot_m;
--create table kitqc.lot_m (
--	lnfk serial primary key,
--    "ln" varchar(50) not null,
--    family varchar(20) not null,
--    site varchar(10) not null,
--    unique(ln, family)
--);

drop table kitqc.metric_m;
create table kitqc.metric_m (
	obsid serial primary key,
    sequencer varchar(20) null,
    "sample" varchar(100) null,
    metric varchar(100) null,
    "value" numeric null,
	filefk int references kitqc.file_m(filefk) on delete cascade,
	pnfk int references kitqc.product_m(pnfk) on delete cascade,
    --lnfk int references kitqc.lot_m(lnfk) on delete cascade,
    unique(filefk, pnfk, sample, metric)
);

-- Stored Procedure
CREATE OR REPLACE PROCEDURE kitqc.sp_upload_qc123_data(id integer)
 LANGUAGE plpgsql
AS $procedure$
begin

--insert into file master
insert into kitqc.file_m (filename, qc_date, qc_by)
select distinct
	"filename",
	--run,''
	cast(date as date) as qc_date,
	qc_by
from kitqc.metrics_stg
on conflict do nothing;

-- insert into product master
insert into kitqc.product_m (pn, pn_descrip, ln, family, site)
select distinct 
	pn, pn_descrip, ln, family, site
from kitqc.metrics_stg 
on conflict do nothing;

-- insert into lot master
--insert into kitqc.lot_m (ln, family, site)
--select distinct stg.ln, stg.family, stg.site
--from kitqc.metrics_stg stg
--on conflict do nothing;

-- insert into metric master 
insert into kitqc.metric_m (filefk, pnfk, sequencer, sample, metric, value)
select f.filefk, p.pnfk, stg.sequencer, stg.description, stg.metric, stg.value
from kitqc.metrics_stg stg
join kitqc.file_m f
	on stg.filename = f.filename
join kitqc.product_m p 
	on stg.pn = p.pn 
	and stg.pn_descrip  = p.pn_descrip
	and stg.ln = p.ln 
	and stg.family = p.family
	and stg.site = p.site
on conflict (filefk, pnfk, sample, metric)
do update set value = EXCLUDED.value;

end
$procedure$
;

select count(1)
from kitqc.metric_m ;

select * from kitqc.metrics_stg 
where description = 'T1_161555_163095_161983_162960'
and metric = 'Valid Barcodes'
and ln = '200148'


update kitqc.metrics_stg set value = 0.999
where description = 'T1_161555_163095_161983_162960'
and metric = 'Valid Barcodes'
and ln = '200148'; 

select * 
from kitqc.metric_m 
where sample = 'T1_161555_163095_161983_162960'
and metric = 'Valid Barcodes'