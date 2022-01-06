drop table yield.qc_data;
create table yield.qc_data (
    pn varchar(50) null,
    descrip varchar(100) null,
    wo varchar(100) null,
    assay varchar(50) null,
    submit_date varchar(50) null,
    start_date varchar(50) null,
    end_date varchar(50) null,
    is_repeat varchar(50) null,
    status varchar(50) null,
    schedule varchar(50) null,
    clean_wo varchar(50) null,
    product varchar(100) null,
    pass_rate varchar(50) null,
    item_type varchar(50) null,
    final_testing varchar(50) null,
    kpi_family varchar(50) null,
    queue_time varchar(50) null,
    process_time varchar(50) null
);

drop table yield.qc_data_sg;
create table yield.qc_data_sg (
    pn varchar(20) null,
    descrip varchar(100) null,
    wo varchar(40) null,
    assay varchar(50) null,
    submit_date varchar(20) null,
    start_date varchar(20) null,
    end_date varchar(20) null,
    status varchar(20) null,
    failed_assay varchar(50) null,
    item_type varchar(20) null
);