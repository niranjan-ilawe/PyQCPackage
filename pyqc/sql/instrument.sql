drop table yield.instr_qc_data ;
create table yield.instr_qc_data (
	qc_date varchar(30) null,
	submit_date varchar(30) null,
	sn varchar(30) null,
	cosmetic_disp varchar(20) null,
	functional_disp varchar(20) null,
	final_disp varchar(20) null,
	qc_attempt varchar(10) null,
	second_sampling varchar(10) null,
	pn varchar(30) null
);

