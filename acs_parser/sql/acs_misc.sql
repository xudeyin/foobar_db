drop table acs_dataset_columns;
drop table acs_dataset;

create table acs_dataset (
    state                     char(2) not null,
    table_name                varchar(32) not null,
    file_name                 varchar(32) not null,
    start_pos                 int4 not null,
    length                    int4 not null,
    description               varchar(256) not null,
    subject_area              varchar(128)
);

create table acs_dataset_columns (
    state                     char(2) not null,
    table_name                varchar(32) not null,
    column_name               varchar(32) not null,
    description               varchar(256) not null,
    primary key (state, table_name, column_name)
);
