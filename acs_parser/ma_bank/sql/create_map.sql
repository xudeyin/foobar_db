drop table bank_branch_columns;
create table bank_branch_columns (
    column_name                 char(4) NOT NULL,
    full_name                   varchar(255) NOT NULL,
    lookup_idx                  int,
    year                        int,
    PRIMARY KEY (column_name)
);
