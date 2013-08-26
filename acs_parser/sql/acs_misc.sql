create table acs_dataset (
    id          INT4 NOT NULL PRIMARY KEY,
    table_name  VARCHAR (64) NOT NULL,
    description VARCHAR (256),
    subject_area  VARCHAR (64) 
);

create table acs_dataset_columns (
    id          INT4 NOT NULL,
    column_name VARCHAR (64) NOT NULL,
    description VARCHAR (256),

    PRIMARY KEY( id, column_name ),
    FOREIGN KEY( id ) REFERENCES acs_dataset
);
