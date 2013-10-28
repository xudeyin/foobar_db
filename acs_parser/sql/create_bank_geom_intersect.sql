drop table bank_geom_intersect;
create table bank_geom_intersect
(
    bank_id                int4  	not null,
    radius                 float        not null,
    geoid                  varchar(64)  not null,
    logrecno               int4         not null,
    area1	           float,
    area2		   float,
    intersect_area         float,
    ratio                  float,
    srid                   int4,
    primary key(bank_id, radius, geoid)
);

create index idx_bank_intersect_logrecno on bank_geom_intersect (logrecno);
