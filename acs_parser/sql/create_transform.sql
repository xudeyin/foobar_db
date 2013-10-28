CREATE TABLE tl_2011_transform
(
sumlev             int4,
geoid              varchar(12),
logrecno           int4,
state_fips         int4,
county_fips        int4,
tract	           int4,
block_group_id     int4,
state	           char(2)
);

alter table tl_2011_transform add primary key(sumlev, geoid);
SELECT AddGeometryColumn('tl_2011_transform', 'geom26986', 26986, 'MULTIPOLYGON',2);
create index idx_geom_26986 on tl_2011_transform using gist (geom26986);

insert into tl_2011_transform select 
	g.sumlev, t.geoid, g.logrecno, g.state_fips, g.county_fips, g.tract, g.block_group_id, 'MA', ST_Transform(t.geom, 26986) from vg20115 g, tl_2011_25_tract t where g.sumlev=140 and substr(g.geocode, 8)=t.geoid;


insert into tl_2011_transform select 
	g.sumlev, t.geoid, g.logrecno, g.state_fips, g.county_fips, g.tract, g.block_group_id, 'MA', ST_Transform(t.geom, 26986) from vg20115 g, tl_2011_25_bg t where g.sumlev=150 and substr(g.geocode, 8)=t.geoid;

