\COPY bank_branch_columns from '../data/bank_branch_column_map.csv' DELIMITER ',' CSV;
\COPY bank_branch from '../data/bank_branch_utf.csv' DELIMITER ',' CSV;

select AddGeometryColumn('bank_branch', 'geom', 4269, 'POINT', 2);

update bank_branch set geom = ST_SetSRID(ST_MakePoint(c016, c015), 4269);

create index idx_ma_bank_branches_geom on bank_branch using GIST(geom);
