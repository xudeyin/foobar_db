create view vg20115 as SELECT g20115.type, g20115.state, g20115.sumlev, g20115.region_id AS logrecno, 
    g20115.state_fips, g20115.county_fips, g20115.tract, g20115.block_group_id, 
        g20115.geocode
           FROM g20115;

