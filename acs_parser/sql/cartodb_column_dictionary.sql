copy (
    select x.cartodb_table, a.table_name, concat(concat (b.table_name,'_'), substr(b.column_name, 7)), b.description from acs_dataset a, acs_dataset_columns b , cartodb_table_map x where a.table_name = b.table_name and a.table_name = x.acs_table order by a.table_name, b.column_name
) to '/tmp/column_dictionary.csv' with DELIMITER ',' QUOTE '"' csv header;

copy (
    select x.cartodb_table, a.table_name, a.description, a.subject_area from acs_dataset a, cartodb_table_map x where a.table_name = x.acs_table order by a.table_name
) to '/tmp/table_dictionary.csv' with DELIMITER ',' QUOTE '"' csv header;
