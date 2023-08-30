create application role if not exists app_instance_role;

create or alter versioned schema app_instance_schema;

 

create or replace streamlit app_instance_schema.streamlit from '/libraries' main_file='streamlit.py';

CREATE SEQUENCE APP_INSTANCE_SCHEMA.INC_1_SEQ START 1 INCREMENT 1;
CREATE TABLE IF NOT EXISTS APP_INSTANCE_SCHEMA.GSHEETS_PIPELINE(p_id NUMBER DEFAULT APP_INSTANCE_SCHEMA.INC_1_SEQ.NEXTVAL, spreadsheet_id VARCHAR, sheet_name VARCHAR, Auth_secret VARCHAR, warehouse_name VARCHAR, database_name VARCHAR, schema_name VARCHAR, table_name VARCHAR, pipeline_name VARCHAR, update_freq VARCHAR);





GRANT INSERT, SELECT, UPDATE, DELETE ON TABLE APP_INSTANCE_SCHEMA.GSHEETS_PIPELINE TO APPLICATION ROLE APP_INSTANCE_ROLE;
-- GRANT ALL ON SEQUENCE Autoincrement TO APPLICATION ROLE APP_INSTANCE_ROLE;
GRANT ALL ON SEQUENCE APP_INSTANCE_SCHEMA.INC_1_SEQ TO APPLICATION ROLE APP_INSTANCE_ROLE;
 

-- create or replace function app_instance_schema.gspread()

--    returns varchar

--    language python

--    runtime_version = 3.8

--    packages = ('snowflake-snowpark-python')

--    imports = ('/gspread.zip')

--    handler = 'run';

 

-- Grant usage and permissions on objects

grant usage on schema app_instance_schema to application role app_instance_role;

grant usage on streamlit app_instance_schema.streamlit to application role app_instance_role;