{% macro create_test_proc_ANSI() %}
{% set sql %}
CREATE PROCEDURE TPC_SBX_ANALYTICS_DB.INGESTION.TestSP_ANSI
AS
INSERT INTO TPC_SBX_ANALYTICS_DB.INGESTION.POC_Testing_Anit(EMPLOYEE_NAME) SELECT * from database.schema.table;
{% endset %}
{% do run_query(sql) %}
{% endmacro %}