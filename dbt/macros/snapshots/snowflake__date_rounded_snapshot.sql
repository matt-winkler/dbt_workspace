{% macro snowflake__date_rounded_snapshot(source_view, target_table) %}
  
  {% set distant_future = '9999-12-31' %}
  {% set sql %}
    MERGE INTO {{target.database}}.{{target_table}} TGT
      USING (
        SELECT
            NULL as merge_key,
            GU.RPM_ACCOUNT_ID,
            GU.FREE_USER,
            GU.FREE_INGEST,
            GU.START_DATE
        FROM
            {{source_view}} GU
            INNER JOIN {{target.database}}.{{target_table}} AED ON
                AED.RPM_ACCOUNT_ID = GU.RPM_ACCOUNT_ID
                AND AED.IS_ACTIVE
                AND (
                    NOT(EQUAL_NULL(AED.FREE_USER_COUNT, GU.FREE_USER))
                    OR NOT(EQUAL_NULL(AED.FREE_INGEST_QTY, GU.FREE_INGEST))
                )
        UNION ALL
        SELECT
            RPM_ACCOUNT_ID as merge_key,
            RPM_ACCOUNT_ID,
            FREE_USER,
            FREE_INGEST,
            START_DATE
        FROM
            {{source_view}}
    ) updates
    ON
        TGT.RPM_ACCOUNT_ID = UPDATES.merge_key
        AND TGT.IS_ACTIVE
    WHEN MATCHED
        AND (
            NOT(EQUAL_NULL(TGT.FREE_USER_COUNT, UPDATES.FREE_USER))
            OR NOT(EQUAL_NULL(TGT.FREE_INGEST_QTY, UPDATES.FREE_INGEST))
        )
        THEN UPDATE
            SET TGT.IS_ACTIVE = FALSE,
                TGT.EFFECTIVE_END_TIMESTAMP = DATEADD('ms', -1, UPDATES.START_DATE),
                TGT.EDW_UPDATE_TIMESTAMP = DATEADD('ms', -1, UPDATES.START_DATE),
                TGT.EDW_UPDATE_USER = CURRENT_USER
    WHEN NOT MATCHED
        THEN INSERT (RPM_ACCOUNT_ID, FREE_USER_COUNT, FREE_INGEST_QTY, EFFECTIVE_START_TIMESTAMP, EFFECTIVE_END_TIMESTAMP,
        IS_ACTIVE, EDW_CREATE_TIMESTAMP, EDW_UPDATE_TIMESTAMP, EDW_CREATE_USER, EDW_UPDATE_USER)
        VALUES (UPDATES.RPM_ACCOUNT_ID, UPDATES.FREE_USER, UPDATES.FREE_INGEST, UPDATES.START_DATE, {{ distant_future }},
                TRUE, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_USER, CURRENT_USER)
    ;
    {% endset %}

    {{return(sql)}}


{% endmacro %}