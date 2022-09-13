{% macro custom_snapshot_build(source_relation, target_relation) %}
{% set sql %}
    MERGE INTO {{target_relation}} TGT
    USING (
        SELECT
            NULL as merge_key,
            GU.*
        FROM
            {{source_relation}} GU
            INNER JOIN {{target_relation}} AED ON
                AED.RPM_ACCOUNT_ID = GU.RPM_ACCOUNT_ID
                AND AED.IS_ACTIVE
                AND (
                    NOT(EQUAL_NULL(AED.FREE_USER_COUNT, GU.FREE_USER_COUNT))
                    OR NOT(EQUAL_NULL(AED.FREE_INGEST_QTY, GU.FREE_INGEST_QTY))
                )
        UNION ALL
        SELECT
            RPM_ACCOUNT_ID as merge_key,
            *
        FROM
            {{source_relation}}
    ) updates
    ON
        TGT.RPM_ACCOUNT_ID = UPDATES.merge_key
        AND TGT.IS_ACTIVE
    WHEN MATCHED
        AND (
            NOT(EQUAL_NULL(TGT.FREE_USER_COUNT, UPDATES.FREE_USER_COUNT))
            OR NOT(EQUAL_NULL(TGT.FREE_INGEST_QTY, UPDATES.FREE_INGEST_QTY))
        )
        THEN UPDATE
            SET TGT.IS_ACTIVE = FALSE,
                TGT.EFFECTIVE_END_TIMESTAMP = DATEADD('ms', -1, UPDATES.EFFECTIVE_START_TIMESTAMP),
                TGT.EDW_UPDATE_TIMESTAMP = DATEADD('ms', -1, UPDATES.EFFECTIVE_START_TIMESTAMP),
                TGT.EDW_UPDATE_USER = CURRENT_USER
    WHEN NOT MATCHED
        THEN INSERT (RPM_ACCOUNT_ID, FREE_USER_COUNT, FREE_INGEST_QTY, EFFECTIVE_START_TIMESTAMP, EFFECTIVE_END_TIMESTAMP,
        IS_ACTIVE, EDW_CREATE_TIMESTAMP, EDW_UPDATE_TIMESTAMP, EDW_CREATE_USER, EDW_UPDATE_USER)
        VALUES (UPDATES.RPM_ACCOUNT_ID, UPDATES.FREE_USER_COUNT, UPDATES.FREE_INGEST_QTY, UPDATES.EFFECTIVE_START_TIMESTAMP,
                UPDATES.EFFECTIVE_END_TIMESTAMP, UPDATES.IS_ACTIVE, UPDATES.EDW_CREATE_TIMESTAMP, UPDATES.EDW_UPDATE_TIMESTAMP, UPDATES.EDW_CREATE_USER, 
                UPDATES.EDW_UPDATE_USER)
    ;
{% endset %}
 {{return(sql)}}
{% endmacro %}