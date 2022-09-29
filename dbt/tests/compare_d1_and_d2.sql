{{ 
  audit_helper.compare_all_columns(
    a_relation=ref('dataset_1'),
    b_relation=ref('dataset_2'),
    primary_key='dt'
  ) 
}}
where conflicting_values > 0

