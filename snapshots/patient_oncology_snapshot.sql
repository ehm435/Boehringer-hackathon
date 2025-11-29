{% snapshot patient_oncology_snapshot %}

{{
  config(
    target_database = 'HACKATON_SILVER',
    target_schema   = 'SNAPSHOTS',
    unique_key      = 'patient_id',
    strategy        = 'check',
    check_cols      = [
      'es_paciente_oncologico'
    ]
  )
}}

SELECT
    patient_id,
    es_paciente_oncologico
FROM {{ ref('stg_patient_oncology_status') }}

{% endsnapshot %}
