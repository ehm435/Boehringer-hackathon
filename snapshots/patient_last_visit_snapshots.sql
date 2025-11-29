{% snapshot patient_last_visit_snapshot %}

{{
  config(
    target_database = 'HACKATON_SILVER',
    target_schema   = 'SNAPSHOTS',
    unique_key      = 'patient_id',
    strategy        = 'check',
    check_cols      = [
      'fecha_ultima_visita'
    ]
  )
}}

SELECT
    patient_id,
    fecha_ultima_visita
FROM {{ ref('stg_patient_last_visit') }}

{% endsnapshot %}
