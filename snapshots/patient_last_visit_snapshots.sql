{% snapshot patient_last_visit_snapshot %}

{{
  config(
    target_database = 'HACKATON_SILVER',
    target_schema   = 'SNAPSHOTS',
    unique_key      = 'patient_id',
    strategy        = 'timestamp',
    updated_at      = 'fecha_ultima_visita',
    invalidate_hard_deletes = true
  )
}}

select
    patient_id,
    fecha_ultima_visita
from {{ ref('stg_patient_last_visit') }}

{% endsnapshot %}
