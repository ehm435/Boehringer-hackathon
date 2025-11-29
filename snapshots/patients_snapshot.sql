{% snapshot patients_snapshot %}

{{
  config(
    target_database = 'HACKATON_SILVER',
    target_schema   = 'SNAPSHOTS',
    unique_key      = 'patient_id',
    strategy        = 'check',
    check_cols      = [
      'nivel_educativo_codigo',
      'estado_civil_codigo',
      'idioma_entrevista_codigo'
    ]
  )
}}

select
    patient_id,
    sexo_biologico_codigo,
    edad_anios,
    nivel_educativo_codigo,
    estado_civil_codigo,
    embarazo_estado_codigo,
    idioma_entrevista_codigo
from {{ ref('stg_patients') }}

{% endsnapshot %}
