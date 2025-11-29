{% snapshot patient_conditions_snapshot %}

{{
  config(
    target_database = 'HACKATON_SILVER',
    target_schema   = 'SNAPSHOTS',
    unique_key      = 'patient_id',
    strategy        = 'check',
    check_cols      = [
      'hta_diagnosticada_codigo',
      'diabetes_diagnosticada_codigo',
      'tabaquismo_actual_codigo',
      'cardiopatia_diagnosticada_codigo',
      'epoc_u_otro_pulmonar_cronico_codigo'
    ]
  )
}}

select
    patient_id,

    -- Hipertensión
    hta_diagnosticada_codigo,
    hta_tratamiento_farmaco_codigo,

    -- Diabetes
    diabetes_diagnosticada_codigo,
    diabetes_tratamiento_farmaco_codigo,

    -- Tabaquismo
    tabaquismo_vida_codigo,
    tabaquismo_actual_codigo,

    -- Alcohol
    consumo_alcohol_frecuencia_codigo,

    -- Cardiopatía / respiratorio
    cardiopatia_diagnosticada_codigo,
    asma_diagnosticada_codigo,
    epoc_u_otro_pulmonar_cronico_codigo
from {{ ref('stg_patient_conditions') }}

{% endsnapshot %}
