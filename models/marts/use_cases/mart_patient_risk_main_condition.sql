-- models/marts/mart_patient_risk_main_condition.sql

{{ config(
    materialized = 'view'
) }}

with base as (

    select
        f.fact_patient_clinical_sk,
        f.patient_sk,
        p.patient_id_natural,
        p.edad_anios,
        p.sexo_biologico_codigo,
        d.full_date                 as fecha_referencia,

        -- directamente desde la fact
        f.indice_riesgo,
        f.prioridad_semaforo,
        f.condicion_principal_riesgo

    from {{ ref('fact_patient_clinical') }} f
    join {{ ref('dim_patient') }} p
      on f.patient_sk = p.patient_sk
    join {{ ref('dim_date') }} d
      on f.date_sk = d.date_sk
),

latest_per_patient as (

    select
        *
    from base
    qualify row_number() over (
        partition by patient_sk
        order by fecha_referencia desc
    ) = 1

)

select
    patient_sk,
    patient_id_natural,
    fecha_referencia,
    edad_anios,
    sexo_biologico_codigo,
    indice_riesgo,
    prioridad_semaforo,
    condicion_principal_riesgo
from latest_per_patient
