{{ config(materialized='view') }}

with src as (
    select * from {{ ref('base_questionnaire') }}
)

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
from src
