{{ config(materialized='view') }}

SELECT
    CAST("SEQN"    AS NUMBER(38,0)) AS patient_id,

    -- Hipertensión
    CAST("BPQ020"  AS NUMBER(3,0))  AS hta_diagnosticada_codigo,
    CAST("BPQ050A" AS NUMBER(3,0))  AS hta_tratamiento_farmaco_codigo,

    -- Diabetes
    CAST("DIQ010"  AS NUMBER(3,0))  AS diabetes_diagnosticada_codigo,
    CAST("DIQ050"  AS NUMBER(3,0))  AS diabetes_tratamiento_farmaco_codigo,

    -- Tabaquismo
    CAST("SMQ020"  AS NUMBER(3,0))  AS tabaquismo_vida_codigo,     -- ha fumado ≥100 cigarrillos
    CAST("SMQ040"  AS NUMBER(3,0))  AS tabaquismo_actual_codigo,   -- fuma ahora

    -- Alcohol
    CAST("ALQ160"  AS NUMBER(3,0))  AS consumo_alcohol_frecuencia_codigo,

    -- Cardiopatía / respiratorio
    CAST("MCQ010"  AS NUMBER(3,0))  AS cardiopatia_diagnosticada_codigo,
    CAST("MCQ050"  AS NUMBER(3,0))  AS asma_diagnosticada_codigo,
    CAST("MCQ160A" AS NUMBER(3,0))  AS epoc_u_otro_pulmonar_cronico_codigo

from {{ source('raw_data', 'raw_questionnaire') }}
