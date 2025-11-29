{{ config(materialized='view') }}

SELECT
    -- ID paciente
    CAST("SEQN" AS varchar) AS patient_id,

    -- Fecha/hora de la última visita
    TO_TIMESTAMP_NTZ("ULTIMA_VISITA") AS ultima_visita_ts,

    -- Paciente oncológico (True/False)
    CAST("ESTADO_ONCOLOGICO" AS BOOLEAN) AS es_paciente_oncologico

FROM MEDIC_RAW.RAW_DATA.raw_extra_information
