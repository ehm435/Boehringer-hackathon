{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ ref('base_raw_extra_information') }}
)

SELECT
    patient_id,
    es_paciente_oncologico
FROM src;
