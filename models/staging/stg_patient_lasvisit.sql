{{ config(materialized='view') }}

WITH src AS (
    SELECT * FROM {{ ref('base_raw_extra_information') }}
)

SELECT
    patient_id,
    ultima_visita_ts AS fecha_ultima_visita
FROM src;
