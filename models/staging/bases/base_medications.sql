{{ config(materialized='view') }}

SELECT
    CAST("SEQN"    AS varchar) AS patient_id,
    CAST("RXDUSE"   AS NUMBER(3,0))  AS indicador_uso_medicamento_codigo,
    "RXDDRUG"                       AS nombre_medicamento,
    "RXDDRGID"                      AS codigo_medicamento,
    CAST("RXDDAYS"  AS NUMBER(6,0))  AS dias_uso_reportados

from {{ source('raw_data', 'raw_medications') }}
