{{ config(materialized='view') }}

with src as (
    select * from {{ ref('base_medications') }}
)

select
    patient_id,
    codigo_medicamento,
    indicador_uso_medicamento_codigo,
    dias_uso_reportados
from src
where codigo_medicamento is not null
