{{ config(materialized='view') }}

with src as (
    select * from {{ ref('base_medications') }}
),

clean as (
    select
        codigo_medicamento,
        nombre_medicamento
    from src
    where codigo_medicamento is not null
),

dedup as (
    select
        codigo_medicamento,
        any_value(nombre_medicamento) as nombre_medicamento
    from clean
    group by codigo_medicamento
)

select *
from dedup
