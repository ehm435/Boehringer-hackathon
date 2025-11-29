{{ config(materialized='view') }}

with src as (
    select * from {{ ref('base_demographic') }}
),

sexo_norm as (
    select
        *,
        case
            when sexo_biologico_codigo = 1 then 'M'
            when sexo_biologico_codigo = 2 then 'F'
            else null
        end as sexo_biologico
    from src
)

select
    patient_id,
    sexo_biologico as sexo,
    edad_anios,
    nivel_educativo_codigo,
    estado_civil_codigo,
    embarazo_estado_codigo,
    idioma_entrevista_codigo
from sexo_norm
