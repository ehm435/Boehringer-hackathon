{{ config(materialized='view') }}

with src as (
    select * from {{ ref('base_demographic') }}
)

select
    patient_id,
    sexo_biologico_codigo,
    edad_anios,
    nivel_educativo_codigo,
    estado_civil_codigo,
    embarazo_estado_codigo,
    idioma_entrevista_codigo
from src
