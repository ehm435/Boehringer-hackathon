{{ config(materialized='view') }}

with src as (
    select * from {{ ref('base_labs') }}
)

select
    patient_id,

    -- Glucosa / HbA1c
    glucosa_plasmatica_mg_dl,
    hba1c_porcentaje,

    -- Función renal
    creatinina_mg_dl,
    urea_mg_dl,

    -- Perfil lipídico
    colesterol_total_mg_dl,
    colesterol_hdl_mg_dl,
    colesterol_ldl_mg_dl,

    -- Electrolitos
    sodio_mmol_l,
    potasio_mmol_l,

    -- Hemograma básico
    hemoglobina_g_dl,
    hematocrito_porcentaje
from src