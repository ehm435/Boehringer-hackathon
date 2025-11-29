{{ config(materialized='view') }}

with src as (
    select * from {{ ref('base_examination') }}
)

select
    patient_id,

    -- Tensión arterial (mmHg)
    tension_sistolica_1_mm_hg,
    tension_sistolica_2_mm_hg,
    tension_sistolica_3_mm_hg,
    tension_sistolica_4_mm_hg,

    tension_diastolica_1_mm_hg,
    tension_diastolica_2_mm_hg,
    tension_diastolica_3_mm_hg,

    -- Antropometría
    peso_kg,
    talla_cm,
    imc,
    cintura_cm
from src
