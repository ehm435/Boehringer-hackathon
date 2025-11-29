{{ config(materialized='view') }}

SELECT
    CAST("SEQN"   AS NUMBER(38,0)) AS patient_id,

    -- Tensión arterial (mmHg)
    CAST("BPXSY1" AS NUMBER(3,0))  AS tension_sistolica_1_mm_hg,
    CAST("BPXSY2" AS NUMBER(3,0))  AS tension_sistolica_2_mm_hg,
    CAST("BPXSY3" AS NUMBER(3,0))  AS tension_sistolica_3_mm_hg,
    CAST("BPXSY4" AS NUMBER(3,0))  AS tension_sistolica_4_mm_hg,

    CAST("BPXDI1" AS NUMBER(3,0))  AS tension_diastolica_1_mm_hg,
    CAST("BPXDI2" AS NUMBER(3,0))  AS tension_diastolica_2_mm_hg,
    CAST("BPXDI3" AS NUMBER(3,0))  AS tension_diastolica_3_mm_hg,

    -- Peso / altura / IMC / cintura
    CAST("BMXWT"    AS NUMBER(6,2)) AS peso_kg,
    CAST("BMXHT"    AS NUMBER(5,2)) AS talla_cm,
    CAST("BMXBMI"   AS NUMBER(5,2)) AS imc,
    CAST("BMXWAIST" AS NUMBER(5,2)) AS cintura_cm

from {{ source('raw_data', 'raw_examination') }}

