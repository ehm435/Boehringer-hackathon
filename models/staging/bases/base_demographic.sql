{{ config(materialized='view') }}

SELECT
    CAST("SEQN"      AS NUMBER(38,0)) AS patient_id,
    CAST("RIAGENDR"  AS NUMBER(3,0))  AS sexo_biologico_codigo,   -- 1=hombre, 2=mujer
    CAST("RIDAGEYR"  AS NUMBER(3,0))  AS edad_anios,
    CAST("DMDEDUC2"  AS NUMBER(3,0))  AS nivel_educativo_codigo,
    CAST("DMDMARTL"  AS NUMBER(3,0))  AS estado_civil_codigo,
    CAST("RIDEXPRG"  AS NUMBER(3,0))  AS embarazo_estado_codigo,
    CAST("SIALANG"   AS NUMBER(3,0))  AS idioma_entrevista_codigo
from {{ source('raw_data', 'raw_demographic') }}