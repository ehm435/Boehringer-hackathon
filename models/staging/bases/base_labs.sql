{{ config(materialized='view') }}

SELECT
    CAST("SEQN"   AS varchar) AS patient_id,

    -- Glucosa y HbA1c
    CAST("LBXGLT"  AS NUMBER(6,1))  AS glucosa_plasmatica_mg_dl,
    CAST("LBXGH"   AS NUMBER(4,2))  AS hba1c_porcentaje,

    -- Función renal
    CAST("LBXSCR"  AS NUMBER(5,2))  AS creatinina_mg_dl,
    CAST("LBXSBU"  AS NUMBER(5,1))  AS urea_mg_dl,

    -- Perfil lipídico
    CAST("LBXTC"   AS NUMBER(5,1))  AS colesterol_total_mg_dl,
    CAST("LBDHDD"  AS NUMBER(5,1))  AS colesterol_hdl_mg_dl,
    CAST("LBDLDL"  AS NUMBER(5,1))  AS colesterol_ldl_mg_dl,

    -- Electrolitos
    CAST("LBXSNASI"  AS NUMBER(5,1))  AS sodio_mmol_l,
    CAST("LBXSKSI" AS NUMBER(5,1))  AS potasio_mmol_l,

    -- Hemograma básico
    CAST("LBXHGB"  AS NUMBER(4,1))  AS hemoglobina_g_dl,
    CAST("LBXHCT"  AS NUMBER(5,1))  AS hematocrito_porcentaje

from {{ source('raw_data', 'raw_labs') }}
