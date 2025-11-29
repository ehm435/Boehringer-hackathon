{{ config(
    materialized = 'table'
) }}

-- 1) Demografía (desde snapshot demográfico)
with demo as (

    select
        patient_id                         as patient_id_natural,
        sexo_biologico_codigo,
        edad_anios,
        nivel_educativo_codigo,
        estado_civil_codigo,
        embarazo_estado_codigo,
        idioma_entrevista_codigo
    from {{ ref('patients_snapshot') }}
    where dbt_valid_to is null          -- solo la versión vigente

),

-- 2) Condiciones clínicas (HTA, DM2, etc.)
cond as (

    select
        patient_id,

        hta_diagnosticada_codigo,
        diabetes_diagnosticada_codigo,
        tabaquismo_actual_codigo,
        cardiopatia_diagnosticada_codigo,
        epoc_u_otro_pulmonar_cronico_codigo
    from {{ ref('patient_conditions_snapshot') }}
    where dbt_valid_to is null

),

-- 3) Oncología
onc as (

    select
        patient_id,
        es_paciente_oncologico
    from {{ ref('patient_oncology_snapshot') }}
    where dbt_valid_to is null

),

-- 4) Última visita
lastv as (

    select
        patient_id,
        fecha_ultima_visita
    from {{ ref('patient_last_visit_snapshot') }}
    where dbt_valid_to is null

),

-- 5) Join lógico por patient_id
joined as (

    select
        d.patient_id_natural,

        -- Demografía
        d.sexo_biologico_codigo,
        d.edad_anios,
        d.nivel_educativo_codigo,
        d.estado_civil_codigo,
        d.embarazo_estado_codigo,
        d.idioma_entrevista_codigo,

        -- Condiciones / hábitos
        c.hta_diagnosticada_codigo,
        c.diabetes_diagnosticada_codigo,
        c.tabaquismo_actual_codigo,
        c.cardiopatia_diagnosticada_codigo,
        c.epoc_u_otro_pulmonar_cronico_codigo,

        -- Oncología
        o.es_paciente_oncologico,

        -- Última visita
        lv.fecha_ultima_visita

    from demo d
    left join cond  c  on d.patient_id_natural = c.patient_id
    left join onc   o  on d.patient_id_natural = o.patient_id
    left join lastv lv on d.patient_id_natural = lv.patient_id

)

-- 6) Añadimos surrogate key de dimensión
select
    {{ dbt_utils.generate_surrogate_key(['patient_id_natural']) }} as patient_sk,
    joined.*
from joined
