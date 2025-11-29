{{ config(
    materialized = 'table'
) }}

-- 1) Dimensión paciente (ya consolidada)
with dim_p as (

    select
        patient_sk,
        patient_id_natural,
        fecha_ultima_visita
    from {{ ref('dim_patient') }}

),

-- 2) Vitals (TA, IMC, etc.)
vitals as (

    select
        patient_id,
        -- Para simplificar, usamos la primera toma como "media"
        tension_sistolica_1_mm_hg  as tension_sistolica_media,
        tension_diastolica_1_mm_hg as tension_diastolica_media,
        imc
    from {{ ref('stg_patient_vitals') }}

),

-- 3) Labs (últimos valores disponibles)
labs as (

    select
        patient_id,
        glucosa_plasmatica_mg_dl,
        hba1c_porcentaje,
        creatinina_mg_dl,
        urea_mg_dl,
        colesterol_total_mg_dl,
        colesterol_hdl_mg_dl,
        colesterol_ldl_mg_dl,
        sodio_mmol_l,
        potasio_mmol_l,
        hemoglobina_g_dl,
        hematocrito_porcentaje
    from {{ ref('stg_patient_labs') }}

),

-- 4) Medications agregadas por paciente
meds as (

    select
        patient_id,
        count(*)                                        as num_medicamentos_activos,
        avg(dias_uso_reportados)                       as dias_medios_uso_reportados
    from {{ ref('stg_patient_medications') }}
    group by patient_id

),

-- 5) Join clínico por paciente (sin aún date_sk)
joined as (

    select
        p.patient_sk,
        p.patient_id_natural,

        -- Calculamos fecha de referencia para la fact:
        -- Si hay última visita, usamos esa fecha; si no, current_date()
        coalesce(
            cast(p.fecha_ultima_visita as date),
            current_date()
        ) as fecha_referencia,

        -- Vitals
        v.tension_sistolica_media,
        v.tension_diastolica_media,
        v.imc,

        -- Labs
        l.glucosa_plasmatica_mg_dl,
        l.hba1c_porcentaje,
        l.creatinina_mg_dl,
        l.urea_mg_dl,
        l.colesterol_total_mg_dl,
        l.colesterol_hdl_mg_dl,
        l.colesterol_ldl_mg_dl,
        l.sodio_mmol_l,
        l.potasio_mmol_l,
        l.hemoglobina_g_dl,
        l.hematocrito_porcentaje,

        -- Medications resumidas
        coalesce(m.num_medicamentos_activos, 0)        as num_medicamentos_activos,
        m.dias_medios_uso_reportados,

        -- Métricas de utilización (placeholder si aún no tienes tabla de urgencias/no show)
        cast(null as number(38,0)) as num_urgencias_12m,
        cast(null as number(38,0)) as num_no_show_12m,
        cast(null as number(38,0)) as num_medicos_distintos_12m

    from dim_p p
    left join vitals v on p.patient_id_natural = v.patient_id
    left join labs   l on p.patient_id_natural = l.patient_id
    left join meds   m on p.patient_id_natural = m.patient_id

),

-- 6) Join con dim_date para sacar el date_sk
joined_with_date as (

    select
        j.*,
        d.date_sk
    from joined j
    left join {{ ref('dim_date') }} d
        on d.full_date = j.fecha_referencia

)

-- 7) Fact final con surrogate key
select
    {{ dbt_utils.generate_surrogate_key(['patient_sk', 'date_sk']) }} as fact_patient_clinical_sk,

    date_sk,
    patient_sk,

    -- Vitals
    tension_sistolica_media,
    tension_diastolica_media,
    imc,

    -- Labs
    glucosa_plasmatica_mg_dl,
    hba1c_porcentaje,
    creatinina_mg_dl,
    urea_mg_dl,
    colesterol_total_mg_dl,
    colesterol_hdl_mg_dl,
    colesterol_ldl_mg_dl,
    sodio_mmol_l,
    potasio_mmol_l,
    hemoglobina_g_dl,
    hematocrito_porcentaje,

    -- Meds
    num_medicamentos_activos,
    dias_medios_uso_reportados,

    -- Utilización (cuando tengas fuente real, sustituyes los NULL del CTE)
    num_urgencias_12m

from joined_with_date;
