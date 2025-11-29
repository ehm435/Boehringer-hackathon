{{ config(
    materialized = 'table'
) }}

-- 1) Dimensión paciente (demografía + diagnósticos + oncología + última visita)
with dim_p as (

    select
        patient_sk,
        patient_id_natural,
        edad_anios,
        hta_diagnosticada_codigo,
        diabetes_diagnosticada_codigo,
        cardiopatia_diagnosticada_codigo,
        epoc_u_otro_pulmonar_cronico_codigo,
        es_paciente_oncologico,
        tabaquismo_actual_codigo,
        fecha_ultima_visita
    from {{ ref('dim_patient') }}

),

-- 2) Vitals (TA, IMC) desde la silver
vitals as (

    select
        patient_id,
        tension_sistolica_1_mm_hg  as tension_sistolica_media,
        tension_diastolica_1_mm_hg as tension_diastolica_media,
        imc
    from {{ ref('stg_patient_vitals') }}

),

-- 3) Labs desde la silver
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

-- 4) Medicación agregada por paciente
meds as (

    select
        patient_id,
        count(*)                          as num_medicamentos_activos,
        avg(dias_uso_reportados)         as dias_medios_uso_reportados
    from {{ ref('stg_patient_medications') }}
    group by patient_id

),

-- 5) Join clínico por paciente (todavía sin date_sk)
joined as (

    select
        p.patient_sk,
        p.patient_id_natural,

        -- Fecha de referencia: última visita; si no hay, hoy
        coalesce(
            cast(p.fecha_ultima_visita as date),
            current_date()
        ) as fecha_referencia,

        -- Demografía / clínica de dim_patient
        p.edad_anios,
        p.hta_diagnosticada_codigo,
        p.diabetes_diagnosticada_codigo,
        p.cardiopatia_diagnosticada_codigo,
        p.epoc_u_otro_pulmonar_cronico_codigo,
        p.es_paciente_oncologico,
        p.tabaquismo_actual_codigo,
        p.fecha_ultima_visita,

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

        -- Meds
        coalesce(m.num_medicamentos_activos, 0)  as num_medicamentos_activos,
        m.dias_medios_uso_reportados,

        -- Uso del sistema (placeholder por ahora)
        cast(null as number(38,0)) as num_urgencias_12m,
        cast(null as number(38,0)) as num_no_show_12m,
        cast(null as number(38,0)) as num_medicos_distintos_12m

    from dim_p p
    left join vitals v on p.patient_id_natural = v.patient_id
    left join labs   l on p.patient_id_natural = l.patient_id
    left join meds   m on p.patient_id_natural = m.patient_id

),

-- 6) Join con dim_date para sacar date_sk
joined_with_date as (

    select
        j.*,
        d.date_sk
    from joined j
    left join {{ ref('dim_date') }} d
        on d.full_date = j.fecha_referencia

),

-- 7) Cálculo de puntos por condición (ponderaciones que definiste)
scoring as (

    select
        jwd.*,

        -- 🟥 ROJO
        case
            when tension_sistolica_media >= 140
              or tension_diastolica_media >= 90
            then 6 else 0
        end as puntos_hta_no_controlada,

        case
            when hta_diagnosticada_codigo = 1
            then 5 else 0
        end as puntos_hta_diagnosticada,

        case
            when diabetes_diagnosticada_codigo = 1
            then 6 else 0
        end as puntos_diabetes_diagnosticada,

        case
            when cardiopatia_diagnosticada_codigo = 1
            then 6 else 0
        end as puntos_cardiopatia_diagnosticada,

        0 as puntos_asma_diagnosticada, -- no tienes campo asma aún

        case
            when epoc_u_otro_pulmonar_cronico_codigo = 1
            then 6 else 0
        end as puntos_epoc_diagnosticada,

        case
            when es_paciente_oncologico
            then 6 else 0
        end as puntos_oncologico,

        case
            when edad_anios >= 75
            then 4 else 0
        end as puntos_edad_75_o_mas,

        case
            when fecha_ultima_visita is not null
             and datediff('month', cast(fecha_ultima_visita as date), current_date()) > 12
            then 4 else 0
        end as puntos_visita_mas_12m,

        -- 🟧 NARANJA
        case
            when coalesce(tabaquismo_actual_codigo, 0) > 0
            then 3 else 0
        end as puntos_fumador_activo,

        case
            when edad_anios between 60 and 74
            then 2 else 0
        end as puntos_edad_60_74,

        case
            when fecha_ultima_visita is not null
             and datediff('month', cast(fecha_ultima_visita as date), current_date())
                 between 6 and 12
            then 2 else 0
        end as puntos_visita_6_12m,

        case
            when imc between 35 and 40
            then 2 else 0
        end as puntos_imc_35_40,

        -- 🟨 AMARILLO
        case
            when imc >= 30 and imc < 35
            then 1 else 0
        end as puntos_imc_30_35,

        case
            when num_medicamentos_activos >= 5
            then 1 else 0
        end as puntos_polimedicacion,

        case
            when colesterol_ldl_mg_dl between 130 and 159
            then 1 else 0
        end as puntos_ldl_130_159,

        case
            when hba1c_porcentaje between 6.0 and 6.4
            then 1 else 0
        end as puntos_hba1c_6_6_4,

        case
            when colesterol_total_mg_dl between 200 and 240
            then 1 else 0
        end as puntos_col_total_200_240

    from joined_with_date jwd
),

-- 8) Suma de puntos → índice de riesgo
scoring_sum as (

    select
        scoring.*,

        (
            puntos_hta_no_controlada +
            puntos_hta_diagnosticada +
            puntos_diabetes_diagnosticada +
            puntos_cardiopatia_diagnosticada +
            puntos_asma_diagnosticada +
            puntos_epoc_diagnosticada +
            puntos_oncologico +
            puntos_edad_75_o_mas +
            puntos_visita_mas_12m +
            puntos_fumador_activo +
            puntos_edad_60_74 +
            puntos_visita_6_12m +
            puntos_imc_35_40 +
            puntos_imc_30_35 +
            puntos_polimedicacion +
            puntos_ldl_130_159 +
            puntos_hba1c_6_6_4 +
            puntos_col_total_200_240
        ) as indice_riesgo

    from scoring
),

-- 9) Máximo de puntos entre todos los factores
with_max as (

    select
        scoring_sum.*,

        greatest(
            puntos_hta_no_controlada,
            puntos_hta_diagnosticada,
            puntos_diabetes_diagnosticada,
            puntos_cardiopatia_diagnosticada,
            puntos_asma_diagnosticada,
            puntos_epoc_diagnosticada,
            puntos_oncologico,
            puntos_edad_75_o_mas,
            puntos_visita_mas_12m,
            puntos_fumador_activo,
            puntos_edad_60_74,
            puntos_visita_6_12m,
            puntos_imc_35_40,
            puntos_imc_30_35,
            puntos_polimedicacion,
            puntos_ldl_130_159,
            puntos_hba1c_6_6_4,
            puntos_col_total_200_240
        ) as max_puntos_factor

    from scoring_sum
),

-- 10) Semáforo + condición principal de riesgo (TODO aquí)
scoring_final as (

    select
        w.*,

        case
            when indice_riesgo >= 10 then 'ROJO'
            when indice_riesgo between 6 and 9 then 'NARANJA'
            when indice_riesgo between 3 and 5 then 'AMARILLO'
            when indice_riesgo between 1 and 2 then 'VERDE'
            else 'AZUL'
        end as prioridad_semaforo,

        case
            when max_puntos_factor = 0
                then 'Sin factores de riesgo relevantes'

            when puntos_oncologico = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Paciente oncológico'

            when puntos_cardiopatia_diagnosticada = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Cardiopatía diagnosticada'

            when puntos_diabetes_diagnosticada = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Diabetes diagnosticada'

            when puntos_hta_no_controlada = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Hipertensión no controlada'

            when puntos_epoc_diagnosticada = max_puntos_factor
                 and max_puntos_factor > 0
                then 'EPOC / enfermedad pulmonar crónica'

            when puntos_visita_mas_12m = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Sin revisión en > 12 meses'

            when puntos_fumador_activo = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Fumador activo'

            when puntos_imc_35_40 = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Obesidad severa (IMC 35–40)'

            when puntos_edad_75_o_mas = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Edad avanzada (≥ 75 años)'

            when puntos_polimedicacion = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Polimedicación'

            when puntos_hba1c_6_6_4 = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Prediabetes (HbA1c 6.0–6.4 %)'

            when puntos_ldl_130_159 = max_puntos_factor
                 and max_puntos_factor > 0
                then 'LDL elevado (130–159 mg/dL)'

            when puntos_col_total_200_240 = max_puntos_factor
                 and max_puntos_factor > 0
                then 'Colesterol total alto (200–240 mg/dL)'

            else 'Otro factor de riesgo'
        end as condicion_principal_riesgo

    from with_max w
)

-- 11) SELECT final: tabla de hechos GOLD
select
    {{ dbt_utils.generate_surrogate_key(['patient_sk', 'date_sk']) }} as fact_patient_clinical_sk,

    date_sk,
    patient_sk,

    -- Métricas clínicas base
    tension_sistolica_media,
    tension_diastolica_media,
    imc,
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
    num_medicamentos_activos,
    dias_medios_uso_reportados,
    num_urgencias_12m,
    num_no_show_12m,
    num_medicos_distintos_12m,

    -- Puntos por factor
    puntos_hta_no_controlada,
    puntos_hta_diagnosticada,
    puntos_diabetes_diagnosticada,
    puntos_cardiopatia_diagnosticada,
    puntos_asma_diagnosticada,
    puntos_epoc_diagnosticada,
    puntos_oncologico,
    puntos_edad_75_o_mas,
    puntos_visita_mas_12m,
    puntos_fumador_activo,
    puntos_edad_60_74,
    puntos_visita_6_12m,
    puntos_imc_35_40,
    puntos_imc_30_35,
    puntos_polimedicacion,
    puntos_ldl_130_159,
    puntos_hba1c_6_6_4,
    puntos_col_total_200_240,

    -- Score global + semáforo + motivo principal
    indice_riesgo,
    prioridad_semaforo,
    condicion_principal_riesgo

from scoring_final
