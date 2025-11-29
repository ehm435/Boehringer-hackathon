-- models/marts/mart_patient_history_with_risk.sql

{{ config(
    materialized = 'view'
) }}

select
    f.fact_patient_clinical_sk,
    f.date_sk,
    d.full_date                    as fecha_referencia,

    f.patient_sk,
    p.patient_id_natural,

    -- Datos de paciente
    p.edad_anios,
    p.sexo,
    p.hta_diagnosticada_codigo,
    p.diabetes_diagnosticada_codigo,
    p.cardiopatia_diagnosticada_codigo,
    p.epoc_u_otro_pulmonar_cronico_codigo,
    p.es_paciente_oncologico,
    p.tabaquismo_actual_codigo,
    p.fecha_ultima_visita,

    -- Métricas clínicas
    f.tension_sistolica_media,
    f.tension_diastolica_media,
    f.imc,
    f.glucosa_plasmatica_mg_dl,
    f.hba1c_porcentaje,
    f.creatinina_mg_dl,
    f.urea_mg_dl,
    f.colesterol_total_mg_dl,
    f.colesterol_hdl_mg_dl,
    f.colesterol_ldl_mg_dl,
    f.num_medicamentos_activos,

    -- Uso del sistema (si los completas después)
    f.num_urgencias_12m,
    f.num_no_show_12m,
    f.num_medicos_distintos_12m,

    -- Score y semáforo ya calculados en GOLD
    f.indice_riesgo,
    f.prioridad_semaforo

from {{ ref('fact_patient_clinical') }} f
join {{ ref('dim_patient') }} p
  on f.patient_sk = p.patient_sk
join {{ ref('dim_date') }} d
  on f.date_sk = d.date_sk
