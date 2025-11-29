{% snapshot drugs_snapshot %}

{{
  config(
    target_database = 'HACKATON_SILVER',
    target_schema   = 'SNAPSHOTS',
    unique_key      = 'codigo_medicamento',
    strategy        = 'check',
    check_cols      = [
      'nombre_medicamento'
    ]
  )
}}

select
    codigo_medicamento,
    nombre_medicamento
from {{ ref('stg_drugs') }}

{% endsnapshot %}
