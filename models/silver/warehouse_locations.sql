{{ config(
    materialized='incremental',
    unique_key='warehouse_name',
	incremental_strategy='delete+insert',
    tags=['silver']     
) }}

{% set src = source('warehouse_loc', 'warehouse')%}

SELECT
    CITY_NAME,
    STATE_NAME,
    REGION,
    WAREHOUSE_NAME,
    
    {% if is_incremental() %}
        ingestion_ts
    {% else %}
        CURRENT_TIMESTAMP AS ingestion_ts
    {% endif %}

    from {{src.database}}.{{target.schema}}_{{src.schema}}.{{src.identifier}}

    -- dev__raw