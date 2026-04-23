{% materialization iceberg_table, adapter='snowflake' %}

    {%- set meta            = config.get('meta', {}) -%}
    {%- set external_volume = meta.get('external_volume') -%}
    {%- set base_location   = meta.get('base_location') -%}
    {%- set catalog         = meta.get('catalog', 'SNOWFLAKE') -%}

    {%- if not external_volume -%}
        {{ exceptions.raise_compiler_error(
            "iceberg_table materialization requires `external_volume` inside meta config. "
            ~ "Example: {{ config(materialized='iceberg_table', meta={'external_volume': 'my_vol', 'base_location': 'path/'}) }}"
        ) }}
    {%- endif -%}

    {%- if not base_location -%}
        {{ exceptions.raise_compiler_error(
            "iceberg_table materialization requires `base_location` inside meta config. "
            ~ "Example: {{ config(materialized='iceberg_table', meta={'external_volume': 'my_vol', 'base_location': 'path/'}) }}"
        ) }}
    {%- endif -%}

    {%- set relation  = this.incorporate(type='table') -%}
    {%- set model_sql = sql -%}

    {%- set iceberg_props -%}
        EXTERNAL_VOLUME = '{{ external_volume }}'
        CATALOG = '{{ catalog }}'
        BASE_LOCATION = '{{ base_location }}'
    {%- endset -%}

    {%- set iceberg_ddl -%}
        CREATE OR REPLACE ICEBERG TABLE {{ relation }}
            {{ iceberg_props }}
        AS (
            {{ model_sql }}
        )
    {%- endset -%}

    {{ run_query(iceberg_ddl) }}

    {{ return({'relations': [relation]}) }}

{% endmaterialization %}
