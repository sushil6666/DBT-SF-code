{% materialization hybrid_table, adapter='snowflake' %}

    {%- set primary_key = config.get('primary_key') -%}

    {%- if not primary_key -%}
        {{ exceptions.raise_compiler_error(
            "hybrid_table materialization requires `primary_key` in config. "
            ~ "Example: {{ config(materialized='hybrid_table', primary_key='id') }}"
        ) }}
    {%- endif -%}

    {%- if primary_key is string -%}
        {%- set pk_cols = primary_key -%}
    {%- else -%}
        {%- set pk_cols = primary_key | join(', ') -%}
    {%- endif -%}

    {%- set relation  = this.incorporate(type='table') -%}
    {%- set model_sql = sql -%}

    {%- set staging = make_temp_relation(this) -%}

<<<<<<< HEAD
    {%- call statement('create_staging') -%}
        {{ create_table_as(
=======
    {{ run_query(
        sf_create_table_as(
>>>>>>> 7a0c6dc07c1dc19917e3cb830bbcba85bd421400
            relation   = staging,
            sql        = model_sql,
            table_type = 'temporary'
        ) }}
    {%- endcall -%}

    {%- set columns = adapter.get_columns_in_relation(staging) -%}
    {%- set col_defs = [] -%}
    {%- for col in columns -%}
        {%- do col_defs.append(col.quoted ~ ' ' ~ col.dtype) -%}
    {%- endfor -%}

    {%- call statement('create_hybrid') -%}
        CREATE OR REPLACE HYBRID TABLE {{ relation }} (
            {{ col_defs | join(',\n            ') }},
            PRIMARY KEY ({{ pk_cols }})
        )
    {%- endcall -%}

    {%- call statement('main') -%}
        INSERT INTO {{ relation }} SELECT * FROM {{ staging }}
    {%- endcall -%}

    {{ return({'relations': [relation]}) }}

{% endmaterialization %}