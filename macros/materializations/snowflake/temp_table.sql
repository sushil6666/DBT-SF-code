{% materialization temp_table, adapter='snowflake' %}

    {%- set relation = this.incorporate(type='table') -%}
    {%- set model_sql = sql -%}

    {{ run_query(
        create_table_as(
            relation  = relation,
            sql       = model_sql,
            table_type = 'temporary'
        )
    ) }}

    {{ return({'relations': [relation]}) }}

{% endmaterialization %}
