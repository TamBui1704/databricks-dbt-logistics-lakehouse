{% macro drop_elementary_tables() %}
    {% set tables = ['dbt_metrics', 'dbt_models', 'dbt_seeds', 'dbt_snapshots', 'dbt_tests'] %}
    {% for tbl in tables %}
        {% set query = "DROP TABLE IF EXISTS workspace.elementary." ~ tbl %}
        {% do run_query(query) %}
        {{ log("Dropped table " ~ tbl, info=True) }}
    {% endfor %}
{% endmacro %}
