{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
{% if min_value is not none %} and {{ column_name }} < {{ min_value }} {% endif %}
{% if max_value is not none %} or ({{ column_name }} is not null and {{ column_name }} > {{ max_value }}) {% endif %}
{% endtest %}
