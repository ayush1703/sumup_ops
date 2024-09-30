{% test check_cols(model, column_name, column_name2 ) %}
select * 
from {{ model }}
where {{ column_name }} > {{ column_name2 }}
{% endtest %}
