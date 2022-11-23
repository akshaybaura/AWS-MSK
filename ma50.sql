CREATE TABLE public.honest_table_with_ma50 (
    amount_requested character varying(65535) ENCODE lzo,
    application_date character varying(65535) ENCODE lzo,
    loan_title character varying(65535) ENCODE lzo,
    risk_score integer ENCODE az64,
    debt_to_income_ratio character varying(65535) ENCODE lzo,
    zip_code character varying(65535) ENCODE lzo,
    state character varying(65535) ENCODE lzo,
    employment_length character varying(65535) ENCODE lzo,
    policy_code character varying(65535) ENCODE lzo,
    ma50 integer encode az64
)
DISTSTYLE EVEN;	

insert into honest_table_with_ma50 ( 
    amount_requested,
    application_date,
    loan_title,
    risk_score,
    debt_to_income_ratio,
    zip_code,
    state,
    employment_length,
    policy_code, 
    ma50
)
select 
    amount_requested,
    application_date,
    loan_title,
    risk_score,
    debt_to_income_ratio,
    zip_code,
    state,
    employment_length,
    policy_code,
    avg(risk_score) over (order by application_date::date rows between 50 preceding and current row) as ma50 
from honest_table;