--one time creation
CREATE TABLE public.honest_table (
    amount_requested character varying(65535),
    application_date character varying(65535),
    loan_title character varying(65535),
    risk_score double precision,
    debt_to_income_ratio character varying(65535),
    zip_code character varying(65535),
    state character varying(65535),
    employment_length character varying(65535),
    policy_code character varying(65535),
    riskscorema50 double precision,
    _modified_ts bigint
)
DISTSTYLE EVEN;		

--run before glue job run (e.g. with a lambda function)
truncate table honest_table_staging;

--run after glue job finishes 
insert into honest_table ( 
    amount_requested,
    application_date,
    loan_title,
    risk_score,
    debt_to_income_ratio,
    zip_code,
    state,
    employment_length,
    policy_code, 
    riskscorema50,
    _modified_ts
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
    avg(risk_score) over (order by application_date::date rows between 50 preceding and current row) as riskscorema50,
    extract(epoch from sysdate) as  _modified_ts
from honest_table_staging;