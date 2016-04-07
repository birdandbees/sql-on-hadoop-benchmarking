select payment_origin, count(*) from loans.loan_task_facts_parquet group by payment_origin;
