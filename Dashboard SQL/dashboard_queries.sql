SELECT COUNT(*) AS total_lctr
FROM aml_project.gold_lctr;

SELECT report_type1, COUNT(*) AS cnt
FROM aml_project.gold_lctr
GROUP BY report_type1;

with exp as(SELECT *,
  EXPLODE(transaction_id) AS transaction_id2
FROM
  workspace.aml_project.gold_lctr)

  select transaction_id2 as Transaction_ids,count(report_type1) as Total_count ,collect_set(report_type1) as reports from exp
  group by transaction_id2
