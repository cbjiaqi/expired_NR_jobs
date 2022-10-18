set spark.yarn.queue=${queue};
set spark.app.name=Check-Expired-Rec-Jobs-NR-vs-Hive;
set spark.dynamicAllocation.initialExecutors=10;
set spark.dynamicAllocation.maxExecutors=50;
set spark.executor.memory=4g;
set spark.yarn.executor.memoryOverhead=4096;
set spark.sql.shuffle.partitions=1000;
set hive.exec.dynamic.partition.mode=nonstrict;

drop table if exists ${db}.qqq_check_rec_jobs_new_relic_${table_end};
create table if not exists ${db}.qqq_check_rec_jobs_new_relic_${table_end} as
--select t1.srcjob_id as srcjob_id, t1.recjob_id as recjob_id, t1.send_time as send_time_nr, t2.prenddate
select t1.recjob_id as recjob_id, t1.send_time as send_time_nr, t2.prenddate
from ${db}.qqq_get_rec_jobs_new_relic_${table_end} t1
join
sitedata.hhjob t2
on t1.recjob_id=t2.did
where t2.prenddate<t1.send_time;