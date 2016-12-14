set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapred.max.split.size=1024000000;
set mapred.min.split.size.per.node=1024000000;
set mapred.min.split.size.per.rack=1024000000;
set mapreduce.jobtracker.split.metainfo.maxsize = -1;
set mapreduce.job.queuename=root.queue3;


//欺诈样本
CREATE TABLE IF NOT EXISTS lxr_tx_1601(
card string,
trans_at double, 
trans_dt string,
trans_tm string
);


INSERT OVERWRITE TABLE lxr_tx_1601
select ar_pri_acct_no, trans_at, trans_dt, trans_tm
from tbl_arsvc_fraud_trans where fraud_tp = '62' and trans_dt>="20160101" and trans_dt<="20160131";


//欺诈样本正常样本
CREATE TABLE IF NOT EXISTS lxr_spend_1601(
card string,
trans_at double, 
pdate string,
loc_trans_tm string
);
 
INSERT OVERWRITE TABLE lxr_spend_1601
select pri_acct_no_conv, trans_at, pdate, loc_trans_tm
from tbl_common_his_trans 
where trans_id='S22' and pdate>='20160101' and pdate<='20160131';
