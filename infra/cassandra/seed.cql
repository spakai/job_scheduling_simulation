INSERT INTO worker_demo.datasets
  (dataset_id, dataset_version, record_count, bucket_count, seed, created_at)
VALUES ('local-v1', 1, 16, 4, 20260820, '2026-08-20T00:00:00Z');

INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',0,0,100,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',0,4,300,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',0,8,500,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',0,12,700,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',1,1,200,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',1,5,400,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',1,9,600,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',1,13,800,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',2,2,150,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',2,6,350,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',2,10,550,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',2,14,750,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',3,3,250,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',3,7,450,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',3,11,650,0);
INSERT INTO worker_demo.records_by_bucket (dataset_id,bucket,record_id,input_number,checksum)
VALUES ('local-v1',3,15,1000,0);
