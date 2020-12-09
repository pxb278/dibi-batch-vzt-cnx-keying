INSERT INTO dibi_batch.vzt_datashare(
	datashare_id,  bill_street_no, bill_street_name, bill_city, bill_state, bill_zip, svc_street_no, svc_street_name, svc_city, svc_state, svc_zip, first_name, middle_name, last_name, efx_cnx_modify_batch_id,keying_flag)
	VALUES (3456780,'123 st','Lk CIR','WA','FL','678905','3456 st','marylnd','NW','AZ','6709895','JOhn','L','HARRISSON',80,'T');
	
		update dibi_batch.vzt_datashare set efx_bill_conf_cd='M' where datashare_id='3456780'
			update dibi_batch.vzt_datashare set efx_cnx_modify_dt=current_date where datashare_id='3456780'
			update verizonds.vzt_datashare set efx_svc_conf_cd='M' where datashare_id='3456780'
			
			
			SELECT A.DATASHARE_ID AS SURROGATE_KEY ,A.FIRST_NAME,A.MIDDLE_NAME,A.LAST_NAME,  
			A.BILL_STREET_NO,A.BILL_STREET_NAME,A.BILL_CITY,A.BILL_STATE,A.BILL_ZIP,A.SVC_STREET_NO,A.SVC_STREET_NAME,  
			A.SVC_CITY,A.SVC_STATE,A.SVC_ZIP  
			FROM verizonds.VW_VZT_DATASHARE A WHERE  
			A.FIRST_NAME IS NOT NULL AND  
			A.LAST_NAME IS NOT NULL AND  
			A.EFX_CONF_CD IN ('M','L') AND   
			A.EFX_CNX_MODIFY_BATCH_ID=467 AND   
			A.KEYING_FLAG='T' ORDER BY SURROGATE_KEY;
			
			
			
INSERT INTO verizonds.vzt_datashare(datashare_id,  bill_street_no, bill_street_name, bill_city, bill_state, bill_zip, svc_street_no, svc_street_name, svc_city, svc_state, svc_zip, first_name, middle_name, last_name, efx_cnx_modify_batch_id,keying_flag)
VALUES (3456781,'123 st','Lk CIR','WA','FL','678905','3456 st','marylnd','NW','AZ','6709895','JOhn','L','HARRISSON',467,'T');
	
update verizonds.vzt_datashare set efx_svc_conf_cd='M' where datashare_id='3456781'
update verizonds.vzt_datashare set efx_cnx_modify_dt=current_date where datashare_id='3456781'

Insert into  verizonds.VZ_DATASHARE_CNX_REPO  (VZ_DATASHARE_CNX_REPO_ID,FIRST_NAME, LAST_NAME,EFX_MODIFY_BATCH_ID) 
values ('89798','Jhon', 'Brayan','367') 


Insert into verizonds.VW_VZT_DATASHARE_CNX_RESP (DATASHARE_ID,
EFX_CNX_MODIFY_BATCH_ID,KEYING_FLAG) values ('392195059',480,'T')

SELECT DATASHARE_ID, EFX_CNX_ID, EFX_HHLD_ID, EFX_ADDR_ID, EFX_ERROR_CODE, EFX_BEST_KEY_SOURCE, 
EFX_CONF_CD, EFX_CNX_MODIFY_BATCH_ID FROM verizonds.VW_VZT_DATASHARE_CNX_RESP
WHERE EFX_CNX_MODIFY_BATCH_ID = 467 AND KEYING_FLAG='T'

update verizonds.VW_VZT_DATASHARE_CNX_RESP set EFX_CNX_MODIFY_BATCH_ID=480 where DATASHARE_ID='392195059'

SELECT VZ_DATASHARE_CNX_REPO_ID,FIRST_NAME, LAST_NAME  FROM verizonds.VZ_DATASHARE_CNX_REPO 
WHERE EFX_MODIFY_BATCH_ID=367
update verizonds.VZ_DATASHARE_CNX_REPO set EFX_MODIFY_BATCH_ID=480 where VZ_DATASHARE_CNX_REPO_ID ='89798'
Insert into dibi_batch.connexuskeyoverride (datashareid,individualkey,overriderequestor,householdkey,status,datecreated,datemodified,connexuskeyoverrideid) values('392164223','123','abc','hhkey','actgivebeforeupdate',current_date,current_date,'567')

update verizonds.vzt_datashare set EFX_CNX_MODIFY_BATCH_ID=455 , EFX_OVERRIDE_STATUS='Active' where DATASHARE_ID='394801448';
	  
SELECT DATASHARE_ID, OLD_BEST_CNX_ID, EFX_CNX_ID, EFX_HHLD_ID, EFX_ADDR_ID, EFX_OVERRIDE_CNX_ID, 
	EFX_OVERRIDE_HHLD_ID FROM verizonds.VW_VZT_DATASHARE WHERE EFX_CNX_MODIFY_BATCH_ID = 455 AND EFX_OVERRIDE_STATUS = 'Active' AND COALESCE(EFX_CNX_ID, 'X') <> COALESCE(OLD_BEST_CNX_ID, 'Y');

SELECT VZ_DATASHARE_CNX_REPO_ID,FIRST_NAME, LAST_NAME FROM verizonds.VZ_DATASHARE_CNX_REPO WHERE EFX_MODIFY_BATCH_ID=467
insert into verizonds.VZ_DATASHARE_CNX_REPO (VZ_DATASHARE_CNX_REPO_ID,FIRST_NAME, LAST_NAME,EFX_MODIFY_BATCH_ID) 
values('12345678','Lowis','Brown',467)



update verizonds.VZ_DATASHARE_CNX_REPO set EFX_MODIFY_BATCH_ID=472 where VZ_DATASHARE_CNX_REPO_ID='12345678'
SELECT VZ_DATASHARE_CNX_REPO_ID,FIRST_NAME, LAST_NAME  FROM verizonds.VZ_DATASHARE_CNX_REPO WHERE EFX_MODIFY_BATCH_ID=468

SELECT DATASHARE_ID, FIRST_NAME, MIDDLE_NAME, LAST_NAME, ACCOUNT_NO, EFX_CNX_ID,
EFX_HHLD_ID, EFX_ADDR_ID,EFX_SOURCE_OF_MATCH,EFX_BEST_KEY_SOURCE,EFX_CNX_MODIFY_DT,EFX_CONF_CD 
FROM verizonds.VW_VZT_DATASHARE 
WHERE  EFX_ERROR_CODE IS NULL AND EFX_CNX_ID IS NOT NULL AND EFX_CNX_MODIFY_BATCH_ID = 468
 

select * from verizonds.VZ_DATASHARE_CNX_REPO
delete from verizonds.VZ_DATASHARE_CNX_REPO where efx_create_batch_id=468

CREATE OR REPLACE VIEW dibi_batch.vw_vzt_datashare_cnx_resp (datashare_id, line_of_business, btn, bill_street_no, bill_street_name, bill_city, bill_state, bill_zip, svc_street_no, svc_street_name, svc_city, svc_state, svc_zip, first_name, middle_name, last_name, business_name, ssn_taxid, account_no, live_final_indicator, source_business, account_refresh_date, fiber_indicator, behavior_score, dcr_code, final_bill_date, write_off_date, account_balance, past_due_amount, account_established, email_address, fraud_indicator, efx_create_dt, efx_create_batch_id, efx_modify_dt, efx_modify_batch_id, 
															 --efx_addr, efx_city, efx_state, efx_zip, 
															 efx_cnx_id, efx_hhld_id, efx_addr_id, efx_error_code, efx_conf_cd, efx_source_of_match, efx_best_key_source, efx_cnx_modify_dt, efx_cnx_modify_batch_id, delta_dt, delta_batch_id, efx_override_cnx_id, efx_override_hhld_id, efx_override_modify_dt, efx_override_modify_by, efx_override_status, efx_override_batch_id, old_best_cnx_id, keying_flag) AS SELECT  a.datashare_id,
           a.line_of_business,
           a.btn,
           a.bill_street_no,
           a.bill_street_name,
           a.bill_city,
           a.bill_state,
           a.bill_zip,
           a.svc_street_no,
           a.svc_street_name,
           a.svc_city,
           a.svc_state,
           a.svc_zip,
           a.first_name,
           a.middle_name,
           a.last_name,
           a.business_name,
           a.ssn_taxid,
           a.account_no,
           a.live_final_indicator,
           a.source_business,
           a.account_refresh_date,
           a.fiber_indicator,
           a.behavior_score,
           a.dcr_code,
           a.final_bill_date,
           a.write_off_date,
           a.account_balance,
           a.past_due_amount,
           a.account_established,
           a.email_address,
           a.fraud_indicator,
           a.efx_create_dt,
           a.efx_create_batch_id,
           a.efx_modify_dt,
           a.efx_modify_batch_id,
           --a.efx_addr,
           --a.efx_city,
           --a.efx_state,
          -- a.efx_zip,
           CASE WHEN UPPER(a.efx_override_status)='ACTIVE' THEN  a.efx_override_cnx_id  ELSE a.efx_cnx_id END efx_cnx_id,
           CASE WHEN UPPER(a.efx_override_status)='ACTIVE' THEN  a.efx_override_hhld_id  ELSE a.efx_hhld_id END  efx_hhld_id,
           a.efx_addr_id,
           CASE WHEN UPPER(a.efx_override_status)='ACTIVE' THEN  NULL  ELSE a.efx_error_code END efx_error_code,
           CASE WHEN UPPER(a.efx_override_status)='ACTIVE' THEN  NULL  ELSE a.efx_conf_cd END efx_conf_cd,
           CASE WHEN UPPER(a.efx_override_status)='ACTIVE' THEN  '0'  ELSE a.efx_source_of_match END efx_source_of_match,
           CASE WHEN UPPER(a.efx_override_status)='ACTIVE' THEN  'O'  ELSE a.efx_best_key_source END efx_best_key_source,
           a.efx_cnx_modify_dt,
           a.efx_cnx_modify_batch_id,
           a.delta_dt,
           a.delta_batch_id,
           a.efx_override_cnx_id,
           a.efx_override_hhld_id,
           a.efx_override_modify_dt,
           a.efx_override_modify_by,
           a.efx_override_status,
           a.efx_override_batch_id,
           a.old_best_cnx_id,
           a.KEYING_FLAG
       FROM dibi_batch.vw_vzt_datashare a;


-- View: dibi_batch.vw_vzt_datashare

-- DROP VIEW dibi_batch.vw_vzt_datashare;

CREATE OR REPLACE VIEW dibi_batch.vw_vzt_datashare
 AS
 SELECT a.datashare_id,
    a.line_of_business,
    a.btn,
    a.bill_street_no,
    a.bill_street_name,
    a.bill_city,
    a.bill_state,
    a.bill_zip,
    a.svc_street_no,
    a.svc_street_name,
    a.svc_city,
    a.svc_state,
    a.svc_zip,
    a.first_name,
    a.middle_name,
    a.last_name,
    a.business_name,
    a.ssn_taxid,
    a.account_no,
    a.live_final_indicator,
    a.source_business,
    a.account_refresh_date,
    a.fiber_indicator,
    a.behavior_score,
    a.dcr_code,
    a.final_bill_date,
    a.write_off_date,
    a.account_balance,
    a.past_due_amount,
    a.account_established,
    a.email_address,
    a.fraud_indicator,
    a.efx_create_dt,
    a.efx_create_batch_id,
    a.efx_modify_dt,
    a.efx_modify_batch_id,
        CASE
            WHEN a.efx_best_key_source = 'B'::bpchar THEN a.efx_bill_cnx_id
            ELSE a.efx_svc_cnx_id
        END AS efx_cnx_id,
        CASE
            WHEN a.efx_best_key_source = 'B'::bpchar THEN a.efx_bill_hhld_id
            ELSE a.efx_svc_hhld_id
        END AS efx_hhld_id,
        CASE
            WHEN a.efx_best_key_source = 'B'::bpchar THEN a.efx_bill_addr_id
            ELSE a.efx_svc_addr_id
        END AS efx_addr_id,
        CASE
            WHEN a.efx_best_key_source = 'B'::bpchar THEN a.efx_bill_cnx_error_code
            ELSE a.efx_svc_cnx_error_code
        END AS efx_error_code,
        CASE
            WHEN a.efx_best_key_source = 'B'::bpchar THEN a.efx_bill_conf_cd
            ELSE a.efx_svc_conf_cd
        END AS efx_conf_cd,
        CASE
            WHEN a.efx_best_key_source = 'B'::bpchar THEN
            CASE
                WHEN a.efx_bill_conf_cd::text = 'H'::text THEN '2'::text
                ELSE
                CASE
                    WHEN a.efx_bill_conf_cd::text = 'M'::text THEN '4'::text
                    ELSE
                    CASE
                        WHEN a.efx_bill_conf_cd::text = 'L'::text THEN '7'::text
                        ELSE NULL::text
                    END
                END
            END
            ELSE
            CASE
                WHEN a.efx_svc_conf_cd::text = 'H'::text THEN '2'::text
                ELSE
                CASE
                    WHEN a.efx_svc_conf_cd::text = 'M'::text THEN '4'::text
                    ELSE
                    CASE
                        WHEN a.efx_svc_conf_cd::text = 'L'::text THEN '7'::text
                        ELSE NULL::text
                    END
                END
            END
        END AS efx_source_of_match,
    a.efx_best_key_source,
    a.efx_cnx_modify_dt,
    a.efx_cnx_modify_batch_id,
    a.delta_dt,
    a.delta_batch_id,
    a.efx_override_cnx_id,
    a.efx_override_hhld_id,
    a.efx_override_modify_dt,
    a.efx_override_modify_by,
    a.efx_override_status,
    a.efx_override_batch_id,
    a.old_best_cnx_id,
    a.keying_flag
   FROM dibi_batch.vzt_datashare a
  WHERE a.efx_cnx_modify_dt IS NOT NULL OR a.line_of_business::text = 'C'::text;

ALTER TABLE dibi_batch.vw_vzt_datashare
    OWNER TO postgres;

-- SEQUENCE: dibi_batch.att_cnx_individual_delta_seq

-- DROP SEQUENCE dibi_batch.att_cnx_individual_delta_seq;

CREATE SEQUENCE dibi_batch.SEQ_VZ_DATASHARE_CNX_REPO
    INCREMENT 1
    START 100000000
    MINVALUE 1
    MAXVALUE 9223372036854775807
    CACHE 1;

ALTER SEQUENCE dibi_batch.att_cnx_individual_delta_seq
    OWNER TO postgres;