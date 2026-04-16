CREATE OR REPLACE PACKAGE ETL_OWNER.ETL_ATTR_COLLECTION is
/**
* Type:          ETL<br/>
* Layer:       PM_OWNER</br>
* Package description:</br>
*	Client list for modelling purpose. It contains clients with products in Profile, IBIS, T24, MUREX, TERMINAL and clients with disponent card to retail DDA
*</br>
* @headcom
*    20240612: frschindler@csob.sk - added procedures for attr_interaction and attr_afp
*    20240717: frschindler@csob.sk - attr_afp changed income type to use CD_REC_VALUE
*	 20240724: mbusik@csob.sk - fix lvl3_category for credit card accounts
*							  - change filter of technical transactions - missing source system cashdesk (642)
*    20240814: frschindler@csob.sk - attr_ins changed travel_ins column
*	 20240814: mbusik@csob.sk - attr_elb transactions fix - add LOCO M24
*							  - attr_elb remove duplicities
*    20250114: mbusik@csob.sk - attr_afp changes in produt mapping
*    20250204: mbusik@csob.sk - fix wrong number in PM_OWNER.CALL_CENTRUM_HISTORY
*                             - more logs were added
*    20250205: mbusik@csob.sk - hint for brnach visists in crm sessions was added
*                             - distinct was added to pm_owner.terminal table
*                             - filter in L0_OWNER.CONT_INTR (bank_account_detail) on meas_cd was added
*                             - group by was applied on txn_type_list
*    20250528: dhudo@csob.sk  - zmena schemy z L0_OWNER na VEMA_OWNER pre tabulku: VEMA_EMPL_BANK
*/

  cVERSION constant varchar2(16) default 'v1.00';
  cSTEP_NAME constant varchar2(50) default 'ETL_ATTR_COLLECTION';
  cDEBUG constant boolean := false;
  j number := 0;  
  /**
  * @param p_audit_id - run id.
  * @param process_dt - date of run
  * @param p_repair - repair run
  */
  procedure on_process(p_audit_id in number, p_audit_date in date, p_repair in boolean);
  
  procedure main(p_audit_id in number, p_audit_date in date, p_repair in boolean);  

  function version return varchar2;
end ETL_ATTR_COLLECTION;
/

CREATE OR REPLACE PACKAGE BODY ETL_OWNER.ETL_ATTR_COLLECTION IS
  g_stat awf_xmap.stats;
  g_step integer default 0; 
  g_MONTH_ID    integer         default null;             -- month of execution YYYYMM
  g_PART_PREFIX varchar2(10)    default 'P_';             -- partition prefix
  g_PART_NAME   varchar2(30)    default null;             -- name of partition (P_YYYYMM)
  process_dt_hist date default DATE '2000-01-01';
  process_dt_prev_month date default DATE '2000-01-01';


  procedure init_stats is begin g_stat.sel := 0; g_stat.ins := 0; g_stat.upd := 0; g_stat.del := 0; g_stat.merg := 0; end;
  procedure stat_sel(p_count in integer) is begin g_stat.sel := p_count; end;
  procedure stat_ins(p_count in integer) is begin g_stat.ins := p_count; end;
  procedure stat_upd(p_count in integer) is begin g_stat.upd := p_count; end;
  procedure stat_del(p_count in integer) is begin g_stat.del := p_count; end;
  procedure stat_merg(p_count in integer) is begin g_stat.merg := p_count; end;
  procedure init_all is begin g_step:=0; init_stats; end;
  
  procedure init_global_par(process_dt IN DATE) is
    -- max hist date
  
  begin
    
	process_dt_hist  := ADD_MONTHS(process_dt, -60);
    process_dt_prev_month  := ADD_MONTHS(process_dt, -1);
    g_MONTH_ID   := TO_NUMBER(TO_CHAR(process_dt,'YYYYMM'));
    g_PART_NAME  := g_PART_PREFIX || g_MONTH_ID;
      
  end;  
  
  procedure step(p_stepname in varchar2, p_source in varchar2, p_target in varchar2) is
    l_stepname varchar2(255);
  begin
  j := j + 1;
  select substr(nvl(p_stepname,' '),1,65) into l_stepname from dual;
  l_stepname := j || ' ' || l_stepname;
    if (g_step > 0) then
      awf_xmap.step_progress(p_stats => g_stat, p_debug => cDEBUG);
      null;
    end if;
    init_stats;
    g_step := g_step + 1;
    awf_xmap.step_begin(p_stepnum => g_step, p_stepname => l_stepname, p_source => p_source, p_target => p_target, p_debug => cDEBUG);
  end; 
  
  
  /****************************************************************
**  MANAZMENT PARTICII
**  Dynamicke pridavanie particii pre pripad. ze pre dany dataset neexistuju
**  Truncate particie pre pripad, ze existuje
**  Format particie je s prefixom P_ pokracujuc mesiacom YYYYMM - P_YYYYMM
****************************************************************/  
  PROCEDURE mng_partitions(p_owner IN VARCHAR2, p_table_name IN VARCHAR2, p_proc_name IN VARCHAR2, process_dt IN DATE) IS
    l_check integer;
    my_sql varchar2(2000);
  BEGIN
    
    SELECT COUNT(1) INTO l_check FROM ALL_TAB_PARTITIONS 
    WHERE 
        TABLE_OWNER = p_owner 
        AND TABLE_NAME = p_table_name 
        AND PARTITION_NAME = g_PART_PREFIX || TO_NUMBER(TO_CHAR(process_dt,'YYYYMM'))
    ;

    IF l_check = 0 THEN
    
        step(p_stepname => p_proc_name || ' CREATE PART ' || g_PART_PREFIX || TO_NUMBER(TO_CHAR(process_dt,'YYYYMM')) || ' FOR ' || p_table_name, p_source => 'N/A', p_target => 'N/A');
    
        --EXECUTE IMMEDIATE      
        my_sql :=
       'ALTER TABLE '       || p_owner      || '.'      || p_table_name || 
        ' ADD PARTITION '   || g_PART_PREFIX || TO_NUMBER(TO_CHAR(process_dt,'YYYYMM'))  ||
        ' VALUES ('         || TO_NUMBER(TO_CHAR(process_dt,'YYYYMM'))   ||
                 ')'       ||
        --' TABLESPACE LACR_DATA';
        ' TABLESPACE PM_OWNER_DATA';
        --COMMIT;
        
        dbms_output.put_line(my_sql);
        execute immediate my_sql;
    
    ELSE    
    
        step(p_stepname => p_proc_name || ' TRUNC PART ' || g_PART_PREFIX || TO_NUMBER(TO_CHAR(process_dt,'YYYYMM')) || ' FOR ' || p_table_name, p_source => 'N/A', p_target => 'N/A');
        
        ETL_OWNER.etl_dbms_util.truncate_partition(p_table_name, p_owner, g_PART_PREFIX || TO_NUMBER(TO_CHAR(process_dt,'YYYYMM')));    
        
    END IF;
    
  END;

/****************************************************************
**  Management STATISTIK
**  Dynamicke urcovanie ci bude napocitavat statistiky:
**      - globalne (nema particie)
**      - na particii (pokial je tabulka particiovana)
****************************************************************/  
  PROCEDURE mng_stats(p_owner IN VARCHAR2, p_table_name IN VARCHAR2, p_proc_name IN VARCHAR2) IS
    l_check integer;
    
  BEGIN
    
    SELECT COUNT(1) INTO l_check FROM ALL_TAB_PARTITIONS 
    WHERE 
        TABLE_OWNER = p_owner 
        AND TABLE_NAME = p_table_name 
        AND PARTITION_NAME = g_PART_NAME
    ;
    
    IF l_check = 0 THEN

        step(p_stepname => p_proc_name || ' CALC STAT ON ' || p_table_name, p_source => 'N/A', p_target => 'N/A');
      
      DBMS_STATS.gather_table_stats(
                                ownname => p_owner, 
                                tabname => p_table_name, 
                                cascade => TRUE, 
                                method_opt=>'FOR ALL COLUMNS SIZE SKEWONLY'
                                );    
    ELSE

        step(p_stepname => p_proc_name || ' CALC STAT ON ' || p_table_name || ' FOR ' || g_PART_NAME, p_source => 'N/A', p_target => 'N/A');
      
      DBMS_STATS.gather_table_stats(
                                ownname => p_owner, 
                                tabname => p_table_name, 
                                partname => g_PART_NAME,
                                granularity => 'PARTITION',
                                cascade => TRUE, 
                                method_opt=>'FOR ALL COLUMNS SIZE SKEWONLY'
                                );
    END IF;
    
  END;

  PROCEDURE append_ATTR_CLIENT_BASE(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_ATTR_CLIENT_BASE';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_CLIENT_BASE';
    
  BEGIN
	
	-- create CC_CODE segement pairs
    step(p_stepname => l_proc_name || ' TRUNC', p_source => 'N/A', p_target => 'STG_CC_CODE_SEG');
	ETL_OWNER.etl_dbms_util.truncate_table('STG_CC_CODE_SEG',l_owner_name);

    step(p_stepname => l_proc_name || ' INSERT STG_CC_CODE_SEG', p_source => 'N/A', p_target => 'STG_CC_CODE_SEG');	
	INSERT INTO PM_OWNER.STG_CC_CODE_SEG 
		select
			cc CC_CODE,
			CASE WHEN sgm = 'Micro_SME' THEN 'Micro SME'
                 WHEN sgm = 'Corp' THEN 'CORP' 
                 ELSE sgm END segment
		from uni_owner.pobocky
		UNION ALL
		-- IBIS CC
		select '55-00', 'FI' from dual
		UNION ALL
		select '25-04', 'SME' from dual
		UNION ALL
		select '25-01', 'CORP' from dual
		UNION ALL
		select '25-02', 'CORP' from dual
		UNION ALL
		select '25-03', 'CORP' from dual
		UNION ALL
		select '25-05', 'CORP' from dual
		UNION ALL
		select '25-07', 'Micro SME' from dual
		UNION ALL
		select '0', 'RET' from dual
		UNION ALL
		select '1', 'Micro SME' from dual
		UNION ALL
		select '2', 'Micro SME' from dual
		UNION ALL
		select '3', 'CORP' from dual;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    step(p_stepname => l_proc_name || ' TRUNC', p_source => 'N/A', p_target => 'PRODUCT_LEVEL_CATEGORY');
	ETL_OWNER.etl_dbms_util.truncate_table('PRODUCT_LEVEL_CATEGORY',l_owner_name);

    step(p_stepname => l_proc_name || ' INSERT PRODUCT_LEVEL_CATEGORY', p_source => 'N/A', p_target => 'PRODUCT_LEVEL_CATEGORY');	
    INSERT INTO PM_OWNER.PRODUCT_LEVEL_CATEGORY
        SELECT
            cst255.code prod_src_val,
            cst255.c255_desc_cs prod_dsc,
            cst255.src_syst_cd,
            cst255.src_syst_dsc,
            lvl5.c2175_dataid lvl5_category,
            lvl5.c2175_bus_desc_sk lvl5_category_dsc,
            lvl4.c2175_dataid lvl4_category,
            lvl4.c2175_bus_desc_sk lvl4_category_dsc,
            lvl3.c2175_dataid lvl3_category,
            lvl3.c2175_bus_desc_sk lvl3_category_dsc,
            lvl2.c2175_dataid lvl2_category,
            lvl2.c2175_bus_desc_sk lvl2_category_dsc,
            lvl1.c2175_dataid lvl1_category,
            lvl1.c2175_bus_desc_sk lvl1_category_dsc    
        FROM (
            SELECT * FROM (
                SELECT
                    c255_code code,
                    c255_desc_cs,
                    c255_dataid dataid,
                    c255_systemid source_id,
                    c255_validfrom,
                    c255_validto,
                    case when c255_systemid in (41, 522) then 41 else 42 end system_id,
                    CASE WHEN c255_systemid = 41 Then 510
                         WHEN c255_systemid = 522 Then 501
                         WHEN c255_systemid = 42 THEN 520
                         WHEN c255_systemid = 145 THEN 790
                         WHEN c255_systemid = 141 THEN 999
                         WHEN c255_systemid = 50 THEN 820
                         ELSE 0 END src_syst_cd,
                    CASE WHEN c255_systemid = 41 Then 'PROFILE'
                         WHEN c255_systemid = 522 Then 'T24'
                         WHEN c255_systemid = 42 THEN 'IBIS'
                         WHEN c255_systemid = 145 THEN 'INSURANCE'
                         WHEN c255_systemid = 141 THEN 'TERMINAL'
                         WHEN c255_systemid = 50 THEN 'LEASING'
                         ELSE 'UNKNOWN' END src_syst_dsc,
                    ROW_NUMBER() OVER(PARTITION BY c255_dataid ORDER BY c255_validfrom DESC) row_num
                FROM meta_owner.meta_cst_0255
                WHERE 1=1
                    AND c255_dataid is not null 
                    AND c255_brandid = 2
                    AND c255_systemid in (41, 42, 50, 522, 145, 141))
            WHERE row_num = 1) cst255
                
        LEFT JOIN (
            SELECT 
                c2175_s3katprodid,
                c2175_crmprdhiskid,
                c2175_bus_desc_sk,
                c2175_validfrom,
                c2175_validto,
                c2175_level,
                c2175_dataid,
                ROW_NUMBER() OVER(partition by c2175_s3katprodid ORDER BY c2175_validfrom DESC) row_num
            FROM meta_owner.meta_cst_2175
                WHERE c2175_level = 5) lvl5
            ON lvl5.c2175_s3katprodid = cst255.dataid
            AND lvl5.row_num = 1
            --AND (CASE WHEN lvl5.c2175_s3katprodid = 2163 OR lvl5.c2175_validfrom <= DATE '2022-12-31' THEN 1 ELSE 0 END) = 1
          
        LEFT JOIN (
            SELECT 
                c2175_s3katprodid,
                c2175_crmprdhiskid,
                c2175_bus_desc_sk,
                c2175_validfrom,
                c2175_validto,
                c2175_level,
                c2175_dataid,
                ROW_NUMBER() OVER(partition by c2175_dataid ORDER BY c2175_validfrom DESC) row_num
            FROM meta_owner.meta_cst_2175
                WHERE c2175_level = 4) lvl4
            ON lvl4.c2175_dataid = lvl5.c2175_crmprdhiskid
            AND lvl4.row_num = 1
        
        LEFT JOIN (
            SELECT 
                c2175_s3katprodid,
                c2175_crmprdhiskid,
                c2175_bus_desc_sk,
                c2175_validfrom,
                c2175_validto,
                c2175_level,
                c2175_dataid,
                ROW_NUMBER() OVER(partition by c2175_dataid ORDER BY c2175_validfrom DESC) row_num
            FROM meta_owner.meta_cst_2175
                WHERE c2175_level = 3) lvl3
            ON lvl3.c2175_dataid = COALESCE(lvl4.c2175_crmprdhiskid, lvl5.c2175_crmprdhiskid)
            AND lvl3.row_num = 1
            
        LEFT JOIN (
            SELECT 
                c2175_s3katprodid,
                c2175_crmprdhiskid,
                c2175_bus_desc_sk,
                c2175_validfrom,
                c2175_validto,
                c2175_level,
                c2175_dataid,
                ROW_NUMBER() OVER(partition by c2175_dataid ORDER BY c2175_validfrom DESC) row_num
            FROM meta_owner.meta_cst_2175
                WHERE c2175_level = 2) lvl2
            ON lvl2.c2175_dataid = COALESCE(lvl3.c2175_crmprdhiskid, lvl4.c2175_crmprdhiskid, lvl5.c2175_crmprdhiskid)
            AND lvl2.row_num = 1
        
        LEFT JOIN (
            SELECT 
                c2175_s3katprodid,
                c2175_crmprdhiskid,
                c2175_bus_desc_sk,
                c2175_validfrom,
                c2175_validto,
                c2175_level,
                c2175_dataid,
                ROW_NUMBER() OVER(partition by c2175_dataid ORDER BY c2175_validfrom DESC) row_num
            FROM meta_owner.meta_cst_2175
                WHERE c2175_level = 1) lvl1
            ON lvl1.c2175_dataid = COALESCE(lvl2.c2175_crmprdhiskid, lvl3.c2175_crmprdhiskid, lvl4.c2175_crmprdhiskid, lvl5.c2175_crmprdhiskid)
            AND lvl1.row_num = 1
        WHERE lvl5.c2175_s3katprodid is not null;

	-- ATTR_CLIENT_BASE
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    -- clients from T24, IBIS, PROFILE    
	ETL_OWNER.etl_dbms_util.truncate_table('STG_CLIENT_BASE_MAIN_SYST',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_CLIENT_BASE_MAIN_SYST    
		SELECT
			customerid,
			MIN(open_dt) open_dt,
			-- the oldest not null cc_code
			COALESCE(max(CASE WHEN CRIT2=1 AND CRIT=1 THEN CC_CODE ELSE NULL END), max(CASE WHEN CRIT2=0 AND CRIT=1 THEN NULL ELSE NULL END)) CC_CODE,
			max(T24) T24,
			max(PROFILE) PROFILE,
			max(IBIS) IBIS
		FROM (
			SELECT
				PR.PARTY_ID customerid,
				CG.CONT_GEN_OPEN_DT open_dt,
				CASE WHEN UCC.SEGMENT is null AND CC.COST_CNTR_SRC_VAL not like '%-%' THEN null ELSE CC.COST_CNTR_SRC_VAL END CC_CODE,
				CASE WHEN C.SRC_SYST_CD = 501 THEN 1 ELSE 0 END T24,
				CASE WHEN C.SRC_SYST_CD = 510 THEN 1 ELSE 0 END PROFILE, 
				CASE WHEN C.SRC_SYST_CD = 520 THEN 1 ELSE 0 END IBIS,
				ROW_NUMBER() OVER (PARTITION BY PR.PARTY_ID, (CASE WHEN UCC.SEGMENT is null THEN 0 ELSE 1 END) ORDER BY CG.CONT_GEN_OPEN_DT, PC.party_cont_start_dt, CC.cost_cntr_src_val) AS CRIT,
				CASE WHEN UCC.SEGMENT is null THEN 0 ELSE 1 END CRIT2
			FROM L0_OWNER.prod PRD 
			
			JOIN L0_OWNER.CONT_PROD CP
				ON  CP.prod_id = PRD.prod_id
				AND CP.cont_prod_start_dt <= process_dt
				AND CP.cont_prod_end_dt > process_dt
				
			JOIN L0_OWNER.CONT C
				ON  C.cont_id = CP.cont_id
				AND (C.cont_type_cd || C.cont_single_acct_in) IN ('3N','3Y','2Y')
				AND C.src_syst_cd in (501, 510, 520)
					 
			JOIN L0_OWNER.CONT_STAT CST
				ON  CST.cont_id = C.cont_id
				AND CST.cont_stat_start_dt <= process_dt
				AND CST.cont_stat_end_dt > process_dt
				AND CASE WHEN CST.cont_stat_type_cd IN (0,2) AND C.src_syst_cd in (501, 510) THEN 1
                         WHEN  CST.cont_stat_type_cd IN (0) AND C.src_syst_cd in (520) THEN 1 ELSE 0 END = 1
				
			JOIN L0_OWNER.CONT_GEN CG
				ON  CG.cont_id = C.cont_id
				AND CG.cont_gen_start_dt <= process_dt
				AND CG.cont_gen_open_dt <= process_dt
				-----	NOVA PRODUKCIA T24 s PREDCISLIM 200002 sa berie od polky marca 2022 (kvoli test datam na PROD T24), MIGROVANE OTP DO T24 sa beru vsetky
				AND CASE
						WHEN C.SRC_SYST_CD = 501 AND substr(C.CONT_ACCT, 1, 6) = '200002' AND CG.cont_gen_open_dt < date '2022-03-14' THEN 0
						ELSE 1
					END = 1 -- Zmena R.Nichta 21.7.2022 (Zmena kvoli odfiltrovavanym spotrebakom, ktore sa po migracii stracali v reportoch)
				AND CG.cont_gen_close_dt > process_dt
				AND CG.cont_gen_end_dt > process_dt
				
			LEFT JOIN L0_OWNER.CONT_COST_CNTR CCC
				ON  CCC.cont_id = C.cont_id
				AND CCC.cont_cost_cntr_start_dt <= process_dt
				AND CCC.cont_cost_cntr_end_dt > process_dt
				
			LEFT JOIN L0_OWNER.cost_cntr CC
				ON  CC.cost_cntr_cd = CCC.cost_cntr_cd
				AND CC.src_syst_cd in (120, 510, 520)
				AND CC.cost_cntr_type_cd = case when CC.src_syst_cd = 510 then 5
												when CC.src_syst_cd = 520 then 7
										   else 14 end    
						   
			JOIN L0_OWNER.PARTY_CONT PC
				ON  PC.cont_id = C.cont_id
				AND PC.party_cont_start_dt <= process_dt
				AND PC.party_cont_end_dt > process_dt
                AND CASE WHEN C.src_syst_cd in (501, 510) AND PC.party_role_cd IN (1) THEN 1
                         WHEN C.src_syst_cd in (520) AND PC.party_role_cd IN (1) THEN 1
                         ELSE 0 END = 1
				   
			JOIN L0_OWNER.PARTY_RLTD PR
				ON  PR.child_party_id = PC.party_id
				AND PR.party_rltd_start_dt <= process_dt
				AND PR.party_rltd_end_dt > process_dt
				AND PR.party_rltd_rsn_cd = 4 -- customerid

			LEFT JOIN PM_OWNER.STG_CC_CODE_SEG UCC
				ON UCC.CC_CODE = CC.COST_CNTR_SRC_VAL
			  
			WHERE 1=1  
			AND PRD.prod_type_cd = 1)
		GROUP BY customerid;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;
    
    -- clients with authorized card to retail account of which they are not the owner
	ETL_OWNER.etl_dbms_util.truncate_table('STG_CLIENT_BASE_UNICARD',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_CLIENT_BASE_UNICARD
		SELECT
			PR.PARTY_ID customerid,
			MIN(CAG.card_gen_create_dt) open_dt,
			NULL CC_CODE,
			1 UNICARD
		FROM L0_OWNER.CARD

		JOIN L0_OWNER.CARD_GEN CAG
			ON  CAG.card_id = CARD.card_id
			AND CAG.card_gen_start_dt <=  process_dt
			AND CAG.card_gen_end_dt > process_dt
			AND CAG.card_gen_exp_dt > process_dt

		-- select only active card    
		JOIN L0_OWNER.CARD_STAT CARS
			ON  CARS.card_id = CARD.card_id
			AND CARS.card_stat_start_dt <=  process_dt
			AND CARS.card_stat_end_dt > process_dt
			AND CARS.card_stat_type_cd IN (12, 13, 16, 17)
			
		-- select only disponent card
		JOIN L0_OWNER.PARTY_CARD PCARD
			ON  PCARD.card_id = CARD.card_id
			AND PCARD.party_card_start_dt <= process_dt
			AND PCARD.party_card_end_dt > process_dt
			AND PCARD.party_role_cd = 8     

		-- card owner - customerid  
		JOIN L0_OWNER.PARTY_RLTD PR
			ON  PR.child_party_id = PCARD.party_id
			AND PR.party_rltd_start_dt <= process_dt
			AND PR.party_rltd_end_dt > process_dt
			AND PR.party_rltd_rsn_cd = 4
			
		JOIN L0_OWNER.CONT_CARD CC
			ON  CC.card_id = CARD.card_id
			AND CC.cont_card_start_dt <= process_dt
			AND CC.cont_card_end_dt > process_dt 
			
		JOIN L0_OWNER.PARTY_CONT PC
			ON  PC.cont_id = CC.cont_id
			AND PC.party_cont_start_dt <= process_dt
			AND PC.party_cont_end_dt > process_dt
			AND PC.party_role_cd = 1 -- client

		-- only retail account owner
		JOIN L0_OWNER.PARTY P2
			ON P2.party_id = PC.party_id
			AND P2.party_subtp_cd = 0
		   
		WHERE
			CARD.src_syst_cd = 650			
		GROUP BY PR.PARTY_ID, NULL, 1;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    -- clients in system CAPITOL and MUREX (funds and financial market)  
	ETL_OWNER.etl_dbms_util.truncate_table('STG_CLIENT_BASE_CAPITOL',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_CLIENT_BASE_CAPITOL
        SELECT
            COALESCE(cap.customerid, mur.customerid) customerid,
            COALESCE(cap.OPEN_DT, mur.OPEN_DT) open_dt,
            COALESCE(cap.CC_CODE, mur.CC_CODE) CC_CODE,
            1 CAPITOL 
        FROM
            (SELECT
                PRL.party_id CUSTOMERID,
                MIN(CG.cont_gen_open_dt) OPEN_DT,
                MAX(P.party_subtp_cd) CC_CODE,
                1 CAPITOL
            FROM L0_OWNER.CONT C
            
            JOIN L0_OWNER.CONT_GEN CG
                ON  CG.cont_id = C.cont_id
                AND CG.cont_gen_start_dt <= process_dt
                AND CG.cont_gen_open_dt <= process_dt
                AND CG.cont_gen_close_dt > process_dt
                AND CG.cont_gen_open_dt > DATE '1900-01-01'
                AND CG.cont_gen_end_dt > process_dt
                
            JOIN L0_OWNER.FUND_CONT FC
                ON  FC.cont_id = C.cont_id
                AND FC.fund_cont_start_dt <= process_dt
                AND FC.fund_cont_end_dt > process_dt
                AND FC.portf_attr_cd IN (-1, 17, 19, 20, 21, 22, 32, 35, 36) -- SR portfolio
                
            JOIN L0_OWNER.CONT_PROD CP
                ON  CP.cont_id = C.cont_id
                AND CP.cont_prod_start_dt <= process_dt
                AND CP.cont_prod_end_dt > process_dt
        
            JOIN L0_OWNER.PARTY_CONT PC
                ON  PC.cont_id = C.cont_id
                AND PC.party_cont_start_dt <= process_dt
                AND PC.party_cont_end_dt > process_dt
                AND PC.party_role_cd = 1
                
            JOIN L0_OWNER.PARTY P
                ON  P.party_id = PC.party_id
                AND P.src_syst_cd = 90
                
            JOIN L0_OWNER.PARTY_RLTD PRL
                ON  PRL.child_party_id = P.party_id
                AND PRL.party_rltd_rsn_cd = 4
                AND PRL.party_rltd_start_dt <= process_dt
                AND PRL.party_rltd_end_dt > process_dt
                
            JOIN L0_OWNER.PARTY_ROLE_STAT PRS
                ON  PRS.party_id = P.party_id
                AND PRS.party_role_cd = 1
                AND PRS.party_role_stat_start_dt <= process_dt
                AND PRS.party_role_stat_end_dt > process_dt
                
            JOIN L0_OWNER.CONT_STAT CST
                ON  CST.cont_id = C.cont_id
                AND CST.cont_stat_start_dt <= process_dt
                AND CST.cont_stat_end_dt > process_dt
            -----   STAV FONDOV
            JOIN
            (
                SELECT
                    CSTG.cont_stat_type_cd,
                    CSTG.cont_stat_type_ds
                FROM L0_OWNER.cont_stat_type_gen CSTG
                WHERE
                    CSTG.cont_stat_type_gen_start_dt <= process_dt
                    AND CSTG.cont_stat_type_gen_end_dt > process_dt
                    AND CSTG.cont_stat_type_cd NOT IN (1016,1021,1037,1038)
                    /*
                    'P20_PROPADLA-Propadlá',
                    'P20_VYPORADANO-Vypořádáno',
                    'P20_ZRUSENA-Zrušeno',
                    'P20_ZRUSIT-Zrušit'
                    */
            ) CSG
                ON  CSG.cont_stat_type_cd = CST.cont_stat_type_cd
            WHERE
               C.src_syst_cd = 90
            GROUP BY PRL.party_id, 1) cap
        
        FULL JOIN (    
            SELECT
                PRL.PARTY_ID CUSTOMERID,
                MIN(CG.cont_gen_open_dt) open_dt,
                MAX(P.party_subtp_cd) CC_CODE,
                1 MUREX
            FROM L0_OWNER.CONT C
            JOIN L0_OWNER.CONT_GEN CG
                ON  CG.cont_id = C.cont_id
                AND CG.cont_gen_start_dt <= process_dt
                AND CG.cont_gen_end_dt > process_dt
                AND CG.cont_gen_open_dt <= process_dt
                AND CG.cont_gen_open_dt > DATE '1900-01-01'
                AND CG.cont_gen_close_dt > process_dt
                AND CG.cont_gen_matur_dt > process_dt
            JOIN L0_OWNER.FM_POS_BAL FPB
                ON  FPB.cont_id = C.cont_id
                AND FPB.meas_cd = 3000
                AND FPB.fm_pos_bal_dt > ADD_MONTHS(process_dt, -1)
            -- SR products
            JOIN L0_OWNER.GL_ACCT GA
                ON  GA.gl_acct_id = FPB.gl_acct_id
                AND GA.src_syst_cd = 530
                AND GA.gl_acct_dmn_cd = 27
            JOIN L0_OWNER.PARTY_CONT PC
                ON  PC.cont_id = C.cont_id
                AND PC.party_cont_start_dt  <= process_dt
                AND PC.party_cont_end_dt    >  process_dt
                AND PC.party_role_cd = 1
            JOIN L0_OWNER.PARTY P
                ON  P.party_id = PC.party_id
                AND P.src_syst_cd = 40
            JOIN L0_OWNER.PARTY_RLTD PRL
                ON  PRL.child_party_id = P.party_id
                AND PRL.party_rltd_rsn_cd = 4
                AND PRL.party_rltd_start_dt <= process_dt
                AND PRL.party_rltd_end_dt   >  process_dt     
            WHERE
                C.src_syst_cd = 40
            GROUP BY
                PRL.party_id, 1) mur
        ON cap.customerid = mur.customerid;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;
 
	ETL_OWNER.etl_dbms_util.truncate_table('STG_CLIENT_BASE_LEASING',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_CLIENT_BASE_LEASING
        SELECT
            CUSTOMERID,
            open_dt,
            COALESCE(CC_CODE_VAL, CC_CODE_PAR) CC_CODE,  
            LEASING       
        FROM (
            SELECT
                PRL.PARTY_ID CUSTOMERID,
                MIN(CG.cont_gen_open_dt) OVER(PARTITION BY PRL.PARTY_ID) open_dt,
                TO_CHAR(MAX(CCC.COST_CNTR_SRC_VAL) OVER(PARTITION BY PRL.PARTY_ID)) CC_CODE_VAL,
                TO_CHAR(MAX(P.party_subtp_cd) OVER(PARTITION BY PRL.PARTY_ID)) CC_CODE_PAR,                
                1 LEASING,
                ROW_NUMBER() OVER(PARTITION BY PRL.PARTY_ID ORDER BY PCC.party_cost_cntr_start_dt DESC) AS row_num
            FROM L0_OWNER.PARTY P
                
            JOIN L0_OWNER.PARTY_CONT PC
                ON  PC.party_id = P.party_id
                AND PC.party_role_cd = 142 -- Klient Leasing SR
                AND PC.party_cont_start_dt <= process_dt
                AND PC.party_cont_end_dt > process_dt
            
            JOIN L0_OWNER.CONT_GEN CG
                ON  CG.cont_id = PC.cont_id
                AND CG.cont_gen_start_dt <= process_dt
                AND CG.cont_gen_end_dt > process_dt
                AND CG.cont_gen_close_dt > process_dt
                
            JOIN L0_OWNER.PARTY_RLTD PRL
                ON  PRL.child_party_id = P.party_id
                AND PRL.party_rltd_start_dt <= process_dt
                AND PRL.party_rltd_end_dt > process_dt
                AND PRL.party_rltd_rsn_cd = 4
                        
            LEFT JOIN L0_OWNER.PARTY_COST_CNTR PCC
                ON  PCC.party_id = PRL.party_id
                AND PCC.party_cost_cntr_start_dt <= process_dt
                --AND PCC.party_cost_cntr_end_dt > process_dt
                
            LEFT JOIN L0_OWNER.cost_cntr CCC
                ON  CCC.cost_cntr_cd = PCC.cost_cntr_cd
                AND CCC.src_syst_cd in (120, 510, 520, 530)
                AND CCC.cost_cntr_type_cd = case when CCC.src_syst_cd = 510 then 5
                                                 when CCC.src_syst_cd = 520 then 7
                                                 when CCC.src_syst_cd = 530 then 2
                                            else 14 end
                         
            WHERE
                P.src_syst_cd = 820)
        WHERE row_num = 1;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

	ETL_OWNER.etl_dbms_util.truncate_table('STG_CLIENT_BASE_INSURANCE',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_CLIENT_BASE_INSURANCE
        SELECT
            CUSTOMERID,
            open_dt,
            COALESCE(CC_CODE_VAL, CC_CODE_PAR) CC_CODE,  
            INSURANCE       
        FROM (
            SELECT        
                PR.PARTY_ID CUSTOMERID,
                MIN(CG.cont_gen_open_dt) OVER(PARTITION BY PR.PARTY_ID) open_dt,
                TO_CHAR(MAX(CCC.COST_CNTR_SRC_VAL) OVER(PARTITION BY PR.PARTY_ID)) CC_CODE_VAL,
                TO_CHAR(MAX(P.party_subtp_cd) OVER(PARTITION BY PR.PARTY_ID)) CC_CODE_PAR,
                MAX(CONT_STAV.OPENED) OVER(PARTITION BY PR.PARTY_ID) OPENED,
                1 INSURANCE,
                ROW_NUMBER() OVER(PARTITION BY PR.PARTY_ID ORDER BY PCC.party_cost_cntr_start_dt DESC) AS row_num
            FROM L0_OWNER.PARTY P
                
            JOIN L0_OWNER.PARTY_CONT PC
                ON  PC.party_id = P.party_id
                AND PC.party_role_cd = 168 -- Klient Poisťovni SR
                AND PC.party_cont_start_dt <= process_dt
                AND PC.party_cont_end_dt > process_dt
                
            JOIN L0_OWNER.CONT C
                ON  C.cont_id = PC.cont_id
                AND (C.cont_type_cd = 3 OR (C.cont_type_cd = 2 AND C.cont_single_acct_in = 'Y'))
                AND C.src_syst_cd = 790
             
            JOIN L0_OWNER.PARTY_RLTD PR
                ON  PR.child_party_id = P.party_id
                AND PR.party_rltd_start_dt <= process_dt
                AND PR.party_rltd_end_dt > process_dt
                AND PR.party_rltd_rsn_cd = 4 
            
            JOIN L0_OWNER.CONT_STAT
                ON  CONT_STAT.cont_id = C.cont_id
                AND CONT_STAT.cont_stat_start_dt <= process_dt
                AND CONT_STAT.cont_stat_end_dt > process_dt
               
            JOIN L0_OWNER.CONT_GEN CG
                ON  CG.cont_id = C.cont_id
                AND CG.cont_gen_start_dt <= process_dt
                AND CG.cont_gen_end_dt > process_dt
            
            JOIN (
                SELECT
                    cont_stat_type_cd,
                    cont_stat_type_ds,
                    CASE
                        WHEN CSTG.cont_stat_type_ds NOT IN ('Storno pojistky',
                                                            'Pojistka nebyla ještě verifikována',
                                                            'Pojistka je chybná a nebyla ještě verifikován',
                                                            'Pojistka je vrácena z verifikace k opravě') THEN 1
                        ELSE 0 END as OPENED
                FROM L0_OWNER.CONT_STAT_TYPE_GEN CSTG
                WHERE
                    CSTG.cont_stat_type_gen_start_dt <= process_dt
                    AND CSTG.cont_stat_type_gen_end_dt > process_dt) CONT_STAV
                ON CONT_STAV.cont_stat_type_cd = CONT_STAT.cont_stat_type_cd
            
            LEFT JOIN L0_OWNER.CONT_DT_ACTL CDA
                ON  CDA.cont_id = C.cont_id
                AND CDA.meas_cd = 8016
                
            LEFT JOIN L0_OWNER.PARTY_COST_CNTR PCC
                ON  PCC.party_id = PR.party_id
                AND PCC.party_cost_cntr_start_dt <= process_dt
                --AND PCC.party_cost_cntr_end_dt > process_dt
                
            LEFT JOIN L0_OWNER.cost_cntr CCC
                ON  CCC.cost_cntr_cd = PCC.cost_cntr_cd
                AND CCC.src_syst_cd in (120, 510, 520, 530)
                AND CCC.cost_cntr_type_cd = case when CCC.src_syst_cd = 510 then 5
                                                 when CCC.src_syst_cd = 520 then 7
                                                 when CCC.src_syst_cd = 530 then 2
                                            else 14 end
        )
        WHERE row_num = 1
        AND OPENED = 1;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;
	
	INSERT INTO PM_OWNER.ATTR_CLIENT_BASE
        SELECT 
            MAIN.MONTH_ID,
            MAIN.CUSTOMERID,
            CI.CCDB_ID,
            CASE WHEN PA.party_subtp_cd>0 THEN COALESCE(PO.party_org_ico_no, PO.party_org_zeco_no) ELSE NULL END ICO, -- fix PB MUREX ID
            MAIN.CC_CODE, 
            CASE 
                -- correction
                WHEN PA.party_subtp_cd=0 AND (UCC.SEGMENT != 'PB' OR UCC.SEGMENT is null) THEN 'RET'
                WHEN (UCC.SEGMENT is null or UCC.SEGMENT in ('RET', 'PB')) AND PA.party_subtp_cd>0 THEN 'Micro SME'
                -- UNICARD
                WHEN UCC.SEGMENT is null AND MAIN.T24=0 AND MAIN.PROFILE=0 AND MAIN.IBIS=0 AND MAIN.CAPITOL=0 AND MAIN.UNICARD=1 THEN 'RET'
                -- others
                WHEN UCC.SEGMENT is not NULL THEN UCC.SEGMENT
                ELSE 'NOSEG' END SEGMENT,
            -- age calculation
            CASE
                WHEN PI.party_indiv_birth_dt IN (DATE '9999-12-31', DATE '1900-01-01', DATE '1911-01-01') OR PA.party_subtp_cd>0 THEN NULL
                WHEN MAIN.INSURANCE = 1 AND TRUNC(months_between(process_dt, PI.party_indiv_birth_dt)/12) >= 110 THEN TRUNC(months_between(process_dt, PI.party_indiv_birth_dt)/12) - 100
                ELSE TRUNC(months_between(process_dt, PI.party_indiv_birth_dt)/12)
            END AGE,
            CASE
                WHEN PA.party_subtp_cd=0 AND (PI.party_indiv_gender_cd='M' OR (PI.party_indiv_gender_cd is null AND substr(PI.party_indiv_rc, 3, 1) in ('0', '1'))) THEN 1 
                WHEN PA.party_subtp_cd=0 AND (PI.party_indiv_gender_cd='F' OR (PI.party_indiv_gender_cd is null AND substr(PI.party_indiv_rc, 3, 1) in ('5', '6'))) THEN 0
                ELSE NULL END SEX_MALE,
            CASE WHEN resid_close.i_resident = 'N' THEN 0 ELSE 1 END i_resident,
            CASE WHEN (COALESCE(resid_close.dissolution_dt, DATE '9999-12-31') <= process_dt OR COALESCE(PI.party_indiv_death_dt, DATE '9999-12-31') <= process_dt) THEN 0
                ELSE 1 END i_live,
            MAIN.FG_TENURE,
            MAIN.BANK_TENURE,
            MAIN.INSURANCE_TENURE,
            MAIN.LEASING_TENURE,
            MAIN.T24,
            MAIN.PROFILE,
            MAIN.IBIS,
            MAIN.UNICARD UNICARD_AUTHORIZED_RET,
            MAIN.CAPITOL,
            MAIN.BANK,
            MAIN.INSURANCE,
            MAIN.LEASING,
            coalesce(EMP.I_EMPLOYEE, 0) I_EMPLOYEE,
            GREATEST(coalesce(EMP.I_BANK_EMPLOYEE, 0), coalesce(EMP.I_BSB_EMPLOYEE, 0)) I_BANK_EMPLOYEE,
            coalesce(EMP.I_INS_EMPLOYEE, 0) I_INS_EMPLOYEE,        
            coalesce(EMP.I_LEAS_EMPLOYEE, 0) I_LEAS_EMPLOYEE,
            coalesce(EMP.I_AM_EMPLOYEE, 0) I_AM_EMPLOYEE,
            CASE WHEN MO.CUSTOMERID is not null THEN 1 ELSE 0 END I_REJECT_MODELLING
        FROM (
            SELECT
                TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM')) MONTH_ID,
                coalesce(MA.customerid, UN.customerid, CA.customerid, INS.CUSTOMERID, LEA.CUSTOMERID) CUSTOMERID,
                max(coalesce((CASE WHEN UCC.SEGMENT is null THEN NULL ELSE CCC.COST_CNTR_SRC_VAL END),
                              MA.CC_CODE, UN.CC_CODE, CA.CC_CODE, LEA.CC_CODE, INS.CC_CODE, '0000')) CC_CODE,
                max(LEAST(process_dt - LEAST(COALESCE(MA.OPEN_DT, process_dt),
                                             COALESCE(UN.OPEN_DT, process_dt),
                                             COALESCE(CA.OPEN_DT, process_dt),
                                             COALESCE(INS.OPEN_DT, process_dt),
                                             COALESCE(LEA.OPEN_DT, process_dt)), process_dt - process_dt_hist)) FG_TENURE,
                max(LEAST(process_dt - LEAST(COALESCE(MA.OPEN_DT, process_dt),
                                             COALESCE(UN.OPEN_DT, process_dt),
                                             COALESCE(CA.OPEN_DT, process_dt)), process_dt - process_dt_hist)) BANK_TENURE,
                max(LEAST(process_dt - COALESCE(INS.OPEN_DT, process_dt), process_dt - process_dt_hist)) INSURANCE_TENURE,
                max(LEAST(process_dt - COALESCE(LEA.OPEN_DT, process_dt), process_dt - process_dt_hist)) LEASING_TENURE,
                max(coalesce(MA.T24, 0)) T24,
                max(coalesce(MA.PROFILE, 0)) PROFILE,
                max(coalesce(MA.IBIS, 0)) IBIS,
                max(coalesce(UN.UNICARD, 0)) UNICARD,
                max(coalesce(CA.CAPITOL, 0)) CAPITOL,
                GREATEST(max(coalesce(MA.T24, 0)), max(coalesce(MA.PROFILE, 0)), max(coalesce(MA.IBIS, 0)),
                         max(coalesce(UN.UNICARD, 0)), max(coalesce(CA.CAPITOL, 0))) BANK, 
                max(coalesce(INS.INSURANCE, 0)) INSURANCE,       
                max(coalesce(LEA.LEASING, 0)) LEASING
            FROM PM_OWNER.STG_CLIENT_BASE_MAIN_SYST MA
            
            FULL JOIN PM_OWNER.STG_CLIENT_BASE_UNICARD UN
                ON MA.CUSTOMERID = UN.CUSTOMERID

            FULL JOIN PM_OWNER.STG_CLIENT_BASE_CAPITOL CA
                ON coalesce(MA.CUSTOMERID, UN.CUSTOMERID) = CA.CUSTOMERID                       
            
            FULL JOIN PM_OWNER.STG_CLIENT_BASE_INSURANCE INS
                ON coalesce(MA.CUSTOMERID, UN.CUSTOMERID, CA.CUSTOMERID) = INS.CUSTOMERID
                
            FULL JOIN PM_OWNER.STG_CLIENT_BASE_LEASING LEA
                ON coalesce(MA.CUSTOMERID, UN.CUSTOMERID, CA.CUSTOMERID, INS.CUSTOMERID) = LEA.CUSTOMERID
                      
            LEFT JOIN L0_OWNER.PARTY_COST_CNTR PCC
                ON  PCC.party_id = coalesce(MA.CUSTOMERID, UN.CUSTOMERID, CA.CUSTOMERID, INS.CUSTOMERID, LEA.CUSTOMERID)
                AND PCC.party_cost_cntr_start_dt <= process_dt
                AND PCC.party_cost_cntr_end_dt > process_dt
                                            
            LEFT JOIN L0_OWNER.cost_cntr CCC
                ON  CCC.cost_cntr_cd = PCC.cost_cntr_cd
                AND CCC.src_syst_cd in (120, 510, 520, 530)
                AND CCC.cost_cntr_type_cd = case when CCC.src_syst_cd = 510 then 5
                                                 when CCC.src_syst_cd = 520 then 7
                                                 when CCC.src_syst_cd = 530 then 2
                                            else 14 end
        
            LEFT JOIN PM_OWNER.STG_CC_CODE_SEG UCC
                ON  UCC.CC_CODE = CCC.COST_CNTR_SRC_VAL										   
        
            WHERE 1=1
            GROUP BY coalesce(MA.customerid, UN.customerid, CA.customerid, INS.CUSTOMERID, LEA.CUSTOMERID), process_dt
            ) MAIN
        
        LEFT JOIN L0_OWNER.PARTY PA
            ON  PA.party_id = main.customerid
        
        LEFT JOIN L0_OWNER.PARTY_INDIV PI
            ON  PI.party_id = main.customerid
            AND PI.party_indiv_start_dt <= process_dt
            AND PI.party_indiv_open_dt <= process_dt
            AND PI.party_indiv_end_dt > process_dt
            AND NVL(PI.party_indiv_close_dt, date '9999-12-31') > process_dt
        
        -- ICO		
        LEFT JOIN L0_OWNER.PARTY_ORG PO
            ON  PO.party_id = main.customerid
            AND PO.party_org_start_dt <= process_dt
            AND PO.party_org_open_dt <= process_dt
            AND PO.party_org_end_dt > process_dt
            AND NVL(PO.party_org_close_dt, date '9999-12-31') > process_dt
            AND PO.party_org_ico_no not in ('00999997') -- spojena skola - divne zaznamy
            
        LEFT JOIN (
            SELECT
                COALESCE(org_resid_close.customerid, indiv_resid.customerid) customerid,
                COALESCE(org_resid_close.i_resident, indiv_resid.i_resident) i_resident,
                COALESCE(org_resid_close.dissolution_dt, DATE '9999-12-31') dissolution_dt
            FROM 
                (SELECT
                    PR.PARTY_ID customerid,
                    MAX(PO.PARTY_ORG_RESID_SR_IN) i_resident,
                    MAX(PRS.party_role_stat_eff_dt) dissolution_dt
                FROM L0_OWNER.PARTY P
                
                LEFT JOIN L0_OWNER.PARTY_ORG PO
                    ON P.PARTY_ID = PO.PARTY_ID
                    AND PO.PARTY_ORG_START_DT <= process_dt 
                    AND PO.PARTY_ORG_END_DT > process_dt
                    
                LEFT JOIN L0_OWNER.PARTY_ROLE_STAT PRS
                    ON P.PARTY_ID = PRS.PARTY_ID
                    AND PRS.party_role_stat_start_dt <= process_dt
                    AND PRS.party_role_stat_end_dt > process_dt
                    AND PRS.party_role_cd = 81
                    AND PRS.party_stat_type_cd in (10007)
                
                JOIN L0_OWNER.PARTY_RLTD PR
                    ON  PR.child_party_id = PO.party_id
                    AND PR.party_rltd_rsn_cd = 4
                    AND PR.party_rltd_start_dt <= process_dt
                    AND PR.party_rltd_end_dt > process_dt   
                GROUP BY PR.PARTY_ID) org_resid_close
            
            FULL JOIN 
            
                (SELECT
                    PR.PARTY_ID customerid,
                    MAX(PO.PARTY_INDIV_RESID_SR_IN) i_resident
                FROM L0_OWNER.PARTY P
                
                LEFT JOIN L0_OWNER.PARTY_INDIV PO
                    ON P.PARTY_ID = PO.PARTY_ID
                    AND PO.PARTY_INDIV_START_DT <= process_dt 
                    AND PO.PARTY_INDIV_END_DT > process_dt
                
                JOIN L0_OWNER.PARTY_RLTD PR
                    ON  PR.child_party_id = PO.party_id
                    AND PR.party_rltd_rsn_cd = 4
                    AND PR.party_rltd_start_dt <= process_dt
                    AND PR.party_rltd_end_dt > process_dt   
                GROUP BY PR.PARTY_ID) indiv_resid  
                ON org_resid_close.customerid = indiv_resid.customerid) resid_close
                ON resid_close.customerid = MAIN.CUSTOMERID   
        
        -- CCDB_ID
        LEFT JOIN (
            SELECT
                CCDB_ID,
                CUSTOMERID
            FROM (
                SELECT
                    distinct
                    pp.cd_rec_party_1 as CCDB_ID
                    , COALESCE(CASE WHEN P.SRC_SYST_CD = 1 THEN p.party_id
                                        WHEN P.SRC_SYST_CD = 110 THEN pr.party_id
                                        ELSE NULL END, NULL) customerid
                    , COUNT(distinct pp.cd_rec_party_1) OVER(PARTITION BY COALESCE(CASE WHEN P.SRC_SYST_CD = 1 THEN p.party_id
                                        WHEN P.SRC_SYST_CD = 110 THEN pr.party_id
                                        ELSE NULL END, NULL)) multiple_customerid
                FROM CCDB_OWNER.oh_w_party_party PP
                -- check existence of party_1
                INNER JOIN CCDB_OWNER.oh_w_party p1
                  ON p1.cd_rec_party = PP.cd_rec_party_1
                    AND p1.cd_rec_object_type = PP.CD_REC_PARTY_1_OBJECT_TYPE
                    AND p1.cd_rec_object_type = PP.cd_rec_party_1_object_type
                    AND p1.cd_rec_source = PP.CD_REC_PARTY_1_SOURCE
                    AND p1.deleted_dt > process_dt
                    -- platnost zaznamu v DMSK
                    and P1.PARTY_START_DT <= process_dt
                    and P1.PARTY_END_DT > process_dt    
                -- check existence of party_2
                INNER JOIN CCDB_OWNER.oh_w_party p2
                  ON p2.cd_rec_party = PP.cd_rec_party_2
                    AND p2.cd_rec_object_type = PP.CD_REC_PARTY_2_OBJECT_TYPE
                    AND p2.cd_rec_object_type = PP.cd_rec_party_2_object_type
                    AND p2.cd_rec_source = PP.CD_REC_PARTY_2_SOURCE
                    AND p2.deleted_dt > process_dt
                    -- platnost zaznamu v DMSK
                    and P2.PARTY_START_DT <= process_dt
                    and P2.PARTY_END_DT > process_dt     
                LEFT JOIN L0_OWNER.PARTY P
                    ON P.PARTY_SRC_KEY = pp.cd_rec_party_2
                    AND P.SRC_SYST_CD=pp.cd_rec_party_2_source
                    AND P.PARTY_SRC_VAL=p2.cd_lgc_orn_arf_key    
                LEFT JOIN L0_OWNER.PARTY_RLTD PR
                    ON  PR.child_party_id = p.party_id
                    AND PR.party_rltd_start_dt <= process_dt
                    AND PR.party_rltd_end_dt > process_dt
                    AND PR.party_rltd_rsn_cd = 4 -- customerid       
                where 1=1
                    -- platnost zaznamu v DMSK
                    and PP.PARTY_PARTY_START_DT <= process_dt
                    and PP.PARTY_PARTY_END_DT > process_dt    
                    and (
                      PP.cd_rec_relation_type = 'PARTY_INSTANCE'
                      and pp.cd_rec_relation_type_source = 'CCDB')
                    and pp.cd_rec_source = 'CCDB'
                    and pp.deleted_dt > process_dt
                    -- party_1 = CRM = CCDB_ID
                    and pp.cd_rec_party_1_source = 'CCDB.CRM'
                    -- relation to CUID or Ataccama (001)
                    AND pp.cd_rec_party_2_source IN ('110', '1')
                    AND COALESCE(CASE WHEN P.SRC_SYST_CD = 1 THEN p.party_id
                                        WHEN P.SRC_SYST_CD = 110 THEN pr.party_id
                                        ELSE NULL END, NULL) is not null)
            WHERE multiple_customerid = 1) CI
            ON CI.CUSTOMERID = MAIN.CUSTOMERID
        
        -- CC
        LEFT JOIN PM_OWNER.STG_CC_CODE_SEG UCC
            ON  UCC.CC_CODE = MAIN.CC_CODE 
        
        -- reject modelling		
        LEFT JOIN (
            SELECT
                PR.party_id customerid
            FROM L0_OWNER.vynimka_udaj u
            
            LEFT JOIN (
                SELECT
                    su.Popis,
                    u.id_ziadost,
                    u.bigint as hodnota
                FROM L0_OWNER.vynimka_udaj u
                INNER JOIN L0_OWNER.VYNIMKA_CIS_STROM_UDAJ su
                    ON su.id = u.id_strom_udaj AND su.id = 521 -- CUID
                ) cuid
                ON u.id_ziadost = cuid.id_ziadost
            
            LEFT JOIN L0_OWNER.vynimka_rozhodnutie vr
                ON u.id_ziadost = vr.ID_ZIADOST
                
            LEFT JOIN (
                SELECT
                    u.id_ziadost,
                    to_date(u.smalldatetime, 'DD.MM.YY') as date_to -- KONVER NA DATE, lebo smalldatetime nevie rok 9999
                FROM L0_OWNER.vynimka_udaj u
                INNER JOIN L0_OWNER.VYNIMKA_CIS_STROM_UDAJ su
                    ON su.id = u.id_strom_udaj 
                    AND su.id = 694 -- POVODNY identifikator pre polozku Datum platnosti
                    
                UNION
                
                SELECT
                    u.id_ziadost,
                    to_date(ccut.Popis,'DD.MM.YYYY') as date_to -- KONVERT NA date, lebo je to STRING
                FROM L0_OWNER.vynimka_udaj u
                INNER JOIN L0_OWNER.VYNIMKA_CIS_CISEL_UDAJ_TYP ccut
                    ON u.id_ciselnik_udaj_typ = ccut.id
                    AND u.id_strom_udaj = 918
                ) datdo
                ON datdo.id_ziadost = u.id_ziadost
            
            LEFT JOIN L0_OWNER.PARTY P
                ON P.party_src_val = trim(cuid.hodnota)
                
            LEFT JOIN L0_OWNER.PARTY_RLTD PR
                ON  PR.child_party_id = P.party_id
                AND PR.party_rltd_rsn_cd = 4
                AND PR.party_rltd_start_dt <= process_dt
                AND PR.party_rltd_end_dt > process_dt
            
            WHERE 1=1
            AND u.id_ciselnik_udaj_typ = 263 -- segment Q
            AND vr.DATUM_ROZHODNUTIA <= process_dt
            AND to_date(datdo.date_to, 'DD.MM.YY') > process_dt
            
            GROUP BY PR.party_id
            ) MO
            ON MAIN.CUSTOMERID = MO.CUSTOMERID
        
        -- employee		
        LEFT JOIN (
            select 
                RC,
                1 I_EMPLOYEE,
                I_LEAS_EMPLOYEE,
                I_INS_EMPLOYEE,
                I_AM_EMPLOYEE,
                I_BANK_EMPLOYEE,
                I_BSB_EMPLOYEE
            from (
                select
                    RC,
                    ENTITY
                from
                    (select
                        replace(BIRTH_NUMBER,'/', '') RC,
                        ENTITY,
                        JOINING_DATE,
                        LEAVING_DATE,
                        rank() over (partition by birth_number order by dc_date desc, leaving_date nulls first) as ranking
                    from VEMA_OWNER.VEMA_EMPL_BANK
                    WHERE dc_date <= greatest((select min(dc_date) from VEMA_OWNER.VEMA_EMPL_BANK), process_dt))
                where ranking = 1
                    AND (LEAVING_DATE >= process_dt or LEAVING_DATE is null)
                    AND JOINING_DATE < process_dt 
                group by RC, ENTITY)
            PIVOT(
                COUNT(entity) 
                FOR entity
                IN ('LEAS' I_LEAS_EMPLOYEE,
                    'INSU' I_INS_EMPLOYEE,
                    'AM' I_AM_EMPLOYEE,
                    'BANK' I_BANK_EMPLOYEE,
                    'BSB' I_BSB_EMPLOYEE)
            )) EMP
            ON EMP.RC = PI.party_indiv_rc;
		           
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);
    
  END;
  
  PROCEDURE append_CARD(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_CARD';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'CARD';
    
  BEGIN
	
	-- CARD
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    -- stage table           
	ETL_OWNER.etl_dbms_util.truncate_table('STG_CARD',l_owner_name);

	INSERT INTO PM_OWNER.STG_CARD
        -- STG_CARD
        SELECT
            CARD.card_id,
            CAG.card_gen_create_dt,
            CAG.card_gen_exp_dt,
            CAG.card_gen_automatic_renew_in,
            CAPP.prod_id,
            CAPP.prod_src_val,
            CC.cont_id,
            PR2.PARTY_ID,
            P2.party_subtp_cd,
            CAG.card_send_meth_cd,
            CAG.pin_send_meth_cd
        FROM L0_OWNER.CARD CARD
        
        JOIN L0_OWNER.CARD_GEN CAG  
            ON  CAG.card_id = CARD.card_id
            AND CAG.card_gen_start_dt <=  process_dt
            AND CAG.card_gen_end_dt > process_dt
            AND CAG.card_gen_exp_dt > process_dt
            
        JOIN L0_OWNER.CARD_PROD CAP
            ON  CAP.card_id = CARD.card_id
            AND CAP.card_prod_start_dt <=  process_dt
            AND CAP.card_prod_end_dt > process_dt
            
        JOIN L0_OWNER.PROD CAPP
            ON  CAPP.prod_id = CAP.prod_id
            AND SUBSTR(CAPP.prod_src_key,1,3) = 'RTY'
      
        JOIN L0_OWNER.CONT_CARD CC
            ON  CC.card_id = CARD.card_id
            AND CC.cont_card_start_dt <=  process_dt
            AND CC.cont_card_end_dt > process_dt
        
        JOIN L0_OWNER.PARTY_CONT PC
            ON  PC.cont_id = CC.cont_id
            AND PC.party_cont_start_dt <= process_dt
            AND PC.party_cont_end_dt > process_dt
            AND PC.party_role_cd = 1 -- client
        
        JOIN L0_OWNER.PARTY_RLTD PR2
            ON  PR2.child_party_id = PC.party_id
            AND PR2.party_rltd_start_dt <= process_dt
            AND PR2.party_rltd_end_dt > process_dt
            AND PR2.party_rltd_rsn_cd = 4 -- customerid
        
        JOIN L0_OWNER.PARTY P2
            ON  P2.party_id = PC.party_id
        
        WHERE
            CARD.src_syst_cd = 650;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    -- stage table           
	ETL_OWNER.etl_dbms_util.truncate_table('STG_CARD_TOKEN',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_CARD_TOKEN
        -- TOKENS
        SELECT
            card_id,
            COUNT(card_id_token) c_token,
            MAX(i_googlepay) i_googlepay,
            MAX(i_applepay) i_applepay,
            MAX(i_merchant) i_merchant    
        FROM (    
            select
                max(card.card_id) card_id,
                max(token.card_id) card_id_token,
                max(CASE WHEN CPC.CARD_PARM_CD_SRC_VAL='103' THEN 1 ELSE 0 END) i_applepay,
                max(CASE WHEN CPC.CARD_PARM_CD_SRC_VAL='216' THEN 1 ELSE 0 END) i_googlepay,
                max(CASE WHEN CPC.CARD_PARM_CD_SRC_VAL='327' THEN 1 ELSE 0 END) i_merchant
                
            FROM L0_OWNER.CARD card
            JOIN L0_OWNER.CARD_RLTD rltd
                ON card.card_id=rltd.card_id
                AND rltd.card_rltd_start_dt <= process_dt
                AND rltd.card_rltd_end_dt > process_dt
            
            JOIN L0_OWNER.CARD token
                ON token.card_id=rltd.child_card_id
                
            JOIN L0_owner.CARD_PARM CP
                ON CP.CARD_ID = token.card_id
                AND CP.CARD_PARM_END_DT = date'9999-12-31'
            
            JOIN L0_OWNER.CARD_PARM_CD CPC
                ON CPC.CARD_PARM_CD_CD=CP.CARD_PARM_CD
                AND CPC.CARD_PARM_TYPE_CD=197
                
            WHERE 1=1
                AND card.src_syst_cd=650
                AND SUBSTR(token.card_src_id,1,3)='ATO'
            GROUP BY card.card_id, token.card_id)
        GROUP BY card_id;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    INSERT INTO pm_owner.card
        SELECT
            to_char(process_dt, 'YYYYMM') month_id,
            PR1.PARTY_ID authorized_user,
            CARD.PARTY_ID account_owner,
            CASE WHEN CARD.party_subtp_cd = 0 THEN 1 ELSE 0 END i_retail_account,
            PRG.party_role_gen_ds card_owner_type,
            -----   INFO ABOUT CARD
            CARD.card_id,
            CARD.prod_id PROD_ID_card,
            CASE WHEN PR1.PARTY_ID = CARD.PARTY_ID THEN 0 ELSE 1 END i_authorized_user,
            CARD.prod_src_val  prod_card,
            PN0.prod_nm_cz card_description,
            CASE WHEN LOWER(PN0.prod_nm_cz) like '%vklad%' AND LOWER(PN1.prod_nm_cz) like '%debet%' THEN 'deposit'
                WHEN LOWER(PN0.prod_nm_cz) not like '%vklad%' AND LOWER(PN1.prod_nm_cz) like '%debet%' THEN 'debit'
                WHEN (LOWER(PN1.prod_nm_cz) like '%kredit%' OR LOWER(PN1.prod_nm_cz) like '%charge%') THEN 'credit'
                ELSE 'unknown' END card_type, 
            CARD.card_gen_create_dt CREATE_DT,
            CARD.card_gen_exp_dt EXPIRATION_DT,
            process_dt - CARD.card_gen_create_dt card_tenure,
            CARD.card_gen_exp_dt - process_dt card_expiration_time,
            CASE WHEN CARD.card_gen_automatic_renew_in='Y' THEN 1 ELSE 0 END i_automatic_renewal,
            CARS.card_stat_start_dt CARD_STATUS_DT,
            CSTG.card_stat_type_cd CARD_STATUS_CD,
            CSTG.card_stat_gen_ds CARD_STATUS_DESCRIPTION,
            process_dt - CARS.card_stat_start_dt TIME_CARD_STATUS_CHANGED,
            CARD.card_send_meth_cd send_method_card,
            CARD.pin_send_meth_cd send_method_pin,
            -----   CARD LIMITS
            CL.card_lmt_start_dt card_limit_start_dt,
            CL.c_card_lmt_change c_card_limit_change,
            process_dt - CL.card_lmt_start_dt time_card_limit_changed,
            CL.ONLINE_CARD_LIMIT,
            CL.ATM_CARD_LIMIT,
            CL.POS_CARD_LIMIT,
            CL.CNP_CARD_LIMIT,
            NVL(SVC_CARD.MA_CP,0) i_travel_insurance,
            
            ----   CARD TOKENS
            coalesce(token.c_token, 0) c_token,
            coalesce(token.i_googlepay, 0) i_googlepay_token,
            coalesce(token.i_applepay, 0) i_applepay_token,
            coalesce(token.i_merchant, 0)  i_merchant_token
        FROM PM_OWNER.STG_CARD CARD
            
        JOIN L0_OWNER.PROD_NM PN0
            ON  PN0.prod_id = CARD.prod_id
            AND PN0.prod_nm_start_dt <=  process_dt
            AND PN0.prod_nm_end_dt > process_dt
            
        JOIN L0_OWNER.PROD_HIER CPH
            ON  CPH.child_prod_id = CARD.prod_id
            AND CPH.prod_hier_start_dt <=  process_dt
            AND CPH.prod_hier_end_dt > process_dt
            AND CPH.prod_hier_lvl_cd = 8
            
        JOIN L0_OWNER.PROD_HIER CPH1
            ON  CPH1.child_prod_id = CARD.prod_id
            AND CPH1.prod_hier_start_dt <=  process_dt
            AND CPH1.prod_hier_end_dt > process_dt
            AND CPH1.prod_hier_lvl_cd = 6
            
        JOIN L0_OWNER.PROD_NM PN1
            ON  PN1.prod_id = CPH1.prod_id
            AND PN1.prod_nm_start_dt <=  process_dt
            AND PN1.prod_nm_end_dt > process_dt
            
        JOIN L0_OWNER.CARD_STAT CARS
            ON  CARS.card_id = CARD.card_id
            AND CARS.card_stat_start_dt <=  process_dt
            AND CARS.card_stat_end_dt > process_dt
            --AND CARS.card_stat_type_cd IN (12,13,16,17)   --  STATUSY PRE AKTIVNU PK
            
        JOIN L0_OWNER.CARD_STAT_TYPE CST
            ON  CST.card_stat_type_cd = CARS.card_stat_type_cd
            
        JOIN L0_OWNER.CARD_STAT_TYPE_GEN CSTG
            ON  CSTG.card_stat_type_cd = CST.card_stat_type_cd
            AND CSTG.card_stat_type_gen_start_dt <=  process_dt
            AND CSTG.card_stat_type_gen_end_dt > process_dt
            
        JOIN L0_OWNER.PARTY_CARD PCARD
            ON  PCARD.card_id = CARD.card_id
            AND PCARD.party_card_start_dt <=  process_dt
            AND PCARD.party_card_end_dt > process_dt
            
        JOIN L0_OWNER.PARTY P
            ON  P.party_id = PCARD.party_id
            AND P.src_syst_cd = 650
            
        JOIN L0_OWNER.PARTY_RLTD PR1
            ON  PR1.child_party_id = PCARD.party_id
            AND PR1.party_rltd_start_dt <= process_dt
            AND PR1.party_rltd_end_dt > process_dt
            AND PR1.party_rltd_rsn_cd = 4 -- customerid
            
        JOIN L0_OWNER.PARTY_ROLE_GEN PRG
            ON  PRG.party_role_cd = PCARD.party_role_cd
            AND PRG.party_role_gen_start_dt <=  process_dt
            AND PRG.party_role_gen_end_dt  > process_dt
        
        LEFT JOIN (
            select 
                card_id,
                max(card_lmt_start_dt) card_lmt_start_dt,
                SUM(CASE WHEN card_lmt_start_dt between ADD_MONTHS(process_dt, -1)+1 AND process_dt AND lmt_type_cd in (4, 6, 40, 41) THEN 1 ELSE 0 END) c_card_lmt_change,
                max(CASE WHEN lmt_type_cd = 4 then card_lmt_am end) ONLINE_CARD_LIMIT,
                max(CASE WHEN lmt_type_cd = 6 then card_lmt_am end) ATM_CARD_LIMIT,
                max(CASE WHEN lmt_type_cd = 40 then card_lmt_am end) POS_CARD_LIMIT,
                max(CASE WHEN lmt_type_cd = 41 then card_lmt_am end) CNP_CARD_LIMIT
            from L0_OWNER.CARD_LMT
            where 1=1
                AND card_lmt_start_dt <= process_dt
                AND card_lmt_end_dt > process_dt
            group by card_id) CL
            ON  CL.card_id = CARD.card_id
        
        LEFT JOIN (
            SELECT
                CSVC.card_id,
                MAX(CASE WHEN CPH.prod_id = 159028 THEN 1 ELSE 0 END) AS MA_CP
                       
            FROM L0_OWNER.CARD_SVC CSVC
        
            JOIN L0_OWNER.PROD CPRD
                ON  CPRD.prod_id = CSVC.prod_id
                AND CPRD.prod_type_cd IN (16,136)
                
            JOIN L0_OWNER.PROD_HIER CPH
                ON  CPH.child_prod_id = CPRD.prod_id
                AND CPH.prod_hier_start_dt <=  process_dt
                AND CPH.prod_hier_end_dt > process_dt
        
            WHERE 1=1
                AND CSVC.card_svc_start_dt <=  process_dt
                AND CSVC.card_svc_end_dt > process_dt
        
            GROUP BY
                CSVC.card_id) SVC_CARD
            ON  SVC_CARD.card_id = CARD.card_id
            
        -- TOKENS
        LEFT JOIN PM_OWNER.STG_CARD_TOKEN token
            ON token.card_id = CARD.card_id;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);

    END;

  PROCEDURE append_CARD_EXPIRED(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_CARD_EXPIRED';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'CARD_EXPIRED';
    
  BEGIN
	
	-- CARD_EXPIRED
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    -- stage table           
	ETL_OWNER.etl_dbms_util.truncate_table('STG_CARD_EXPIRED',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_CARD_EXPIRED
        SELECT /*+ ORDERED */
            CAG.card_id,
            CAG.card_gen_create_dt,
            CAG.card_gen_exp_dt,
            CARD.card_src_id,
            CC.cont_card_start_dt,
            CC.cont_card_end_dt,
            PR2.PARTY_ID account_owner,
            CASE WHEN P2.party_subtp_cd = 0 THEN 1 ELSE 0 END i_retail_account,
            CAPP.prod_id,
            CAPP.prod_src_val,
            CAG.card_send_meth_cd,
            CAG.pin_send_meth_cd
        FROM L0_OWNER.CARD
        JOIN L0_OWNER.CARD_GEN CAG
            ON  CAG.card_id = CARD.card_id
            AND CAG.card_gen_start_dt <= process_dt
            AND CAG.card_gen_exp_dt <= process_dt
            AND CAG.card_gen_exp_dt > process_dt_hist
            AND CAG.card_gen_end_dt = CAG.card_gen_exp_dt
            
        JOIN L0_OWNER.CONT_CARD CC
            ON  CC.CARD_ID = CARD.card_id
            AND CC.CONT_CARD_START_DT <= process_dt          
            AND CC.CONT_CARD_END_DT  = CAG.card_gen_exp_dt
                       
        JOIN L0_OWNER.PARTY_CONT PC
            ON  PC.cont_id = CC.cont_id
            AND PC.party_cont_start_dt <= process_dt
            AND PC.party_cont_end_dt > process_dt
            AND PC.party_role_cd = 1
        
        JOIN L0_OWNER.PARTY_RLTD PR2
            ON  PR2.child_party_id = PC.party_id
            AND PR2.party_rltd_start_dt <= process_dt
            AND PR2.party_rltd_end_dt > process_dt
            AND PR2.party_rltd_rsn_cd = 4 -- customerid
        
        JOIN L0_OWNER.PARTY P2
            ON  P2.party_id = PC.party_id
            
        JOIN L0_OWNER.CARD_PROD CAP
            ON  CAP.card_id = CARD.card_id
            AND CAP.card_prod_start_dt <= process_dt
            AND CAP.card_prod_end_dt = CAG.card_gen_exp_dt

        JOIN L0_OWNER.PROD CAPP
            ON  CAPP.prod_id = CAP.prod_id
            AND SUBSTR(CAPP.prod_src_key,1,3) = 'RTY'
            
        WHERE
            CARD.src_syst_cd = 650;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    INSERT INTO pm_owner.card_expired
        SELECT
            to_number(to_char(process_dt, 'YYYYMM')) month_id,
            PR1.PARTY_ID authorized_user,
            SCE.account_owner account_owner,
            SCE.i_retail_account,
            PRG.party_role_gen_ds card_owner_type,
            SCE.card_id,
            SCE.prod_id PROD_ID_card,
            CASE WHEN PR1.PARTY_ID = SCE.account_owner THEN 0 ELSE 1 END I_authorized_user,
            SCE.prod_src_val PROD_card,
            PN0.prod_nm_cz card_description,
            CASE WHEN LOWER(PN0.prod_nm_cz) like '%vklad%' AND LOWER(PN1.prod_nm_cz) like '%debet%' THEN 'deposit'
                WHEN LOWER(PN0.prod_nm_cz) not like '%vklad%' AND LOWER(PN1.prod_nm_cz) like '%debet%' THEN 'debit'
                WHEN (LOWER(PN1.prod_nm_cz) like '%kredit%' OR LOWER(PN1.prod_nm_cz) like '%charge%') THEN 'credit'
                ELSE 'unknown' END card_type,
            SCE.card_gen_create_dt CREATE_DT,
            SCE.card_gen_exp_dt EXPIRATION_DT,
            process_dt - SCE.card_gen_exp_dt time_card_expired,
            SCE.card_send_meth_cd,
            SCE.pin_send_meth_cd
            
        FROM PM_OWNER.STG_CARD_EXPIRED SCE
            
        JOIN L0_OWNER.PROD_NM PN0
            ON  PN0.prod_id = SCE.prod_id
            AND PN0.prod_nm_start_dt <= process_dt
            AND PN0.prod_nm_end_dt > process_dt
            
        JOIN L0_OWNER.PROD_HIER CPH1
            ON  CPH1.child_prod_id = SCE.prod_id
            AND CPH1.prod_hier_lvl_cd = 6
            
        JOIN L0_OWNER.PROD_NM PN1
            ON  PN1.prod_id = CPH1.prod_id
            AND PN1.prod_nm_start_dt <= process_dt
            AND PN1.prod_nm_end_dt > process_dt

        JOIN L0_OWNER.CARD_STAT CARS
            ON  CARS.card_id = SCE.card_id
            AND CARS.card_stat_start_dt <=  process_dt
            AND CARS.card_stat_end_dt = SCE.card_gen_exp_dt
            --AND CARS.card_stat_type_cd IN (12,13,16,17)   --  STATUSY PRE AKTIVNU PK
            
        JOIN L0_OWNER.CARD_STAT_TYPE CST
            ON  CST.card_stat_type_cd = CARS.card_stat_type_cd
            
        JOIN L0_OWNER.CARD_STAT_TYPE_GEN CSTG
            ON  CSTG.card_stat_type_cd = CST.card_stat_type_cd
            AND CSTG.card_stat_type_gen_start_dt <=  process_dt
            AND CSTG.card_stat_type_gen_end_dt > process_dt  
            
        JOIN L0_OWNER.PARTY_CARD PCARD
            ON  PCARD.card_id = SCE.card_id
            AND PCARD.party_card_start_dt <=  process_dt
            AND PCARD.party_card_end_dt = SCE.card_gen_exp_dt
           
        JOIN L0_OWNER.PARTY_ROLE_GEN PRG
            ON  PRG.party_role_cd = PCARD.party_role_cd
            AND PRG.party_role_gen_start_dt <=  process_dt
            AND PRG.party_role_gen_end_dt  > process_dt
            
        JOIN L0_OWNER.PARTY_RLTD PR1
            ON  PR1.child_party_id = PCARD.party_id
            AND PR1.party_rltd_start_dt <= process_dt
            AND PR1.party_rltd_end_dt > process_dt
            AND PR1.party_rltd_rsn_cd = 4 -- customerid
                       
        LEFT JOIN L0_OWNER.SEND_METH_GEN SMG_K
            ON  SMG_K.send_meth_cd = SCE.card_send_meth_cd
            AND SMG_K.send_meth_gen_start_dt <= process_dt
            AND SMG_K.send_meth_gen_end_dt > process_dt
            
        LEFT JOIN L0_OWNER.SEND_METH_GEN SMG_P
            ON  SMG_P.send_meth_cd = SCE.pin_send_meth_cd
            AND SMG_P.send_meth_gen_start_dt <= process_dt
            AND SMG_P.send_meth_gen_end_dt > process_dt
                       
        WHERE 1=1;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);

    END;

  PROCEDURE append_attr_card(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_attr_card';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_CARD';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);

  BEGIN
	
	-- ATTR_CARD
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    INSERT INTO PM_OWNER.ATTR_CARD 
        SELECT
            main.month_id month_id,
            main.customerid customerid,
            -- OWNER CARDS 
            COALESCE(card_owner.i_opened_card_m, 0) i_opened_card_m,
            COALESCE(card_owner.c_opened_card_m, 0) c_opened_card_m, 
            COALESCE(card_owner.i_opened_card_authorizeduser_m, 0) i_opened_card_authorizeduser_m,
            COALESCE(card_owner.i_opened_deposit_card_m, 0) i_opened_deposit_card_m,    
            COALESCE(card_owner.i_opened_debit_card_m, 0) i_opened_debit_card_m,
            COALESCE(card_owner.i_opened_credit_card_m, 0) i_opened_credit_card_m,
            COALESCE(card_owner.i_changed_card_status_m, 0) i_changed_card_status_m,
            COALESCE(card_owner.i_changed_card_limit_m, 0) i_changed_card_limit_m,    
            COALESCE(card_owner.c_changed_card_limit_m, 0) c_changed_card_limit_m,
            GREATEST(COALESCE(card_q.i_opened_card_q, 0), COALESCE(card_owner.i_opened_card_m, 0)) i_opened_card_q,
            (COALESCE(card_q.c_opened_card_q, 0) + COALESCE(card_owner.c_opened_card_m, 0))/main.c_month_q c_opened_card_q,
            GREATEST(COALESCE(card_q.i_opened_card_authorizeduser_q, 0), COALESCE(card_owner.i_opened_card_authorizeduser_m, 0)) i_opened_card_authorizeduser_q,
            GREATEST(COALESCE(card_q.i_opened_deposit_card_q, 0), COALESCE(card_owner.i_opened_deposit_card_m, 0)) i_opened_deposit_card_q,
            GREATEST(COALESCE(card_q.i_opened_debit_card_q, 0), COALESCE(card_owner.i_opened_debit_card_m, 0)) i_opened_debit_card_q,
            GREATEST(COALESCE(card_q.i_opened_credit_card_q, 0), COALESCE(card_owner.i_opened_credit_card_m, 0)) i_opened_credit_card_q,
            GREATEST(COALESCE(card_q.i_changed_card_status_q, 0), COALESCE(card_owner.i_changed_card_status_m, 0)) i_changed_card_status_q,
            GREATEST(COALESCE(card_q.i_changed_card_limit_q, 0), COALESCE(card_owner.i_changed_card_limit_m, 0)) i_changed_card_limit_q,
            (COALESCE(card_q.c_changed_card_limit_q, 0) + COALESCE(card_owner.c_changed_card_limit_m, 0))/main.c_month_q c_changed_card_limit_q,
            GREATEST(COALESCE(card_y.i_opened_card_y, 0), COALESCE(card_owner.i_opened_card_m, 0)) i_opened_card_y,
            (COALESCE(card_y.c_opened_card_y, 0) + COALESCE(card_owner.c_opened_card_m, 0))/main.c_month_y c_opened_card_y,
            GREATEST(COALESCE(card_y.i_opened_card_authorizeduser_y, 0), COALESCE(card_owner.i_opened_card_authorizeduser_m, 0)) i_opened_card_authorizeduser_y,
            GREATEST(COALESCE(card_y.i_opened_deposit_card_y, 0), COALESCE(card_owner.i_opened_deposit_card_m, 0)) i_opened_deposit_card_y,
            GREATEST(COALESCE(card_y.i_opened_debit_card_y, 0), COALESCE(card_owner.i_opened_debit_card_m, 0)) i_opened_debit_card_y,
            GREATEST(COALESCE(card_y.i_opened_credit_card_y, 0), COALESCE(card_owner.i_opened_credit_card_m, 0)) i_opened_credit_card_y,
            GREATEST(COALESCE(card_y.i_changed_card_status_y, 0), COALESCE(card_owner.i_changed_card_status_m, 0)) i_changed_card_status_y,
            GREATEST(COALESCE(card_y.i_changed_card_limit_y, 0), COALESCE(card_owner.i_changed_card_limit_m, 0)) i_changed_card_limit_y,
            (COALESCE(card_y.c_changed_card_limit_y, 0) + COALESCE(card_owner.c_changed_card_limit_m, 0))/main.c_month_y c_changed_card_limit_y,   
            COALESCE(card_owner.c_card, 0) c_card,
            COALESCE(card_owner.c_card_owner, 0) c_card_owner,
            COALESCE(card_owner.i_card_authorizeduser, 0) i_card_authorizeduser,
            COALESCE(card_owner.c_card_authorizeduser, 0) c_card_authorizeduser,
            COALESCE(card_owner.c_deposit_card, 0) c_deposit_card,
            COALESCE(card_owner.c_debit_card, 0) c_debit_card,
            COALESCE(card_owner.c_credit_card, 0) c_credit_card,
            COALESCE(card_owner.min_card_tenure, 0) min_card_tenure,        
            COALESCE(card_owner.max_card_tenure, 0) max_card_tenure,
            COALESCE(card_owner.min_card_expiration_time, 0) min_card_expiration_time,
            COALESCE(card_owner.max_card_expiration_time, 0) max_card_expiration_time,  
            COALESCE(card_owner.ratio_card_renewal, 0) ratio_card_renewal,
            COALESCE(card_owner.recency_card_status_change, process_dt - process_dt_hist) min_time_card_status_change,  
            COALESCE(card_owner.c_nonactive_card, 0) c_nonactive_card,
            COALESCE(card_owner.c_canceled_card, 0) c_canceled_card,
            COALESCE(card_owner.c_blocked_card, 0) c_blocked_card,
            COALESCE(card_owner.c_lost_card, 0) c_lost_card,
            COALESCE(card_owner.recency_card_limit_change, process_dt - process_dt_hist) recency_card_limit_change,   
            COALESCE(card_owner.min_online_card_limit, 0) min_online_card_limit,   
            COALESCE(card_owner.max_online_card_limit, 0) max_online_card_limit,
            COALESCE(card_owner.min_atm_card_limit, 0) min_atm_card_limit,   
            COALESCE(card_owner.max_atm_card_limit, 0) max_atm_card_limit,
            COALESCE(card_owner.min_pos_card_limit, 0) min_pos_card_limit,    
            COALESCE(card_owner.max_pos_card_limit, 0) max_pos_card_limit,
            COALESCE(card_owner.min_cnp_card_limit, 0) min_cnp_card_limit,    
            COALESCE(card_owner.max_cnp_card_limit, 0) max_cnp_card_limit,  
            COALESCE(card_owner.ratio_card_travel_ins, 0) ratio_card_travel_ins,
            COALESCE(card_owner.c_card_token, 0) c_card_token,
            COALESCE(card_owner.i_google_card_token, 0) i_google_card_token,
            COALESCE(card_owner.i_apple_card_token, 0) i_apple_card_token,
            COALESCE(card_owner.i_merchant_card_token, 0) i_merchant_card_token,
            -- HOLDER CARDS
            COALESCE(card_holder.i_opened_card_holder_m, 0) i_opened_card_holder_m,    
            COALESCE(card_holder.i_opened_deposit_card_holder_m, 0) i_opened_deposit_card_holder_m,    
            COALESCE(card_holder.i_opened_debit_card_holder_m, 0) i_opened_debit_card_holder_m, 
            COALESCE(card_holder.i_opened_credit_card_holder_m, 0) i_opened_credit_card_holder_m,
            COALESCE(card_holder.c_opened_card_holder_m, 0) c_opened_card_holder_m,   
            GREATEST(COALESCE(card_q.i_opened_card_holder_q, 0), COALESCE(card_holder.i_opened_card_holder_m, 0)) i_opened_card_holder_q,
            GREATEST(COALESCE(card_q.i_opened_deposit_card_holder_q, 0), COALESCE(card_holder.i_opened_deposit_card_holder_m, 0)) i_opened_deposit_card_holder_q,
            GREATEST(COALESCE(card_q.i_opened_debit_card_holder_q, 0), COALESCE(card_holder.i_opened_debit_card_holder_m, 0)) i_opened_debit_card_holder_q,
            GREATEST(COALESCE(card_q.i_opened_credit_card_holder_q, 0), COALESCE(card_holder.i_opened_credit_card_holder_m, 0)) i_opened_credit_card_holder_q,
            (COALESCE(card_q.c_opened_card_holder_q, 0) + COALESCE(card_holder.c_opened_card_holder_m, 0))/main.c_month_q c_opened_card_holder_q,
            GREATEST(COALESCE(card_y.i_opened_card_holder_y, 0), COALESCE(card_holder.i_opened_card_holder_m, 0)) i_opened_card_holder_y,
            GREATEST(COALESCE(card_y.i_opened_deposit_card_holder_y, 0), COALESCE(card_holder.i_opened_deposit_card_holder_m, 0)) i_opened_deposit_card_holder_y,
            GREATEST(COALESCE(card_y.i_opened_debit_card_holder_y, 0), COALESCE(card_holder.i_opened_debit_card_holder_m, 0)) i_opened_debit_card_holder_y,
            GREATEST(COALESCE(card_y.i_opened_credit_card_holder_y, 0), COALESCE(card_holder.i_opened_credit_card_holder_m, 0)) i_opened_credit_card_holder_y,
            (COALESCE(card_y.c_opened_card_holder_y, 0) + COALESCE(card_holder.c_opened_card_holder_m, 0))/main.c_month_y c_opened_card_holder_y,   
            COALESCE(card_holder.c_card_holder, 0) c_card_holder,
            COALESCE(card_holder.c_card_retail_holder, 0) c_card_retail_holder,
            COALESCE(card_holder.c_card_nonretail_holder, 0) c_card_nonretail_holder,   
            COALESCE(card_holder.c_deposit_card_holder, 0) c_deposit_card_holder,
            COALESCE(card_holder.c_debit_card_holder, 0) c_debit_card_holder,
            COALESCE(card_holder.c_credit_card_holder, 0) c_credit_card_holder,    
            COALESCE(card_holder.min_card_tenure_holder, 0) min_card_tenure_holder,
            COALESCE(card_holder.max_card_tenure_holder, 0) max_card_tenure_holder,
            COALESCE(card_holder.min_card_expirationtime_holder, 0) min_card_expirationtime_holder,
            COALESCE(card_holder.max_card_expirationtime_holder, 0) max_card_expirationtime_holder,   
            COALESCE(card_holder.c_nonactive_card_holder, 0) c_nonactive_card_holder,
            COALESCE(card_holder.min_card_limit_holder, 0) min_card_limit_holder,
            COALESCE(card_holder.max_card_limit_holder, 0) max_card_limit_holder,
            COALESCE(card_holder.c_card_token_holder, 0) c_card_token_holder,
            COALESCE(card_holder.i_google_card_token_holder, 0) i_google_card_token_holder,
            COALESCE(card_holder.i_apple_card_token_holder, 0) i_apple_card_token_holder,
            COALESCE(card_holder.i_merchant_card_token_holder, 0) i_merchant_card_token_holder,
        --    -- EXPIRED CARDS      
            COALESCE(expcard_owner.i_expired_card_m, 0) i_expired_card_m,
            COALESCE(expcard_owner.c_expired_card_m, 0) c_expired_card_m,
            COALESCE(expcard_owner.i_expired_card_authorizeduser_m, 0) i_expired_card_authorizeduser_m,
            COALESCE(expcard_owner.i_expired_deposit_card_m, 0) i_expired_deposit_card_m,
            COALESCE(expcard_owner.i_expired_debit_card_m, 0) i_expired_debit_card_m,
            COALESCE(expcard_owner.i_expired_credit_card_m, 0) i_expired_credit_card_m,   
            GREATEST(COALESCE(card_q.i_expired_card_q, 0), COALESCE(expcard_owner.i_expired_card_m, 0)) i_expired_card_q,    
            (COALESCE(card_q.c_expired_card_q, 0) + COALESCE(expcard_owner.c_expired_card_m, 0))/main.c_month_q c_expired_card_q,
            GREATEST(COALESCE(card_q.i_expired_card_authorizeduser_q, 0), COALESCE(expcard_owner.i_expired_card_authorizeduser_m, 0)) i_expired_card_authorizeduser_q,
            GREATEST(COALESCE(card_q.i_expired_deposit_card_q, 0), COALESCE(expcard_owner.i_expired_deposit_card_m, 0)) i_expired_deposit_card_q,
            GREATEST(COALESCE(card_q.i_expired_debit_card_q, 0), COALESCE(expcard_owner.i_expired_debit_card_m, 0)) i_expired_debit_card_q,
            GREATEST(COALESCE(card_q.i_expired_credit_card_q, 0), COALESCE(expcard_owner.i_expired_credit_card_m, 0)) i_expired_credit_card_q,    
            GREATEST(COALESCE(card_y.i_expired_card_y, 0), COALESCE(expcard_owner.i_expired_card_m, 0)) i_expired_card_y,    
            (COALESCE(card_y.c_expired_card_y, 0) + COALESCE(expcard_owner.c_expired_card_m, 0))/main.c_month_y c_expired_card_y,
            GREATEST(COALESCE(card_y.i_expired_card_authorizeduser_y, 0), COALESCE(expcard_owner.i_expired_card_authorizeduser_m, 0)) i_expired_card_authorizeduser_y,
            GREATEST(COALESCE(card_y.i_expired_deposit_card_y, 0), COALESCE(expcard_owner.i_expired_deposit_card_m, 0)) i_expired_deposit_card_y,
            GREATEST(COALESCE(card_y.i_expired_debit_card_y, 0), COALESCE(expcard_owner.i_expired_debit_card_m, 0)) i_expired_debit_card_y,
            GREATEST(COALESCE(card_y.i_expired_credit_card_y, 0), COALESCE(expcard_owner.i_expired_credit_card_m, 0)) i_expired_credit_card_y,
            COALESCE(expcard_owner.c_expired_card, 0) c_expired_card,
            COALESCE(expcard_owner.c_expired_card_owner, 0) c_expired_card_owner,
            COALESCE(expcard_owner.i_expired_card_authorizeduser, 0) i_expired_card_authorizeduser,
            COALESCE(expcard_owner.c_expired_card_authorizeduser, 0) c_expired_card_authorizeduser, 
            COALESCE(expcard_owner.c_deposit_expired_card, 0) c_deposit_expired_card,
            COALESCE(expcard_owner.c_debit_expired_card, 0) c_debit_expired_card,
            COALESCE(expcard_owner.c_credit_expired_card, 0) c_credit_expired_card,
            COALESCE(expcard_owner.recency_card_expired, process_dt - process_dt_hist) recency_card_expired,
            COALESCE(expcard_holder.i_expired_card_holder_m, 0) i_expired_card_holder_m,
            COALESCE(expcard_holder.c_expired_card_holder_m, 0) c_expired_card_holder_m,
            COALESCE(expcard_holder.i_expired_deposit_cardholder_m, 0) i_expired_deposit_cardholder_m,
            COALESCE(expcard_holder.i_expired_debit_card_holder_m, 0) i_expired_debit_card_holder_m,
            COALESCE(expcard_holder.i_expired_credit_card_holder_m, 0) i_expired_credit_card_holder_m,    
            GREATEST(COALESCE(card_q.i_expired_card_holder_q, 0), COALESCE(expcard_holder.i_expired_card_holder_m, 0)) i_expired_card_holder_q,
            (COALESCE(card_q.c_expired_card_holder_q, 0) + COALESCE(expcard_holder.c_expired_card_holder_m, 0))/main.c_month_q c_expired_card_holder_q,
            GREATEST(COALESCE(card_q.i_expired_deposit_cardholder_q, 0), COALESCE(expcard_holder.i_expired_deposit_cardholder_m, 0)) i_expired_deposit_cardholder_q,
            GREATEST(COALESCE(card_q.i_expired_debit_card_holder_q, 0), COALESCE(expcard_holder.i_expired_debit_card_holder_m, 0)) i_expired_debit_card_holder_q,
            GREATEST(COALESCE(card_q.i_expired_credit_card_holder_q, 0), COALESCE(expcard_holder.i_expired_credit_card_holder_m, 0)) i_expired_credit_card_holder_q,   
            GREATEST(COALESCE(card_y.i_expired_card_holder_y, 0), COALESCE(expcard_holder.i_expired_card_holder_m, 0)) i_expired_card_holder_y,
            (COALESCE(card_y.c_expired_card_holder_y, 0) + COALESCE(expcard_holder.c_expired_card_holder_m, 0))/main.c_month_y c_expired_card_holder_y,
            GREATEST(COALESCE(card_y.i_expired_deposit_cardholder_y, 0), COALESCE(expcard_holder.i_expired_deposit_cardholder_m, 0)) i_expired_deposit_cardholder_y,
            GREATEST(COALESCE(card_y.i_expired_debit_card_holder_y, 0), COALESCE(expcard_holder.i_expired_debit_card_holder_m, 0)) i_expired_debit_card_holder_y,
            GREATEST(COALESCE(card_y.i_expired_credit_card_holder_y, 0), COALESCE(expcard_holder.i_expired_credit_card_holder_m, 0)) i_expired_credit_card_holder_y, 
            COALESCE(expcard_holder.c_expired_card_holder, 0) c_expired_card_holder,
            COALESCE(expcard_holder.c_expired_card_retail_holder, 0) c_expired_card_retail_holder,
            COALESCE(expcard_holder.c_expired_card_noretail_holder, 0) c_expired_card_noretail_holder,
            COALESCE(expcard_holder.c_deposit_expired_card_holder, 0) c_deposit_expired_card_holder,
            COALESCE(expcard_holder.c_debit_expired_card_holder, 0) c_debit_expired_card_holder,
            COALESCE(expcard_holder.c_credit_expired_card_holder, 0) c_credit_expired_card_holder,
            COALESCE(expcard_holder.recency_card_expired_holder, process_dt - process_dt_hist) recency_card_expired_holder,  
            CASE WHEN COALESCE(expcard_owner.i_expired_card_m, 0) = 1 AND COALESCE(card_owner.i_opened_card_m, 0) = 1 THEN 1 ELSE 0 END i_reopened_card_m,
            CASE WHEN (COALESCE(card_q.i_expired_card_q, 0) + COALESCE(expcard_owner.i_expired_card_m, 0)) = 1
                    AND (COALESCE(card_q.i_opened_card_q, 0) + COALESCE(card_owner.i_opened_card_m, 0)) = 1 THEN 1 ELSE 0 END i_reopened_card_q,           
            CASE WHEN (COALESCE(card_y.i_expired_card_y, 0) + COALESCE(expcard_owner.i_expired_card_m, 0)) = 1
                    AND (COALESCE(card_y.i_opened_card_y, 0) + COALESCE(card_owner.i_opened_card_m, 0)) = 1 THEN 1 ELSE 0 END i_reopened_card_y,  
            COALESCE(expcard_owner.c_expired_card_authorizeduser, 0) + COALESCE(card_owner.c_total_card_authorizeduser, 0) c_total_card_authorizeduser,
            COALESCE(expcard_owner.c_deposit_expired_card, 0) + COALESCE(card_owner.c_total_deposit_card, 0) c_total_deposit_card,
            COALESCE(expcard_owner.c_debit_expired_card, 0) + COALESCE(card_owner.c_total_debit_card, 0) c_total_debit_card,
            COALESCE(expcard_owner.c_credit_expired_card, 0) + COALESCE(card_owner.c_total_credit_card, 0) c_total_credit_card,
            COALESCE(card_owner.c_card_deliverbranch, 0) c_card_deliverbranch,
            COALESCE(card_owner.c_cardpin_deliverbranch, 0) c_cardpin_deliverbranch,
            COALESCE(card_holder.c_card_deliverbranch_holder, 0) c_card_deliverbranch_holder,
            COALESCE(card_holder.c_cardpin_deliverbranch_holder, 0) c_cardpin_deliverbranch_holder
        FROM (
            SELECT
                base.month_id,
                base.customerid,
                base.bank_tenure,
                LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -3)),1)*3,
                      COALESCE(card_q.c_month, 0)+1) c_month_q,
                LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -12)),1)*12,
                      COALESCE(card_y.c_month, 0)+1) c_month_y       
            FROM pm_owner.attr_client_base base
            LEFT JOIN (
                SELECT
                    customerid,
                    COUNT(DISTINCT month_id) c_month
                FROM pm_owner.attr_card
                WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-2), 'YYYYMM'))
                AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                GROUP BY customerid) card_q
                ON card_q.customerid = base.customerid
                
            LEFT JOIN (
                SELECT
                    customerid,
                    COUNT(DISTINCT month_id) c_month
                FROM pm_owner.attr_card
                WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-11), 'YYYYMM'))
                AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                GROUP BY customerid) card_y
                ON card_y.customerid = base.customerid                   
            WHERE base.month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))) main
                          
        LEFT JOIN (
            SELECT
                card.month_id month_id,
                card.account_owner customerid,
                -- actions attributes
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) i_opened_card_m,
                SUM(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) c_opened_card_m, 
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt)
                        AND card.account_owner != card.authorized_user THEN 1 ELSE 0 END) i_opened_card_authorizeduser_m,
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card.card_type = 'deposit' THEN 1 ELSE 0 END) i_opened_deposit_card_m,    
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card.card_type = 'debit' THEN 1 ELSE 0 END) i_opened_debit_card_m, 
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card.card_type = 'credit' THEN 1 ELSE 0 END) i_opened_credit_card_m,
                MAX(CASE WHEN (card.card_status_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) i_changed_card_status_m,
                MAX(CASE WHEN (card.card_limit_start_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) i_changed_card_limit_m,    
                SUM(CASE WHEN (card.card_limit_start_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN c_card_limit_change ELSE 0 END) c_changed_card_limit_m, 
                -- status attributes
                SUM(CASE WHEN card.card_status_cd in (12, 13) THEN 1 ELSE 0 END) c_card,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND card.account_owner = card.authorized_user THEN 1 ELSE 0 END) c_card_owner,
                MAX(CASE WHEN card.card_status_cd in (12, 13) AND card.account_owner != card.authorized_user THEN 1 ELSE 0 END) i_card_authorizeduser,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND card.account_owner != card.authorized_user THEN 1 ELSE 0 END) c_card_authorizeduser,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND card.card_type = 'deposit' THEN 1 ELSE 0 END) c_deposit_card,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND card.card_type = 'debit' THEN 1 ELSE 0 END) c_debit_card,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND card.card_type = 'credit' THEN 1 ELSE 0 END) c_credit_card,
                LEAST(MIN(card.card_tenure), process_dt - process_dt_hist) min_card_tenure,
                LEAST(MAX(card.card_tenure), process_dt - process_dt_hist) max_card_tenure,
                LEAST(MIN(card.card_expiration_time), process_dt - process_dt_hist) min_card_expiration_time,
                LEAST(MAX(card.card_expiration_time), process_dt - process_dt_hist) max_card_expiration_time,   
                ROUND(AVG(card.i_automatic_renewal), 2) ratio_card_renewal,
                LEAST(MIN(card.time_card_status_changed), process_dt - process_dt_hist) recency_card_status_change, 
                SUM(CASE WHEN card.card_status_cd not in (12, 13) THEN 1 ELSE 0 END) c_nonactive_card,
                SUM(CASE WHEN card.card_status_cd in (22) THEN 1 ELSE 0 END) c_canceled_card,
                SUM(CASE WHEN card.card_status_cd in (16, 21, 17) THEN 1 ELSE 0 END) c_blocked_card,
                SUM(CASE WHEN card.card_status_cd in (14, 15) THEN 1 ELSE 0 END) c_lost_card,
                LEAST(MIN(card.time_card_limit_changed), process_dt - process_dt_hist) recency_card_limit_change,
                LEAST(MIN(card.online_card_limit),
                    MIN(card.atm_card_limit),
                    MIN(card.pos_card_limit),
                    MIN(card.cnp_card_limit)) min_card_limit,
                GREATEST(MAX(card.online_card_limit),
                    MAX(card.atm_card_limit),
                    MAX(card.pos_card_limit),
                    MAX(card.cnp_card_limit)) max_card_limit,    
                MIN(card.online_card_limit) min_online_card_limit,   
                MAX(card.online_card_limit) max_online_card_limit,
                MIN(card.atm_card_limit) min_atm_card_limit,   
                MAX(card.atm_card_limit) max_atm_card_limit,
                MIN(card.pos_card_limit) min_pos_card_limit,    
                MAX(card.pos_card_limit) max_pos_card_limit,
                MIN(card.cnp_card_limit) min_cnp_card_limit,    
                MAX(card.cnp_card_limit) max_cnp_card_limit,
                SUM(card.i_travel_insurance) c_card_travel_ins,  
                ROUND(AVG(card.i_travel_insurance), 2) ratio_card_travel_ins,
                SUM(c_token) c_card_token,
                MAX(i_googlepay_token) i_google_card_token,
                MAX(i_applepay_token) i_apple_card_token,
                MAX(i_merchant_token) i_merchant_card_token,     
                SUM(CASE WHEN card.account_owner != card.authorized_user THEN 1 ELSE 0 END) c_total_card_authorizeduser,
                SUM(CASE WHEN card.card_type = 'deposit' THEN 1 ELSE 0 END) c_total_deposit_card,
                SUM(CASE WHEN card.card_type = 'debit' THEN 1 ELSE 0 END) c_total_debit_card,
                SUM(CASE WHEN card.card_type = 'credit' THEN 1 ELSE 0 END) c_total_credit_card,
                SUM(CASE WHEN card.send_method_card = 310 THEN 1 ELSE 0 END) c_card_deliverbranch,
                SUM(CASE WHEN card.send_method_pin = 210 THEN 1 ELSE 0 END) c_cardpin_deliverbranch
            FROM pm_owner.card card
            WHERE month_id = to_char(process_dt, 'YYYYMM')
            GROUP BY card.account_owner, card.month_id) card_owner
            ON main.customerid = card_owner.customerid
            
        LEFT JOIN (
            SELECT
                card.month_id month_id,
                card.authorized_user customerid,
                -- actions attributes
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) i_opened_card_holder_m,       
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card.card_type = 'deposit' THEN 1 ELSE 0 END) i_opened_deposit_card_holder_m,    
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card.card_type = 'debit' THEN 1 ELSE 0 END) i_opened_debit_card_holder_m, 
                MAX(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card.card_type = 'credit' THEN 1 ELSE 0 END) i_opened_credit_card_holder_m,
                SUM(CASE WHEN (card.create_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) c_opened_card_holder_m,       
                -- status attributes   
                SUM(CASE WHEN card.card_status_cd in (12, 13) THEN 1 ELSE 0 END) c_card_holder,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND i_retail_account = 1 THEN 1 ELSE 0 END) c_card_retail_holder,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND i_retail_account = 0 THEN 1 ELSE 0 END) c_card_nonretail_holder,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND card.card_type = 'deposit' THEN 1 ELSE 0 END) c_deposit_card_holder,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND card.card_type = 'debit' THEN 1 ELSE 0 END) c_debit_card_holder,
                SUM(CASE WHEN card.card_status_cd in (12, 13) AND card.card_type = 'credit' THEN 1 ELSE 0 END) c_credit_card_holder,   
                MIN(card.card_tenure) min_card_tenure_holder,
                MAX(card.card_tenure) max_card_tenure_holder,
                LEAST(MIN(card.card_expiration_time), process_dt - process_dt_hist) min_card_expirationtime_holder,
                LEAST(MAX(card.card_expiration_time), process_dt - process_dt_hist) max_card_expirationtime_holder,   
                ROUND(AVG(card.i_automatic_renewal), 2) ratio_card_renewal_holder,   
                SUM(CASE WHEN card.card_status_cd not in (12, 13) THEN 1 ELSE 0 END) c_nonactive_card_holder,
                LEAST(MIN(card.online_card_limit),
                    MIN(card.atm_card_limit),
                    MIN(card.pos_card_limit),
                    MIN(card.cnp_card_limit)) min_card_limit_holder,
                GREATEST(MAX(card.online_card_limit),
                    MAX(card.atm_card_limit),
                    MAX(card.pos_card_limit),
                    MAX(card.cnp_card_limit)) max_card_limit_holder, 
                SUM(card.i_travel_insurance) c_card_travel_ins_holder,  
                ROUND(AVG(card.i_travel_insurance), 2) i_ratio_card_travel_ins_holder,
                SUM(c_token) c_card_token_holder,
                MAX(i_googlepay_token) i_google_card_token_holder,
                MAX(i_applepay_token) i_apple_card_token_holder,
                MAX(i_merchant_token) i_merchant_card_token_holder,
                SUM(CASE WHEN card.send_method_card = 310 THEN 1 ELSE 0 END) c_card_deliverbranch_holder,
                SUM(CASE WHEN card.send_method_pin = 210 THEN 1 ELSE 0 END) c_cardpin_deliverbranch_holder
            FROM pm_owner.card card
            WHERE 1=1
                AND month_id = to_char(process_dt, 'YYYYMM')
                AND card.account_owner != card.authorized_user
            GROUP BY card.authorized_user, card.month_id) card_holder
            ON main.customerid = card_holder.customerid
           
        LEFT JOIN (
            SELECT
                card_expired.month_id month_id,
                card_expired.account_owner customerid,        
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) i_expired_card_m,
                SUM(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) c_expired_card_m,
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) 
                        AND card_expired.account_owner != card_expired.authorized_user THEN 1 ELSE 0 END) i_expired_card_authorizeduser_m, 
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card_expired.card_type = 'deposit' THEN 1 ELSE 0 END) i_expired_deposit_card_m,
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card_expired.card_type = 'debit' THEN 1 ELSE 0 END) i_expired_debit_card_m,
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card_expired.card_type = 'credit' THEN 1 ELSE 0 END) i_expired_credit_card_m,
                        
                COUNT(1) c_expired_card,   
                SUM(CASE WHEN card_expired.account_owner = card_expired.authorized_user THEN 1 ELSE 0 END) c_expired_card_owner,
                MAX(CASE WHEN card_expired.account_owner != card_expired.authorized_user THEN 1 ELSE 0 END) i_expired_card_authorizeduser,
                SUM(CASE WHEN card_expired.account_owner != card_expired.authorized_user THEN 1 ELSE 0 END) c_expired_card_authorizeduser,        
                SUM(CASE WHEN card_expired.card_type = 'deposit' THEN 1 ELSE 0 END) c_deposit_expired_card,
                SUM(CASE WHEN card_expired.card_type = 'debit' THEN 1 ELSE 0 END) c_debit_expired_card,
                SUM(CASE WHEN card_expired.card_type = 'credit' THEN 1 ELSE 0 END) c_credit_expired_card,
                LEAST(MIN(card_expired.time_card_expired), process_dt - process_dt_hist) recency_card_expired  
            FROM pm_owner.card_expired card_expired
            WHERE card_expired.month_id = to_char(process_dt, 'YYYYMM')
            GROUP BY card_expired.account_owner, card_expired.month_id) expcard_owner
            ON main.customerid = expcard_owner.customerid       
        
        LEFT JOIN (
            SELECT
                card_expired.month_id month_id,
                card_expired.authorized_user customerid,
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) i_expired_card_holder_m,
                SUM(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) c_expired_card_holder_m,
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card_expired.card_type = 'deposit' THEN 1 ELSE 0 END) i_expired_deposit_cardholder_m,
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card_expired.card_type = 'debit' THEN 1 ELSE 0 END) i_expired_debit_card_holder_m,
                MAX(CASE WHEN (card_expired.expiration_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) AND card_expired.card_type = 'credit' THEN 1 ELSE 0 END) i_expired_credit_card_holder_m,
                        
                COUNT(1) c_expired_card_holder,
                SUM(card_expired.i_retail_account) c_expired_card_retail_holder,
                COUNT(1) - SUM(card_expired.i_retail_account) c_expired_card_noretail_holder,        
                SUM(CASE WHEN card_expired.card_type = 'deposit' THEN 1 ELSE 0 END) c_deposit_expired_card_holder,
                SUM(CASE WHEN card_expired.card_type = 'debit' THEN 1 ELSE 0 END) c_debit_expired_card_holder,
                SUM(CASE WHEN card_expired.card_type = 'credit' THEN 1 ELSE 0 END) c_credit_expired_card_holder,
                LEAST(MIN(card_expired.time_card_expired), process_dt - process_dt_hist) recency_card_expired_holder
            FROM pm_owner.card_expired card_expired
            WHERE 1=1
            AND month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))
            AND card_expired.account_owner != card_expired.authorized_user
            GROUP BY card_expired.authorized_user, card_expired.month_id) expcard_holder
            ON main.customerid = expcard_holder.customerid
            
        LEFT JOIN (
            SELECT
                customerid,
                COUNT(DISTINCT month_id) + 1 c_month_id,
                MAX(i_opened_card_m) i_opened_card_q,
                SUM(c_opened_card_m) c_opened_card_q,
                MAX(i_opened_card_authorizeduser_m) i_opened_card_authorizeduser_q,
                MAX(i_opened_deposit_card_m) i_opened_deposit_card_q,    
                MAX(i_opened_debit_card_m) i_opened_debit_card_q, 
                MAX(i_opened_credit_card_m) i_opened_credit_card_q,
                MAX(i_changed_card_status_m) i_changed_card_status_q,
                MAX(i_changed_card_limit_m) i_changed_card_limit_q,    
                SUM(c_changed_card_limit_m) c_changed_card_limit_q,
                MAX(i_opened_card_holder_m) i_opened_card_holder_q,    
                MAX(i_opened_deposit_card_holder_m) i_opened_deposit_card_holder_q,    
                MAX(i_opened_debit_card_holder_m) i_opened_debit_card_holder_q, 
                MAX(i_opened_credit_card_holder_m) i_opened_credit_card_holder_q,
                SUM(c_opened_card_holder_m) c_opened_card_holder_q,        
                MAX(i_expired_card_m) i_expired_card_q,
                SUM(c_expired_card_m) c_expired_card_q,
                MAX(i_expired_card_authorizeduser_m) i_expired_card_authorizeduser_q,
                MAX(i_expired_deposit_card_m) i_expired_deposit_card_q,
                MAX(i_expired_debit_card_m) i_expired_debit_card_q,
                MAX(i_expired_credit_card_m) i_expired_credit_card_q,        
                MAX(i_expired_card_holder_m) i_expired_card_holder_q,
                SUM(c_expired_card_holder_m) c_expired_card_holder_q,
                MAX(i_expired_deposit_cardholder_m) i_expired_deposit_cardholder_q,
                MAX(i_expired_debit_card_holder_m) i_expired_debit_card_holder_q,
                MAX(i_expired_credit_card_holder_m) i_expired_credit_card_holder_q
            
            FROM pm_owner.attr_card
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -2), 'YYYYMM'))
                           AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) card_q
            ON main.customerid = card_q.customerid
        
        LEFT JOIN (SELECT
                customerid,
                COUNT(DISTINCT month_id) + 1 c_month_id,
                MAX(i_opened_card_m) i_opened_card_y,
                SUM(c_opened_card_m) c_opened_card_y,
                MAX(i_opened_card_authorizeduser_m) i_opened_card_authorizeduser_y,
                MAX(i_opened_deposit_card_m) i_opened_deposit_card_y,    
                MAX(i_opened_debit_card_m) i_opened_debit_card_y, 
                MAX(i_opened_credit_card_m) i_opened_credit_card_y,
                MAX(i_changed_card_status_m) i_changed_card_status_y,
                MAX(i_changed_card_limit_m) i_changed_card_limit_y,    
                SUM(c_changed_card_limit_m) c_changed_card_limit_y,
                MAX(i_opened_card_holder_m) i_opened_card_holder_y,    
                MAX(i_opened_deposit_card_holder_m) i_opened_deposit_card_holder_y,    
                MAX(i_opened_debit_card_holder_m) i_opened_debit_card_holder_y, 
                MAX(i_opened_credit_card_holder_m) i_opened_credit_card_holder_y,
                SUM(c_opened_card_holder_m) c_opened_card_holder_y,        
                MAX(i_expired_card_m) i_expired_card_y,
                SUM(c_expired_card_m) c_expired_card_y,
                MAX(i_expired_card_authorizeduser_m) i_expired_card_authorizeduser_y,
                MAX(i_expired_deposit_card_m) i_expired_deposit_card_y,
                MAX(i_expired_debit_card_m) i_expired_debit_card_y,
                MAX(i_expired_credit_card_m) i_expired_credit_card_y,        
                MAX(i_expired_card_holder_m) i_expired_card_holder_y,
                SUM(c_expired_card_holder_m) c_expired_card_holder_y,
                MAX(i_expired_deposit_cardholder_m) i_expired_deposit_cardholder_y,
                MAX(i_expired_debit_card_holder_m) i_expired_debit_card_holder_y,
                MAX(i_expired_credit_card_holder_m) i_expired_credit_card_holder_y
            
            FROM pm_owner.attr_card
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -11), 'YYYYMM'))
                           AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) card_y
            ON main.customerid = card_y.customerid;
   
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);
    
    END;

  PROCEDURE append_BANK_ACCOUNT_DETAIL(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_bank_account_detail';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'BANK_ACCOUNT_DETAIL';

  BEGIN
	
	-- BANK ACCOUNT DETAIL
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    INSERT INTO pm_owner.bank_account_detail
        SELECT
            to_char(process_dt, 'YYYYMM') month_id,
            pr.party_id customerid,
            prd.prod_id,
            c.src_syst_cd,
            prd.prod_src_val,
            prd.prod_src_key,
            cp.cont_id,
            pn1.prod_nm_sk prod_name,
            cg.curr_cd currency_cd,
            curr.curr_src_val currency_nm,
            cg.cont_gen_open_dt open_dt,
            cg.cont_gen_close_dt close_dt,
            cg.cont_gen_matur_dt maturity_dt,
            COALESCE(crc.cred_cont_first_drw_dt, to_date('9999-12-31', 'YYYY-MM-DD')) draw_loan_dt,
            CASE WHEN cg.CONT_GEN_MATUR_DT = DATE '9999-12-31' OR cg.CONT_GEN_MATUR_DT <= process_dt THEN 0
                WHEN cg.CONT_GEN_TERM_DAY_CNT >= 0 THEN cg.CONT_GEN_TERM_DAY_CNT
                ELSE cg.CONT_GEN_MATUR_DT - process_dt END c_days_to_maturity,
            COALESCE(crc.cred_cont_annuity_fix_am, 0) LOAN_PAYMENT,
            COALESCE(cl1.cont_lmt_am, 0) CONTRACT_LOAN_LIMIT,
            COALESCE(cl2.cont_lmt_am, 0) DRAWN_LOAN,
            CASE WHEN COALESCE(cl2.cont_lmt_am, 0) > 0 THEN 1 ELSE 0 END I_DRAWN,
            COALESCE(ci.cont_intr_final_rate_pct, 0) INTEREST_RATE,
            -- interest info
            COALESCE(cic.cont_intr_last_dt, CG.CONT_GEN_MATUR_DT) INTEREST_FIX_DT, -- Datum poslednej obnovy, Pri novej produkcii totozny s datumom otvorenia
            COALESCE(cic.cont_intr_next_dt, CG.CONT_GEN_MATUR_DT) INTEREST_FIX_END_DT, -- Datum najblizsej obnovy    
            COALESCE (cic.cont_intr_next_dt - process_dt, 0) c_days_interest_fix_end,        
            COALESCE(PD.C_PARTY, 0) C_CODEBTOR,
            process_dt - COALESCE((CASE WHEN CRC.CRED_CONT_FIRST_DRW_DT = DATE '1900-01-01' THEN null ELSE CRC.CRED_CONT_FIRST_DRW_DT END), CG.CONT_GEN_OPEN_DT) ACCOUNT_TENURE,
            CASE WHEN CG.CONT_GEN_MATUR_DT = DATE '9999-12-31' THEN 0
                 WHEN ROUND(months_between(CG.CONT_GEN_MATUR_DT, CG.CONT_GEN_OPEN_DT)/12, 2) = 100 THEN 0
                 ELSE greatest(CG.CONT_GEN_MATUR_DT - process_dt, 0) END ACCOUNT_MATURITY,   
            COALESCE(CB.PAYBACK_PRINCIPAL, 0) PAYBACK_PRINCIPAL,
            -- dept
            COALESCE(CB.PRINCIPAL_DEPT, 0) PRINCIPAL_DEPT,     --účetní jistina po splatnosti
            CB.TOTAL_DEPT,
            SUBSTR(CI.cont_intr_chng_fq,1,REGEXP_INSTR(CI.cont_intr_chng_fq, 'Y|M|D|Q')) FIX_PERIOD_TEXT,
            CASE WHEN SUBSTR(ci.cont_intr_chng_fq,1,REGEXP_INSTR(ci.cont_intr_chng_fq, 'Y|M|D|Q')) like '%Y' THEN round(to_number(regexp_replace(SUBSTR(ci.cont_intr_chng_fq,1,REGEXP_INSTR(CI.cont_intr_chng_fq, 'Y|M|D|Q')), '[^0-9]', '')), 2)
                WHEN SUBSTR(ci.cont_intr_chng_fq,1,REGEXP_INSTR(ci.cont_intr_chng_fq, 'Y|M|D|Q')) like '%M' THEN round(to_number(regexp_replace(SUBSTR(ci.cont_intr_chng_fq,1,REGEXP_INSTR(CI.cont_intr_chng_fq, 'Y|M|D|Q')), '[^0-9]', ''))/12, 2)
                WHEN SUBSTR(ci.cont_intr_chng_fq,1,REGEXP_INSTR(ci.cont_intr_chng_fq, 'Y|M|D|Q')) like '%Q' THEN round(to_number(regexp_replace(SUBSTR(ci.cont_intr_chng_fq,1,REGEXP_INSTR(CI.cont_intr_chng_fq, 'Y|M|D|Q')), '[^0-9]', ''))/4, 2)
                WHEN SUBSTR(ci.cont_intr_chng_fq,1,REGEXP_INSTR(ci.cont_intr_chng_fq, 'Y|M|D|Q')) like '%D' THEN round(to_number(regexp_replace(SUBSTR(ci.cont_intr_chng_fq,1,REGEXP_INSTR(CI.cont_intr_chng_fq, 'Y|M|D|Q')), '[^0-9]', ''))/365, 2)
                ELSE 0 END FIX_PERIOD_IN_YEAR,             --Vyjadrenie fixacie   
            COALESCE(CB.END_OF_MONTH_BALANCE, 0) END_OF_MONTH_BALANCE,
            COALESCE(CB.MAX_BALANCE, 0) MAX_BALANCE,
            COALESCE(CB.AVG_BALANCE, 0) AVG_BALANCE,
            COALESCE(CB.MIN_BALANCE, 0) MIN_BALANCE,
            COALESCE(CB.CREDIT_TURNOVER, 0) sum_credit_turnover,
            COALESCE(CB.C_CREDIT_TURNOVER, 0) C_CREDIT_TURNOVER
        
        FROM L0_OWNER.prod PRD 
        
        JOIN L0_OWNER.CONT_PROD CP
            ON  CP.prod_id = PRD.prod_id
            AND CP.cont_prod_start_dt <= process_dt
            AND CP.cont_prod_end_dt > process_dt

        JOIN L0_OWNER.CONT C
            ON  C.cont_id = CP.cont_id
            AND (C.cont_type_cd || C.cont_single_acct_in) IN ('3N','3Y','2Y')
            AND C.src_syst_cd in (501, 510, 520)

        JOIN L0_OWNER.PARTY_CONT PC
            ON  PC.cont_id = C.cont_id
            AND PC.party_cont_start_dt <= process_dt
            AND PC.party_cont_end_dt > process_dt
            AND CASE WHEN C.src_syst_cd in (501, 510) AND PC.party_role_cd IN (1) THEN 1
                     WHEN C.src_syst_cd in (520) AND PC.party_role_cd IN (1) THEN 1
                     ELSE 0 END = 1
        
        JOIN L0_OWNER.CONT_COST_CNTR CCC
            ON  CCC.cont_id = CP.CONT_ID
            AND CCC.cont_cost_cntr_start_dt <= process_dt
            AND CCC.cont_cost_cntr_end_dt > process_dt
            
        JOIN L0_OWNER.cost_cntr CC
            ON  CC.cost_cntr_cd = CCC.cost_cntr_cd
            AND CC.src_syst_cd in (120, 510, 520)
            AND CC.cost_cntr_type_cd = case when CC.src_syst_cd = 510 then 5
                                            when CC.src_syst_cd = 520 then 7
                                       else 14 end
        
        JOIN L0_OWNER.PARTY_RLTD PR
            ON  PR.child_party_id = PC.party_id
            AND PR.party_rltd_start_dt <= process_dt
            AND PR.party_rltd_end_dt > process_dt
            AND PR.party_rltd_rsn_cd = 4 -- customerid

        JOIN L0_OWNER.CONT_STAT CST
            ON  CST.cont_id = C.cont_id
            AND CST.cont_stat_start_dt <= process_dt
            AND CST.cont_stat_end_dt > process_dt
            AND CASE WHEN CST.cont_stat_type_cd IN (0,2) AND C.src_syst_cd in (501, 510) THEN 1
                     WHEN CST.cont_stat_type_cd IN (0) AND C.src_syst_cd in (520) THEN 1 ELSE 0 END = 1
        
        JOIN L0_OWNER.CONT_GEN CG
            ON  CG.cont_id = C.cont_id
            AND CG.cont_gen_start_dt <= process_dt
            AND CG.cont_gen_open_dt <= process_dt
            -----	NOVA PRODUKCIA T24 s PREDCISLIM 200002 sa berie od polky marca 2022 (kvoli test datam na PROD T24), MIGROVANE OTP DO T24 sa beru vsetky
            AND CASE
                    WHEN C.SRC_SYST_CD = 501 AND substr(C.CONT_ACCT, 1, 6) = '200002' AND CG.cont_gen_open_dt < date '2022-03-14' THEN 0
                    ELSE 1
                END = 1 -- Zmena R.Nichta 21.7.2022 (Zmena kvoli odfiltrovavanym spotrebakom, ktore sa po migracii stracali v reportoch)      
            AND CG.cont_gen_close_dt > process_dt
            AND CG.cont_gen_end_dt > process_dt  
            
        LEFT JOIN L0_OWNER.CURR
            ON	CURR.curr_cd = CG.curr_cd
        
        LEFT JOIN L0_OWNER.PROD_NM PN1
            ON  PN1.prod_id = CP.prod_id
            AND PN1.prod_nm_start_dt <= process_dt
            AND PN1.prod_nm_end_dt > process_dt
        
        LEFT JOIN L0_OWNER.CONT_LMT CL1
            ON  CL1.cont_id = CP.cont_id
            AND CL1.lmt_type_cd IN (1)
            AND CL1.cont_lmt_start_dt <= process_dt
            AND CL1.cont_lmt_end_dt > process_dt
        
        LEFT JOIN L0_OWNER.CONT_LMT CL2
            ON  CL2.cont_id = CP.cont_id
            AND CL2.lmt_type_cd IN (2)
            AND CL2.cont_lmt_start_dt <= process_dt
            AND CL2.cont_lmt_end_dt > process_dt
            
        LEFT JOIN L0_OWNER.CONT_INTR CI
            ON CI.cont_id = CP.cont_id
            AND CI.cont_intr_start_dt <= process_dt
            AND CI.cont_intr_end_dt > process_dt
            AND CI.meas_cd = 4000 -- added 20250207
            
        LEFT JOIN L0_OWNER.CRED_CONT CRC
            ON CRC.cont_id = CP.cont_id
            AND CRC.cred_cont_start_dt <= process_dt
            AND CRC.cred_cont_end_dt > process_dt
        
        LEFT JOIN
        (
            SELECT
                PC.cont_id,
                COUNT(PC.party_id) as C_PARTY
            FROM L0_OWNER.PARTY_CONT PC
            WHERE
                PC.party_cont_start_dt <= process_dt
                AND PC.party_cont_end_dt > process_dt
                AND PC.party_role_cd = 64 --Codebtor
            GROUP BY
                PC.cont_id
        ) PD ON PD.cont_id=CP.cont_id
        
        LEFT JOIN L0_OWNER.CONT_INTR_CHNG CIC   -- PRIDANE 2016-02-15
            ON  CIC.cont_id = CP.cont_id
            AND CIC.meas_cd = 4000
            AND CIC.cont_intr_start_dt <= process_dt
            AND CIC.cont_intr_end_dt > process_dt
        
        LEFT JOIN (
            SELECT
                C.CONT_ID,
                MAX(CASE WHEN CB.cont_bal_dt = process_dt and CB.meas_cd = 39 and CB.cont_bal_bc_am is not null THEN ABS(CB.cont_bal_bc_am) ELSE 0 END) PAYBACK_PRINCIPAL,
                MAX(CASE WHEN CB.cont_bal_dt = process_dt and CB.meas_cd = 1 and CB.cont_bal_bc_am is not null THEN ABS(CB.cont_bal_bc_am) ELSE 0 END) PRINCIPAL_DEPT,
                MAX(CASE WHEN CB.meas_cd = 1 AND CB.cont_bal_dt = process_dt THEN ABS(CB.cont_bal_bc_am) ELSE 0 END) +
                MAX(CASE WHEN CB.meas_cd = 2 AND CB.cont_bal_dt = process_dt THEN ABS(CB.cont_bal_bc_am) ELSE 0 END) +
                MAX(CASE WHEN CB.meas_cd = 3 AND CB.cont_bal_dt = process_dt THEN ABS(CB.cont_bal_bc_am) ELSE 0 END) +
                MAX(CASE WHEN CB.meas_cd = 6 AND CB.cont_bal_dt = process_dt THEN ABS(CB.cont_bal_bc_am) ELSE 0 END) +
                MAX(CASE WHEN CB.meas_cd = 9 AND CB.cont_bal_dt = process_dt THEN ABS(CB.cont_bal_bc_am) ELSE 0 END) TOTAL_DEPT,
                
                MAX(CASE WHEN CB.meas_cd = 0 AND CB.cont_bal_dt = process_dt AND CB.cont_bal_bc_am IS NOT NULL 
                         THEN CB.cont_bal_bc_am ELSE NULL END) END_OF_MONTH_BALANCE,
                MAX(CASE WHEN CB.meas_cd = 0 AND CB.cont_bal_dt BETWEEN last_day(add_months(process_dt, -1))+1 AND process_dt
                         THEN CB.cont_bal_bc_am ELSE NULL END) MAX_BALANCE,
                ROUND(AVG(CASE WHEN CB.meas_cd = 0 AND CB.cont_bal_dt BETWEEN last_day(add_months(process_dt, -1))+1 AND process_dt
                         THEN CB.cont_bal_bc_am ELSE NULL END), 2) AVG_BALANCE,
                MIN(CASE WHEN CB.meas_cd = 0 AND CB.cont_bal_dt BETWEEN last_day(add_months(process_dt, -1))+1 AND process_dt
                         THEN CB.cont_bal_bc_am ELSE NULL END) MIN_BALANCE,
                SUM(CASE WHEN CB.meas_cd = 1060 AND CB.cont_bal_dt BETWEEN last_day(add_months(process_dt, -1))+1 AND process_dt
                         THEN CB.cont_bal_bc_am ELSE 0 END) CREDIT_TURNOVER,
                SUM(CASE WHEN CB.meas_cd = 1061 AND CB.cont_bal_dt BETWEEN last_day(add_months(process_dt, -1))+1 AND process_dt
                         THEN CB.cont_bal_bc_am ELSE 0 END) C_CREDIT_TURNOVER
            
            FROM L0_OWNER.CONT C
            
            LEFT JOIN L0_OWNER.CONT_BAL CB
                ON  CB.cont_id = C.cont_id
                AND CB.meas_cd IN (0, 1, 2, 3, 6, 9, 39, 1060, 1061)
                AND CB.cont_bal_dt <= process_dt
                AND CB.cont_bal_dt > add_months(process_dt, -1)
                
            GROUP BY
                C.CONT_ID) CB
            ON  CB.cont_id = CP.cont_id
        
        WHERE 1=1  
        AND ((prd.src_syst_cd in (120,510,501) and prd.prod_type_cd = 1)
              or
             (prd.src_syst_cd = 520 and prd.prod_type_cd = 2));

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);

    END;

  PROCEDURE append_bank_account_closed(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_bank_account_closed';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'BANK_ACCOUNT_CLOSED';

  BEGIN
	
	-- BANK ACCOUNT CLOSED
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    INSERT INTO pm_owner.bank_account_closed
        SELECT
            to_char(process_dt, 'YYYYMM') month_id,
            PR.PARTY_ID customerid,
            PRD.PROD_ID,
            C.SRC_SYST_CD,
            PRD.PROD_SRC_VAL,
            PRD.PROD_SRC_KEY,
            CP.CONT_ID,
            PN1.prod_nm_sk PROD_NAME,
            CG.curr_cd currency_cd,
            CURR.curr_src_val currency_nm,
            CG.CONT_GEN_OPEN_DT OPEN_DT,
            CG.CONT_GEN_CLOSE_DT CLOSE_DT,
            CG.CONT_GEN_MATUR_DT MATURITY_DT,    
            (process_dt - CG.CONT_GEN_CLOSE_DT) C_DAYS_SINCE_CLOSING,
            CASE WHEN months_between(CG.CONT_GEN_MATUR_DT, CG.CONT_GEN_OPEN_DT)/12 != 90
                AND CG.CONT_GEN_MATUR_DT != DATE '9999-12-31'
                AND CG.CONT_GEN_CLOSE_DT < CG.CONT_GEN_MATUR_DT
                THEN 1 ELSE 0 END I_PREMATURE_REPAY,   
            COALESCE(CRC.cred_cont_annuity_fix_am, 0) LOAN_PAYMENT,
            COALESCE(CL1.cont_lmt_am, 0) CONTRACT_LOAN_LIMIT,
            COALESCE(CL2.cont_lmt_am, 0) DRAWN_LOAN
            
        FROM L0_OWNER.prod PRD 
        
        JOIN L0_OWNER.CONT_PROD CP
            ON  CP.prod_id = PRD.prod_id
            AND CP.cont_prod_start_dt <= process_dt
            AND CP.cont_prod_end_dt > process_dt
       
        JOIN L0_OWNER.CONT C
            ON  C.cont_id = CP.cont_id
            AND (C.cont_type_cd || C.cont_single_acct_in) IN ('3N','3Y','2Y')
            AND C.src_syst_cd in (501, 510, 520)

        JOIN L0_OWNER.PARTY_CONT PC
            ON  PC.cont_id = C.cont_id
            AND PC.party_cont_start_dt <= process_dt
            AND PC.party_cont_end_dt > process_dt
            AND CASE WHEN C.src_syst_cd in (501, 510) AND PC.party_role_cd IN (1) THEN 1
                     WHEN C.src_syst_cd in (520) AND PC.party_role_cd IN (1) THEN 1
                     ELSE 0 END = 1

        JOIN L0_OWNER.CONT_STAT CST
            ON  CST.cont_id = C.cont_id
            AND CST.cont_stat_start_dt <= process_dt
            AND CST.cont_stat_end_dt > process_dt
            AND CASE WHEN CST.cont_stat_type_cd IN (4,5) AND C.src_syst_cd in (501, 510) THEN 1
                     WHEN CST.cont_stat_type_cd IN (4,5) AND C.src_syst_cd in (520) THEN 1 ELSE 0 END = 1
        
        JOIN L0_OWNER.CONT_COST_CNTR CCC
            ON  CCC.cont_id = CP.cont_id
            AND CCC.cont_cost_cntr_start_dt <= process_dt
            AND CCC.cont_cost_cntr_end_dt > process_dt
            
        JOIN L0_OWNER.cost_cntr CC
            ON  CC.cost_cntr_cd = CCC.cost_cntr_cd
            AND CC.src_syst_cd in (120, 510, 520)
            AND CC.cost_cntr_type_cd = case when CC.src_syst_cd = 510 then 5
                                            when CC.src_syst_cd = 520 then 7
                                       else 14 end
        
        JOIN L0_OWNER.PARTY_RLTD PR
            ON  PR.child_party_id = PC.party_id
            AND PR.party_rltd_start_dt <= process_dt
            AND PR.party_rltd_end_dt > process_dt
            AND PR.party_rltd_rsn_cd = 4 -- customerid
        
        JOIN L0_OWNER.CONT_GEN CG
            ON  CG.cont_id = C.cont_id
            AND CG.cont_gen_start_dt <= process_dt
            AND CG.cont_gen_open_dt <= process_dt
            -----	NOVA PRODUKCIA T24 s PREDCISLIM 200002 sa berie od polky marca 2022 (kvoli test datam na PROD T24), MIGROVANE OTP DO T24 sa beru vsetky
            AND CASE
                    WHEN C.SRC_SYST_CD = 501 AND substr(C.CONT_ACCT, 1, 6) = '200002' AND CG.cont_gen_open_dt < date '2022-03-14' THEN 0
                    ELSE 1
                END = 1 -- Zmena R.Nichta 21.7.2022 (Zmena kvoli odfiltrovavanym spotrebakom, ktore sa po migracii stracali v reportoch)        
            AND CG.cont_gen_close_dt <= process_dt
            AND CG.cont_gen_close_dt > process_dt_hist
            AND CG.cont_gen_end_dt > process_dt
        
        LEFT JOIN L0_OWNER.CURR
            ON	CURR.curr_cd = CG.curr_cd
        
        LEFT JOIN L0_OWNER.PROD_NM PN1
            ON  PN1.prod_id = CP.prod_id
            AND PN1.prod_nm_start_dt <= process_dt
            AND PN1.prod_nm_end_dt > process_dt
        
        LEFT JOIN L0_OWNER.CONT_LMT CL1
            ON  CL1.cont_id = CP.cont_id
            AND CL1.lmt_type_cd IN (1)
            AND CL1.cont_lmt_start_dt <= process_dt
            AND CL1.cont_lmt_end_dt > process_dt
        
        LEFT JOIN L0_OWNER.CONT_LMT CL2
            ON  CL2.cont_id = CP.cont_id
            AND CL2.lmt_type_cd IN (2)
            AND CL2.cont_lmt_start_dt <= process_dt
            AND CL2.cont_lmt_end_dt > process_dt
            
        LEFT JOIN L0_OWNER.CRED_CONT CRC
            ON CRC.cont_id = CP.cont_id
            AND CRC.cred_cont_start_dt <= process_dt
            AND CRC.cred_cont_end_dt > process_dt
        
        WHERE 1=1  
        AND ((prd.src_syst_cd in (120,510,501) and prd.prod_type_cd = 1)
              or
             (prd.src_syst_cd = 520 and prd.prod_type_cd = 2));

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);

    END;

  PROCEDURE append_attr_bank_account(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_attr_bank_account';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_BANK_ACCOUNT';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);

  BEGIN
    
    -- ATTR_bank_account
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);
             

    INSERT INTO PM_OWNER.ATTR_BANK_ACCOUNT
        SELECT
           main.month_id month_id,
           main.customerid customerid,
           
           COALESCE(bank.c_classic_dda, 0) c_classic_dda,
           COALESCE(bank.c_student_dda, 0) c_student_dda,
           COALESCE(bank.c_child_dda, 0) c_child_dda,
           COALESCE(bank.c_foreign_exchange_dda, 0) c_foreign_exchange_dda,
           COALESCE(bank.c_basic_dda, 0) c_basic_dda,
           COALESCE(bank.c_smart_dda, 0) c_smart_dda,		   
           COALESCE(bank.c_pohoda_dda, 0) c_pohoda_dda,	   
           COALESCE(bank.c_private_dda, 0) c_private_dda,
           COALESCE(bank.c_investment_account, 0) c_investment_account,
           COALESCE(bank.c_business_dda, 0) c_business_dda,
           COALESCE(bank.c_blocking_account, 0) c_blocking_account,
           COALESCE(bank.c_mortgage, 0) c_mortgage,
           COALESCE(bank.c_consumer_loan, 0) c_consumer_loan,
           COALESCE(bank.c_overdraft, 0) c_overdraft,
           COALESCE(bank.c_credit_card_account, 0) c_credit_card_account,
           COALESCE(bank.c_business_overdraft, 0) c_business_overdraft,
           COALESCE(bank.c_investment_loan, 0) c_investment_loan,
           COALESCE(bank.c_business_credit_card_account, 0) c_business_credit_card_account,
           COALESCE(bank.c_renovo_loan, 0) c_renovo_loan,
           COALESCE(bank.c_municipality_loan, 0) c_municipality_loan,           
           
           COALESCE(bank.c_dda, 0) c_dda,
           COALESCE(bank.c_retail_package_dda, 0) c_retail_package_dda,
           COALESCE(bank.c_nonretail_package_dda, 0) c_nonretail_package_dda,
           COALESCE(bank.c_special_account, 0) c_special_account,
           COALESCE(bank.c_cash_pooling, 0) c_cash_pooling,
           COALESCE(bank.c_savings_account, 0) c_savings_account,
           COALESCE(bank.c_term_deposit, 0) c_term_deposit,
           COALESCE(bank.c_retail_loan, 0) c_retail_loan,
           COALESCE(bank.c_business_loan, 0) c_business_loan,
           COALESCE(bank.c_bank_guarantee, 0) c_bank_guarantee,
           COALESCE(bank.c_forfaiting, 0) c_forfaiting,
           
           COALESCE(bank.c_package_and_dda, 0) c_package_and_dda,
           COALESCE(bank.c_savings_and_term_deposit, 0) c_savings_and_term_deposit,
           COALESCE(bank.c_loan, 0) c_loan,
           COALESCE(bank.c_business_finance, 0) c_business_finance,
           
           COALESCE(bank.c_total_account, 0) c_total_account,
       
           COALESCE(bank.min_tenure_package_and_dda, 0) min_tenure_package_and_dda,
           COALESCE(bank.min_tenure_savings, 0) min_tenure_savings,
           COALESCE(bank.min_tenure_loan, 0) min_tenure_loan,
           COALESCE(bank.min_tenure_business_finance, 0) min_tenure_business_finance,
           
           COALESCE(bank.max_tenure_package_and_dda, 0) max_tenure_package_and_dda,
           COALESCE(bank.max_tenure_saving, 0) max_tenure_saving,
           COALESCE(bank.max_tenure_loan, 0) max_tenure_loan,
           COALESCE(bank.max_tenure_business_finance, 0) max_tenure_business_finance,
           
           COALESCE(bank.min_maturity_term_deposit, 0) min_maturity_term_deposit,
           COALESCE(bank.min_maturity_loan, 0) min_maturity_loan,
           COALESCE(bank.min_maturity_mortgage, 0) min_maturity_mortgage,
           
           COALESCE(bank.max_maturity_term_deposit, 0) max_maturity_term_deposit,
           COALESCE(bank.max_maturity_loan, 0) max_maturity_loan,
           COALESCE(bank.max_maturity_mortgage, 0) max_maturity_mortgage,
           
           COALESCE(bank.sum_loan_installment, 0) sum_loan_installment,
           COALESCE(bank.i_drawn_loan, 0) i_drawn_loan,
       
           COALESCE(bank.volume_drawn_loan, 0) volume_drawn_loan,
           COALESCE(bank.volume_drawn_regular_loan, 0) volume_drawn_regular_loan,
           COALESCE(bank.volume_drawn_mortgage, 0) volume_drawn_mortgage,
       
           COALESCE(bank.avg_loan_interest_rate, 0) avg_loan_interest_rate,
           COALESCE(bank.avg_mortgage_interest_rate, 0) avg_mortgage_interest_rate,
           COALESCE(bank.min_time_mortgage_interest_fix, 0) min_time_mortgage_interest_fix,
           COALESCE(bank.min_time_loan_interest_fix, 0) min_time_loan_interest_fix,
           COALESCE(bank.i_codebtor, 0) i_codebtor,
           COALESCE(bank.ratio_mortgage_payback, 0) ratio_mortgage_payback,
           COALESCE(bank.ratio_loan_payback, 0) ratio_loan_payback,
           COALESCE(bank.sum_total_debt, 0) sum_total_debt,
       
           COALESCE(bank.sum_dda_balance, 0) sum_dda_balance,
           COALESCE(bank.sum_savings_balance, 0) sum_savings_balance,
           COALESCE(bank.sum_balance, 0) sum_balance,
           COALESCE(bank.sum_loaned_money, 0) sum_loaned_money,
           COALESCE(bank.sum_total_balance, 0) sum_total_balance,
       
           COALESCE(bank.avg_dda_balance_m, 0) avg_dda_balance_m,
           COALESCE(bank.avg_savings_balance_m, 0) avg_savings_balance_m,
           COALESCE(bank.avg_balance_m, 0) avg_balance_m,
           COALESCE(bank.avg_loaned_money_m, 0) avg_loaned_money_m,
           COALESCE(bank.avg_total_balance_m, 0) avg_total_balance_m,
       
           COALESCE(bank.max_min_dda_balance_m, 0) max_min_dda_balance_m,
           COALESCE(bank.max_min_savings_balance_m, 0) max_min_savings_balance_m,
           COALESCE(bank.max_min_balance_m, 0) max_min_balance_m,
       
           COALESCE(bank.sum_credit_turnover_m, 0) sum_credit_turnover_m,
           COALESCE(bank.c_credit_turnover_m, 0) c_credit_turnover_m,
       
           COALESCE(bank.i_opened_total_account_m, 0) i_opened_total_account_m,
           COALESCE(bank.i_opened_package_and_dda_m, 0) i_opened_package_and_dda_m,
           COALESCE(bank.i_opened_savings_account_m, 0) i_opened_savings_account_m,
           COALESCE(bank.i_opened_term_deposit_m, 0) i_opened_term_deposit_m,
           COALESCE(bank.i_opened_loan_m, 0) i_opened_loan_m,
           COALESCE(bank.i_opened_mortgage_m, 0) i_opened_mortgage_m,
           COALESCE(bank.i_opened_business_finance_m, 0) i_opened_business_finance_m,
           COALESCE(bank.i_refix_interest_m, 0) i_refix_interest_m,
           
       -- agg
           GREATEST(COALESCE(bank_q.i_opened_total_account_q, 0), COALESCE(bank.i_opened_total_account_m, 0)) i_opened_total_account_q,
           GREATEST(COALESCE(bank_q.i_opened_package_and_dda_q, 0), COALESCE(bank.i_opened_package_and_dda_m, 0)) i_opened_package_and_dda_q,
           GREATEST(COALESCE(bank_q.i_opened_savings_account_q, 0), COALESCE(bank.i_opened_savings_account_m, 0)) i_opened_savings_account_q,
           GREATEST(COALESCE(bank_q.i_opened_term_deposit_q, 0), COALESCE(bank.i_opened_term_deposit_m, 0)) i_opened_term_deposit_q,
           GREATEST(COALESCE(bank_q.i_opened_loan_q, 0), COALESCE(bank.i_opened_loan_m, 0)) i_opened_loan_q,
           GREATEST(COALESCE(bank_q.i_opened_mortgage_q, 0), COALESCE(bank.i_opened_mortgage_m, 0)) i_opened_mortgage_q,
           GREATEST(COALESCE(bank_q.i_opened_business_finance_q, 0), COALESCE(bank.i_opened_business_finance_m, 0)) i_opened_business_finance_q,
           GREATEST(COALESCE(bank_q.i_refix_interest_q, 0), COALESCE(bank.i_refix_interest_m, 0)) i_refix_interest_q,
       
           (COALESCE(bank_q.avg_dda_balance_q, 0) + COALESCE(bank.sum_dda_balance, 0))/main.c_month_q avg_dda_balance_q,
           (COALESCE(bank_q.avg_savings_balance_q, 0) + COALESCE(bank.sum_savings_balance, 0))/main.c_month_q avg_savings_balance_q,
           (COALESCE(bank_q.avg_balance_q, 0) + COALESCE(bank.sum_balance, 0))/main.c_month_q avg_balance_q,
           (COALESCE(bank_q.avg_loaned_money_q, 0) + COALESCE(bank.sum_loaned_money, 0))/main.c_month_q avg_loaned_money_q,
           (COALESCE(bank_q.avg_total_balance_q, 0) + COALESCE(bank.sum_total_balance, 0))/main.c_month_q avg_total_balance_q,
           
           (COALESCE(bank_q.avg_total_debt_q, 0) + COALESCE(bank.sum_total_debt, 0))/main.c_month_q avg_total_debt_q,
           (COALESCE(bank_q.avg_credit_turnover_q, 0) + COALESCE(bank.sum_credit_turnover_m, 0))/main.c_month_q avg_credit_turnover_q,
           (COALESCE(bank_q.c_credit_turnover_q, 0) + COALESCE(bank.c_credit_turnover_m, 0))/main.c_month_q c_credit_turnover_q,
           
           COALESCE(bank.sum_dda_balance, 0) - COALESCE(bank_q.sum_dda_balance_lag2, 0) diff_dda_balance_q,
           COALESCE(bank.sum_savings_balance, 0) - COALESCE(bank_q.sum_savings_balance_lag2, 0) diff_savings_balance_q,
           COALESCE(bank.sum_balance, 0) - COALESCE(bank_q.sum_balance_lag2, 0) diff_balance_q,
           COALESCE(bank.sum_loaned_money, 0) - COALESCE(bank_q.sum_loaned_money_lag2, 0) diff_loaned_money_q,
           COALESCE(bank.sum_total_balance, 0) - COALESCE(bank_q.sum_total_balance_lag2, 0) diff_total_balance_q,
           
           COALESCE(bank.sum_total_debt, 0) - COALESCE(bank_q.sum_total_debt_lag2, 0) diff_total_debt_q,
           COALESCE(bank.sum_credit_turnover_m, 0) - COALESCE(bank_q.sum_credit_turnover_lag2, 0) diff_credit_turnover_q,
           COALESCE(bank.c_credit_turnover_m, 0) - COALESCE(bank_q.c_credit_turnover_lag2, 0) diff_c_credit_turnover_q,
        
           (abs(COALESCE(bank.sum_dda_balance, 0) - COALESCE(bank_q.sum_dda_balance_lag1, 0)) + abs(COALESCE(bank_q.sum_dda_balance_lag1, 0) - COALESCE(bank_q.sum_dda_balance_lag2, 0)))
           /GREATEST(main.c_month_q-1,1) sum_change_dda_balance_q,
           (abs(COALESCE(bank.sum_savings_balance, 0) - COALESCE(bank_q.sum_savings_balance_lag1, 0)) + abs(COALESCE(bank_q.sum_savings_balance_lag1, 0) - COALESCE(bank_q.sum_savings_balance_lag2, 0)))
           /GREATEST(main.c_month_q-1,1) sum_change_savings_balance_q,
           (abs(COALESCE(bank.sum_balance, 0) - COALESCE(bank_q.sum_balance_lag1, 0)) + abs(COALESCE(bank_q.sum_balance_lag1, 0) - COALESCE(bank_q.sum_balance_lag2, 0)))
           /GREATEST(main.c_month_q-1,1) sum_change_balance_q,
           (abs(COALESCE(bank.sum_loaned_money, 0) - COALESCE(bank_q.sum_loaned_money_lag1, 0)) + abs(COALESCE(bank_q.sum_loaned_money_lag1, 0) - COALESCE(bank_q.sum_loaned_money_lag2, 0)))
           /GREATEST(main.c_month_q-1,1) sum_change_loaned_money_q,
           (abs(COALESCE(bank.sum_total_balance, 0) - COALESCE(bank_q.sum_total_balance_lag1, 0)) + abs(COALESCE(bank_q.sum_total_balance_lag1, 0) - COALESCE(bank_q.sum_total_balance_lag2, 0)))
           /GREATEST(main.c_month_q-1,1) sum_change_total_balance_q,
           
           (abs(COALESCE(bank.sum_total_debt, 0) - COALESCE(bank_q.sum_total_debt_lag1, 0)) + abs(COALESCE(bank_q.sum_total_debt_lag1, 0) - COALESCE(bank_q.sum_total_debt_lag2, 0)))
           /GREATEST(main.c_month_q-1,1) sum_change_total_debt_q,
           (abs(COALESCE(bank.sum_credit_turnover_m, 0) - COALESCE(bank_q.sum_credit_turnover_lag1, 0)) + abs(COALESCE(bank_q.sum_credit_turnover_lag1, 0) - COALESCE(bank_q.sum_credit_turnover_lag2, 0)))
           /GREATEST(main.c_month_q-1,1) sum_change_credit_turnover_q,
           (abs(COALESCE(bank.c_credit_turnover_m, 0) - COALESCE(bank_q.c_credit_turnover_lag1, 0)) + abs(COALESCE(bank_q.c_credit_turnover_lag1, 0) - COALESCE(bank_q.c_credit_turnover_lag2, 0)))
           /GREATEST(main.c_month_q-1,1) sum_change_c_credit_turnover_q,  
       
           GREATEST(COALESCE(bank_y.i_opened_total_account_y, 0), COALESCE(bank.i_opened_total_account_m, 0)) i_opened_total_account_y,
           GREATEST(COALESCE(bank_y.i_opened_package_and_dda_y, 0), COALESCE(bank.i_opened_package_and_dda_m, 0)) i_opened_package_and_dda_y,
           GREATEST(COALESCE(bank_y.i_opened_savings_account_y, 0), COALESCE(bank.i_opened_savings_account_m, 0)) i_opened_savings_account_y,
           GREATEST(COALESCE(bank_y.i_opened_term_deposit_y, 0), COALESCE(bank.i_opened_term_deposit_m, 0)) i_opened_term_deposit_y,
           GREATEST(COALESCE(bank_y.i_opened_loan_y, 0), COALESCE(bank.i_opened_loan_m, 0)) i_opened_loan_y,
           GREATEST(COALESCE(bank_y.i_opened_mortgage_y, 0), COALESCE(bank.i_opened_mortgage_m, 0)) i_opened_mortgage_y,
           GREATEST(COALESCE(bank_y.i_opened_business_finance_y, 0), COALESCE(bank.i_opened_business_finance_m, 0)) i_opened_business_finance_y,
           GREATEST(COALESCE(bank_y.i_refix_interest_y, 0), COALESCE(bank.i_refix_interest_m, 0)) i_refix_interest_y,
       
           (COALESCE(bank_y.avg_dda_balance_y, 0) + COALESCE(bank.sum_dda_balance, 0))/main.c_month_y avg_dda_balance_y,
           (COALESCE(bank_y.avg_savings_balance_y, 0) + COALESCE(bank.sum_savings_balance, 0))/main.c_month_y avg_savings_balance_y,
           (COALESCE(bank_y.avg_balance_y, 0) + COALESCE(bank.sum_balance, 0))/main.c_month_y avg_balance_y,
           (COALESCE(bank_y.avg_loaned_money_y, 0) + COALESCE(bank.sum_loaned_money, 0))/main.c_month_y avg_loaned_money_y,
           (COALESCE(bank_y.avg_total_balance_y, 0) + COALESCE(bank.sum_total_balance, 0))/main.c_month_y avg_total_balance_y,
           
           (COALESCE(bank_y.avg_total_debt_y, 0) + COALESCE(bank.sum_total_debt, 0))/main.c_month_y avg_total_debt_y,
           (COALESCE(bank_y.avg_credit_turnover_y, 0) + COALESCE(bank.sum_credit_turnover_m, 0))/main.c_month_y avg_credit_turnover_y,
           (COALESCE(bank_y.c_credit_turnover_y, 0) + COALESCE(bank.c_credit_turnover_m, 0))/main.c_month_y c_credit_turnover_y,
           
           COALESCE(bank.sum_dda_balance, 0) - COALESCE(bank_y.sum_dda_balance_lag11, 0) diff_dda_balance_y,
           COALESCE(bank.sum_savings_balance, 0) - COALESCE(bank_y.sum_savings_balance_lag11, 0) diff_savings_balance_y,
           COALESCE(bank.sum_balance, 0) - COALESCE(bank_y.sum_balance_lag11, 0) diff_balance_y,
           COALESCE(bank.sum_loaned_money, 0) - COALESCE(bank_y.sum_loaned_money_lag11, 0) diff_loaned_money_y,
           COALESCE(bank.sum_total_balance, 0) - COALESCE(bank_y.sum_total_balance_lag11, 0) diff_total_balance_y,
           
           COALESCE(bank.sum_total_debt, 0) - COALESCE(bank_y.sum_total_debt_lag11, 0) diff_total_debt_y,
           COALESCE(bank.sum_credit_turnover_m, 0) - COALESCE(bank_y.sum_credit_turnover_lag11, 0) diff_avg_credit_turnover_y,
           COALESCE(bank.c_credit_turnover_m, 0) - COALESCE(bank_y.c_credit_turnover_lag11, 0) diff_c_credit_turnover_y,
            
           (abs(COALESCE(bank.sum_dda_balance, 0) - COALESCE(bank_y.sum_dda_balance_lag1, 0)) + abs(COALESCE(bank_y.sum_dda_balance_lag1, 0) - COALESCE(bank_y.sum_dda_balance_lag2, 0))
           + abs(COALESCE(bank_y.sum_dda_balance_lag2, 0) - COALESCE(bank_y.sum_dda_balance_lag3, 0)) + abs(COALESCE(bank_y.sum_dda_balance_lag3, 0) - COALESCE(bank_y.sum_dda_balance_lag4, 0))
           + abs(COALESCE(bank_y.sum_dda_balance_lag4, 0) - COALESCE(bank_y.sum_dda_balance_lag5, 0)) + abs(COALESCE(bank_y.sum_dda_balance_lag5, 0) - COALESCE(bank_y.sum_dda_balance_lag6, 0))
           + abs(COALESCE(bank_y.sum_dda_balance_lag6, 0) - COALESCE(bank_y.sum_dda_balance_lag7, 0)) + abs(COALESCE(bank_y.sum_dda_balance_lag7, 0) - COALESCE(bank_y.sum_dda_balance_lag8, 0))
           + abs(COALESCE(bank_y.sum_dda_balance_lag8, 0) - COALESCE(bank_y.sum_dda_balance_lag9, 0)) + abs(COALESCE(bank_y.sum_dda_balance_lag9, 0) - COALESCE(bank_y.sum_dda_balance_lag10, 0))
           + abs(COALESCE(bank_y.sum_dda_balance_lag10, 0) - COALESCE(bank_y.sum_dda_balance_lag11, 0)))/GREATEST(main.c_month_y-1,1) sum_change_dda_balance_y,
           (abs(COALESCE(bank.sum_savings_balance, 0) - COALESCE(bank_y.sum_savings_balance_lag1, 0)) + abs(COALESCE(bank_y.sum_savings_balance_lag1, 0) - COALESCE(bank_y.sum_savings_balance_lag2, 0))
           + abs(COALESCE(bank_y.sum_savings_balance_lag2, 0) - COALESCE(bank_y.sum_savings_balance_lag3, 0)) + abs(COALESCE(bank_y.sum_savings_balance_lag3, 0) - COALESCE(bank_y.sum_savings_balance_lag4, 0))
           + abs(COALESCE(bank_y.sum_savings_balance_lag4, 0) - COALESCE(bank_y.sum_savings_balance_lag5, 0)) + abs(COALESCE(bank_y.sum_savings_balance_lag5, 0) - COALESCE(bank_y.sum_savings_balance_lag6, 0))
           + abs(COALESCE(bank_y.sum_savings_balance_lag6, 0) - COALESCE(bank_y.sum_savings_balance_lag7, 0)) + abs(COALESCE(bank_y.sum_savings_balance_lag7, 0) - COALESCE(bank_y.sum_savings_balance_lag8, 0))
           + abs(COALESCE(bank_y.sum_savings_balance_lag8, 0) - COALESCE(bank_y.sum_savings_balance_lag9, 0)) + abs(COALESCE(bank_y.sum_savings_balance_lag9, 0) - COALESCE(bank_y.sum_savings_balance_lag10, 0))
           + abs(COALESCE(bank_y.sum_savings_balance_lag10, 0) - COALESCE(bank_y.sum_savings_balance_lag11, 0)))/GREATEST(main.c_month_y-1,1) sum_change_savings_balance_y,
           (abs(COALESCE(bank.sum_balance, 0) - COALESCE(bank_y.sum_balance_lag1, 0)) + abs(COALESCE(bank_y.sum_balance_lag1, 0) - COALESCE(bank_y.sum_balance_lag2, 0))
           + abs(COALESCE(bank_y.sum_balance_lag2, 0) - COALESCE(bank_y.sum_balance_lag3, 0)) + abs(COALESCE(bank_y.sum_balance_lag3, 0) - COALESCE(bank_y.sum_balance_lag4, 0))
           + abs(COALESCE(bank_y.sum_balance_lag4, 0) - COALESCE(bank_y.sum_balance_lag5, 0)) + abs(COALESCE(bank_y.sum_balance_lag5, 0) - COALESCE(bank_y.sum_balance_lag6, 0))
           + abs(COALESCE(bank_y.sum_balance_lag6, 0) - COALESCE(bank_y.sum_balance_lag7, 0)) + abs(COALESCE(bank_y.sum_balance_lag7, 0) - COALESCE(bank_y.sum_balance_lag8, 0))
           + abs(COALESCE(bank_y.sum_balance_lag8, 0) - COALESCE(bank_y.sum_balance_lag9, 0)) + abs(COALESCE(bank_y.sum_balance_lag9, 0) - COALESCE(bank_y.sum_balance_lag10, 0))
           + abs(COALESCE(bank_y.sum_balance_lag10, 0) - COALESCE(bank_y.sum_balance_lag11, 0)))/GREATEST(main.c_month_y-1,1) sum_change_balance_y,
           (abs(COALESCE(bank.sum_loaned_money, 0) - COALESCE(bank_y.sum_loaned_money_lag1, 0)) + abs(COALESCE(bank_y.sum_loaned_money_lag1, 0) - COALESCE(bank_y.sum_loaned_money_lag2, 0))
           + abs(COALESCE(bank_y.sum_loaned_money_lag2, 0) - COALESCE(bank_y.sum_loaned_money_lag3, 0)) + abs(COALESCE(bank_y.sum_loaned_money_lag3, 0) - COALESCE(bank_y.sum_loaned_money_lag4, 0))
           + abs(COALESCE(bank_y.sum_loaned_money_lag4, 0) - COALESCE(bank_y.sum_loaned_money_lag5, 0)) + abs(COALESCE(bank_y.sum_loaned_money_lag5, 0) - COALESCE(bank_y.sum_loaned_money_lag6, 0))
           + abs(COALESCE(bank_y.sum_loaned_money_lag6, 0) - COALESCE(bank_y.sum_loaned_money_lag7, 0)) + abs(COALESCE(bank_y.sum_loaned_money_lag7, 0) - COALESCE(bank_y.sum_loaned_money_lag8, 0))
           + abs(COALESCE(bank_y.sum_loaned_money_lag8, 0) - COALESCE(bank_y.sum_loaned_money_lag9, 0)) + abs(COALESCE(bank_y.sum_loaned_money_lag9, 0) - COALESCE(bank_y.sum_loaned_money_lag10, 0))
           + abs(COALESCE(bank_y.sum_loaned_money_lag10, 0) - COALESCE(bank_y.sum_loaned_money_lag11, 0)))/GREATEST(main.c_month_y-1,1) sum_change_loaned_money_y,
           (abs(COALESCE(bank.sum_total_balance, 0) - COALESCE(bank_y.sum_total_balance_lag1, 0)) + abs(COALESCE(bank_y.sum_total_balance_lag1, 0) - COALESCE(bank_y.sum_total_balance_lag2, 0))
           + abs(COALESCE(bank_y.sum_total_balance_lag2, 0) - COALESCE(bank_y.sum_total_balance_lag3, 0)) + abs(COALESCE(bank_y.sum_total_balance_lag3, 0) - COALESCE(bank_y.sum_total_balance_lag4, 0))
           + abs(COALESCE(bank_y.sum_total_balance_lag4, 0) - COALESCE(bank_y.sum_total_balance_lag5, 0)) + abs(COALESCE(bank_y.sum_total_balance_lag5, 0) - COALESCE(bank_y.sum_total_balance_lag6, 0))
           + abs(COALESCE(bank_y.sum_total_balance_lag6, 0) - COALESCE(bank_y.sum_total_balance_lag7, 0)) + abs(COALESCE(bank_y.sum_total_balance_lag7, 0) - COALESCE(bank_y.sum_total_balance_lag8, 0))
           + abs(COALESCE(bank_y.sum_total_balance_lag8, 0) - COALESCE(bank_y.sum_total_balance_lag9, 0)) + abs(COALESCE(bank_y.sum_total_balance_lag9, 0) - COALESCE(bank_y.sum_total_balance_lag10, 0))
           + abs(COALESCE(bank_y.sum_total_balance_lag10, 0) - COALESCE(bank_y.sum_total_balance_lag11, 0)))/GREATEST(main.c_month_y-1,1) sum_change_total_balance_y,
       
           (abs(COALESCE(bank.sum_total_debt, 0) - COALESCE(bank_y.sum_total_debt_lag1, 0)) + abs(COALESCE(bank_y.sum_total_debt_lag1, 0) - COALESCE(bank_y.sum_total_debt_lag2, 0))
           + abs(COALESCE(bank_y.sum_total_debt_lag2, 0) - COALESCE(bank_y.sum_total_debt_lag3, 0)) + abs(COALESCE(bank_y.sum_total_debt_lag3, 0) - COALESCE(bank_y.sum_total_debt_lag4, 0))
           + abs(COALESCE(bank_y.sum_total_debt_lag4, 0) - COALESCE(bank_y.sum_total_debt_lag5, 0)) + abs(COALESCE(bank_y.sum_total_debt_lag5, 0) - COALESCE(bank_y.sum_total_debt_lag6, 0))
           + abs(COALESCE(bank_y.sum_total_debt_lag6, 0) - COALESCE(bank_y.sum_total_debt_lag7, 0)) + abs(COALESCE(bank_y.sum_total_debt_lag7, 0) - COALESCE(bank_y.sum_total_debt_lag8, 0))
           + abs(COALESCE(bank_y.sum_total_debt_lag8, 0) - COALESCE(bank_y.sum_total_debt_lag9, 0)) + abs(COALESCE(bank_y.sum_total_debt_lag9, 0) - COALESCE(bank_y.sum_total_debt_lag10, 0))
           + abs(COALESCE(bank_y.sum_total_debt_lag10, 0) - COALESCE(bank_y.sum_total_debt_lag11, 0)))/GREATEST(main.c_month_y-1,1) sum_change_total_debt_y,
           (abs(COALESCE(bank.sum_credit_turnover_m, 0) - COALESCE(bank_y.sum_credit_turnover_lag1, 0)) + abs(COALESCE(bank_y.sum_credit_turnover_lag1, 0) - COALESCE(bank_y.sum_credit_turnover_lag2, 0))
           + abs(COALESCE(bank_y.sum_credit_turnover_lag2, 0) - COALESCE(bank_y.sum_credit_turnover_lag3, 0)) + abs(COALESCE(bank_y.sum_credit_turnover_lag3, 0) - COALESCE(bank_y.sum_credit_turnover_lag4, 0))
           + abs(COALESCE(bank_y.sum_credit_turnover_lag4, 0) - COALESCE(bank_y.sum_credit_turnover_lag5, 0)) + abs(COALESCE(bank_y.sum_credit_turnover_lag5, 0) - COALESCE(bank_y.sum_credit_turnover_lag6, 0))
           + abs(COALESCE(bank_y.sum_credit_turnover_lag6, 0) - COALESCE(bank_y.sum_credit_turnover_lag7, 0)) + abs(COALESCE(bank_y.sum_credit_turnover_lag7, 0) - COALESCE(bank_y.sum_credit_turnover_lag8, 0))
           + abs(COALESCE(bank_y.sum_credit_turnover_lag8, 0) - COALESCE(bank_y.sum_credit_turnover_lag9, 0)) + abs(COALESCE(bank_y.sum_credit_turnover_lag9, 0) - COALESCE(bank_y.sum_credit_turnover_lag10, 0))
           + abs(COALESCE(bank_y.sum_credit_turnover_lag10, 0) - COALESCE(bank_y.sum_credit_turnover_lag11, 0)))/GREATEST(main.c_month_y-1,1) sum_change_credit_turnover_y,
           (abs(COALESCE(bank.c_credit_turnover_m, 0) - COALESCE(bank_y.c_credit_turnover_lag1, 0)) + abs(COALESCE(bank_y.c_credit_turnover_lag1, 0) - COALESCE(bank_y.c_credit_turnover_lag2, 0))
           + abs(COALESCE(bank_y.c_credit_turnover_lag2, 0) - COALESCE(bank_y.c_credit_turnover_lag3, 0)) + abs(COALESCE(bank_y.c_credit_turnover_lag3, 0) - COALESCE(bank_y.c_credit_turnover_lag4, 0))
           + abs(COALESCE(bank_y.c_credit_turnover_lag4, 0) - COALESCE(bank_y.c_credit_turnover_lag5, 0)) + abs(COALESCE(bank_y.c_credit_turnover_lag5, 0) - COALESCE(bank_y.c_credit_turnover_lag6, 0))
           + abs(COALESCE(bank_y.c_credit_turnover_lag6, 0) - COALESCE(bank_y.c_credit_turnover_lag7, 0)) + abs(COALESCE(bank_y.c_credit_turnover_lag7, 0) - COALESCE(bank_y.c_credit_turnover_lag8, 0))
           + abs(COALESCE(bank_y.c_credit_turnover_lag8, 0) - COALESCE(bank_y.c_credit_turnover_lag9, 0)) + abs(COALESCE(bank_y.c_credit_turnover_lag9, 0) - COALESCE(bank_y.c_credit_turnover_lag10, 0))
           + abs(COALESCE(bank_y.c_credit_turnover_lag10, 0) - COALESCE(bank_y.c_credit_turnover_lag11, 0)))/GREATEST(main.c_month_y-1,1) sum_change_c_credit_turnover_y,
           
           COALESCE(bank_closed.c_closed_account, 0) c_closed_account,
           COALESCE(bank_closed.c_closed_package_and_dda, 0) c_closed_package_and_dda,
           COALESCE(bank_closed.c_closed_saving, 0) c_closed_saving,
           COALESCE(bank_closed.c_closed_loan, 0) c_closed_loan,
           COALESCE(bank_closed.c_closed_regular_loan, 0) c_closed_regular_loan,
           COALESCE(bank_closed.c_closed_mortgage, 0) c_closed_mortgage,
           COALESCE(bank_closed.c_closed_credit_card_account, 0) c_closed_credit_card_account,
           COALESCE(bank_closed.c_closed_business_finance, 0) c_closed_business_finance,
       
           COALESCE(bank_closed.c_early_paid_off_loan, 0) c_early_paid_off_loan,
           COALESCE(bank_closed.volume_closed_loan, 0) volume_closed_loan,
           COALESCE(bank_closed.volume_closed_regular_loan, 0) volume_closed_regular_loan,
           COALESCE(bank_closed.volume_closed_mortgage, 0) volume_closed_mortgage,
           COALESCE(bank_closed.recency_dda_closed, 0) recency_dda_closed,
           COALESCE(bank_closed.recency_savings_closed, 0) recency_savings_closed,
           COALESCE(bank_closed.recency_loan_closed, 0) recency_loan_closed,
           COALESCE(bank_closed.recency_businessfinance_closed, 0) recency_businessfinance_closed,
       
           COALESCE(bank_closed.maxtime_dda_closed, 0) maxtime_dda_closed,
           COALESCE(bank_closed.maxtime_savings_closed, 0) maxtime_savings_closed,
           COALESCE(bank_closed.maxtime_loan_closed, 0) maxtime_loan_closed,
           COALESCE(bank_closed.maxtime_businessfinance_closed, 0) maxtime_businessfinance_closed,
       
           COALESCE(bank_closed.i_closed_total_account, 0) i_closed_total_account,
           COALESCE(bank_closed.i_closed_package_and_dda, 0) i_closed_package_and_dda,
           COALESCE(bank_closed.i_closed_savings_account, 0) i_closed_savings_account,
           COALESCE(bank_closed.i_closed_term_deposit, 0) i_closed_term_deposit,
           COALESCE(bank_closed.i_closed_loan, 0) i_closed_loan,
           COALESCE(bank_closed.i_closed_mortgage, 0) i_closed_mortgage,
           COALESCE(bank_closed.i_closed_business_finance, 0) i_closed_business_finance,
       
           GREATEST(COALESCE(bank_q.i_closed_total_account_q, 0), COALESCE(bank_closed.i_closed_total_account, 0)) i_closed_total_account_q,
           GREATEST(COALESCE(bank_q.i_closed_package_and_dda_q, 0), COALESCE(bank_closed.i_closed_package_and_dda, 0)) i_closed_package_and_dda_q,
           GREATEST(COALESCE(bank_q.i_closed_savings_account_q, 0), COALESCE(bank_closed.i_closed_savings_account, 0)) i_closed_savings_account_q,
           GREATEST(COALESCE(bank_q.i_closed_term_deposit_q, 0), COALESCE(bank_closed.i_closed_term_deposit, 0)) i_closed_term_deposit_q,
           GREATEST(COALESCE(bank_q.i_closed_loan_q, 0), COALESCE(bank_closed.i_closed_loan, 0)) i_closed_loan_q,
           GREATEST(COALESCE(bank_q.i_closed_mortgage_q, 0), COALESCE(bank_closed.i_closed_mortgage, 0)) i_closed_mortgage_q,
           GREATEST(COALESCE(bank_q.i_closed_business_finance_q, 0), COALESCE(bank_closed.i_closed_business_finance, 0)) i_closed_business_finance_q,
           
           GREATEST(COALESCE(bank_y.i_closed_total_account_y, 0), COALESCE(bank_closed.i_closed_total_account, 0)) i_closed_total_account_y,
           GREATEST(COALESCE(bank_y.i_closed_package_and_dda_y, 0), COALESCE(bank_closed.i_closed_package_and_dda, 0)) i_closed_package_and_dda_y,
           GREATEST(COALESCE(bank_y.i_closed_savings_account_y, 0), COALESCE(bank_closed.i_closed_savings_account, 0)) i_closed_savings_account_y,
           GREATEST(COALESCE(bank_y.i_closed_term_deposit_y, 0), COALESCE(bank_closed.i_closed_term_deposit, 0)) i_closed_term_deposit_y,
           GREATEST(COALESCE(bank_y.i_closed_loan_y, 0), COALESCE(bank_closed.i_closed_loan, 0)) i_closed_loan_y,
           GREATEST(COALESCE(bank_y.i_closed_mortgage_y, 0), COALESCE(bank_closed.i_closed_mortgage, 0)) i_closed_mortgage_y,
           GREATEST(COALESCE(bank_y.i_closed_business_finance_y, 0), COALESCE(bank_closed.i_closed_business_finance, 0)) i_closed_business_finance_y
       
       FROM (
           SELECT
               base.month_id,
               base.customerid,
               base.bank_tenure,
               LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -3)),1)*3,
                     COALESCE(bank_q.c_month, 0)+1) c_month_q,
               LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -12)),1)*12,
                     COALESCE(bank_y.c_month, 0)+1) c_month_y       
           FROM pm_owner.attr_client_base base
           LEFT JOIN (
               SELECT
                   customerid,
                   COUNT(DISTINCT month_id) c_month
               FROM pm_owner.attr_bank_account
               WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-2), 'YYYYMM'))
               AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
               GROUP BY customerid) bank_q
               ON bank_q.customerid = base.customerid
               
           LEFT JOIN (
               SELECT
                   customerid,
                   COUNT(DISTINCT month_id) c_month
               FROM pm_owner.attr_bank_account
               WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-11), 'YYYYMM'))
               AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
               GROUP BY customerid) bank_y
               ON bank_y.customerid = base.customerid                   
           WHERE base.month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))) main
       
       LEFT JOIN (
           SELECT
               bank.month_id,
               bank.customerid,
               
               -- status attributes
               SUM(CASE WHEN bank.lvl4_category = 1111 THEN 1 ELSE 0 END) c_classic_dda,
               SUM(CASE WHEN bank.lvl4_category = 1112 THEN 1 ELSE 0 END) c_student_dda,
               SUM(CASE WHEN bank.lvl4_category = 1113 THEN 1 ELSE 0 END) c_child_dda,
               SUM(CASE WHEN bank.lvl4_category = 1114 THEN 1 ELSE 0 END) c_foreign_exchange_dda,
               SUM(CASE WHEN bank.lvl4_category = 1115 THEN 1 ELSE 0 END) c_basic_dda,
               SUM(CASE WHEN bank.lvl4_category = 1129 THEN 1 ELSE 0 END) c_smart_dda,
               SUM(CASE WHEN bank.lvl4_category in (1121, 1122) THEN 1 ELSE 0 END) c_pohoda_dda,
               SUM(CASE WHEN bank.lvl4_category in (1123, 1124) THEN 1 ELSE 0 END) c_private_dda,
               SUM(CASE WHEN bank.lvl4_category = 1125 THEN 1 ELSE 0 END) c_investment_account,
               SUM(CASE WHEN bank.lvl4_category in (1126, 1127, 1128) THEN 1 ELSE 0 END) c_business_dda,
               SUM(CASE WHEN bank.lvl4_category = 1132 THEN 1 ELSE 0 END) c_blocking_account,
               SUM(CASE WHEN bank.lvl4_category = 1511 THEN 1 ELSE 0 END) c_mortgage,
               SUM(CASE WHEN bank.lvl4_category = 1512 THEN 1 ELSE 0 END) c_consumer_loan,
               SUM(CASE WHEN bank.lvl4_category = 1513 THEN 1 ELSE 0 END) c_overdraft,
               SUM(CASE WHEN bank.lvl4_category = 1514 THEN 1 ELSE 0 END) c_credit_card_account,
               SUM(CASE WHEN bank.lvl3_category = 1920 THEN 1 ELSE 0 END) c_business_overdraft,
               SUM(CASE WHEN bank.lvl3_category = 1940 THEN 1 ELSE 0 END) c_investment_loan,
               SUM(CASE WHEN bank.lvl3_category in (1960) THEN 1 ELSE 0 END) c_business_credit_card_account,
               SUM(CASE WHEN bank.lvl3_category = 1930 THEN 1 ELSE 0 END) c_renovo_loan,
               SUM(CASE WHEN bank.lvl3_category = 1950 THEN 1 ELSE 0 END) c_municipality_loan,
           
               SUM(CASE WHEN bank.lvl3_category = 1110 THEN 1 ELSE 0 END) c_dda,
               SUM(CASE WHEN bank.lvl3_category = 1120 THEN 1 ELSE 0 END) c_retail_package_dda,
               SUM(CASE WHEN bank.lvl3_category = 1160 THEN 1 ELSE 0 END) c_nonretail_package_dda,
               SUM(CASE WHEN bank.lvl3_category = 1130 THEN 1 ELSE 0 END) c_special_account,
               SUM(CASE WHEN bank.lvl3_category = 1140 THEN 1 ELSE 0 END) c_cash_pooling,
               SUM(CASE WHEN bank.lvl3_category = 1410 THEN 1 ELSE 0 END) c_savings_account,
               SUM(CASE WHEN bank.lvl3_category = 1420 THEN 1 ELSE 0 END) c_term_deposit,
               SUM(CASE WHEN bank.lvl3_category in (1550, 1570, 1560, 1580) THEN 1 ELSE 0 END) c_retail_loan,
               SUM(CASE WHEN bank.lvl3_category in (1950, 1920, 1930, 1940) THEN 1 ELSE 0 END) c_business_loan,
               SUM(CASE WHEN bank.lvl3_category = 1610 THEN 1 ELSE 0 END) c_bank_guarantee,
               SUM(CASE WHEN bank.lvl3_category = 1690 THEN 1 ELSE 0 END) c_forfaiting,
           
               SUM(CASE WHEN bank.lvl2_category = 1100 THEN 1 ELSE 0 END) c_package_and_dda,
               SUM(CASE WHEN bank.lvl2_category = 1400 THEN 1 ELSE 0 END) c_savings_and_term_deposit,
               SUM(CASE WHEN bank.lvl2_category = 1500 THEN 1 ELSE 0 END) c_loan,
               SUM(CASE WHEN bank.lvl2_category = 1600 THEN 1 ELSE 0 END) c_business_finance,
               
               count(*) c_total_account,
           
               LEAST(COALESCE(MIN(CASE WHEN bank.lvl2_category = 1100 THEN account_tenure ELSE NULL END), 0), process_dt - process_dt_hist) min_tenure_package_and_dda,
               LEAST(COALESCE(MIN(CASE WHEN bank.lvl2_category = 1400 THEN account_tenure ELSE NULL END), 0), process_dt - process_dt_hist) min_tenure_savings,
               LEAST(COALESCE(MIN(CASE WHEN bank.lvl2_category = 1500 THEN account_tenure ELSE NULL END), 0), process_dt - process_dt_hist) min_tenure_loan,
               LEAST(COALESCE(MIN(CASE WHEN bank.lvl2_category = 1600 THEN account_tenure ELSE NULL END), 0), process_dt - process_dt_hist) min_tenure_business_finance, 
           
               LEAST(COALESCE(MAX(CASE WHEN bank.lvl2_category = 1100 THEN account_tenure ELSE NULL END), 0), process_dt - process_dt_hist) max_tenure_package_and_dda,
               LEAST(COALESCE(MAX(CASE WHEN bank.lvl2_category = 1400 THEN account_tenure ELSE NULL END), 0), process_dt - process_dt_hist) max_tenure_saving,
               LEAST(COALESCE(MAX(CASE WHEN bank.lvl2_category = 1500 THEN account_tenure ELSE NULL END), 0), process_dt - process_dt_hist) max_tenure_loan,
               LEAST(COALESCE(MAX(CASE WHEN bank.lvl2_category = 1600 THEN account_tenure ELSE NULL END), 0), process_dt - process_dt_hist) max_tenure_business_finance, 
           
           
           
               COALESCE(MIN(CASE WHEN bank.lvl3_category = 1420 AND bank.maturity_dt != DATE '9999-12-31'
                        THEN bank.maturity_dt - process_dt ELSE NULL END), 0) min_maturity_term_deposit,
               COALESCE(MIN(CASE WHEN bank.lvl3_category in (1940, 1950, 1560, 1580)
                        AND bank.draw_loan_dt <= process_dt_hist AND bank.maturity_dt >= process_dt_hist
                        THEN bank.maturity_dt - process_dt ELSE NULL END), 0) min_maturity_loan,
               COALESCE(MIN(CASE WHEN bank.lvl3_category in (1550, 1930) AND bank.draw_loan_dt <= process_dt_hist
                        THEN bank.maturity_dt - process_dt ELSE NULL END), 0) min_maturity_mortgage,
           
               COALESCE(MAX(CASE WHEN bank.lvl3_category = 1420 AND bank.maturity_dt != DATE '9999-12-31'
                        THEN bank.maturity_dt - process_dt ELSE NULL END), 0) max_maturity_term_deposit,
               COALESCE(MAX(CASE WHEN bank.lvl3_category in (1940, 1950, 1560, 1580)
                        AND bank.draw_loan_dt <= process_dt_hist AND bank.maturity_dt >= process_dt_hist
                        THEN bank.maturity_dt - process_dt ELSE NULL END), 0) max_maturity_loan,
               COALESCE(MAX(CASE WHEN bank.lvl3_category in (1550, 1930) AND bank.draw_loan_dt <= process_dt_hist
                        THEN bank.maturity_dt - process_dt ELSE NULL END), 0) max_maturity_mortgage,
                        
               SUM(bank.loan_payment) sum_loan_installment,
               MAX(bank.i_drawn) i_drawn_loan,
               
               SUM(bank.drawn_loan) volume_drawn_loan,
               SUM(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) THEN bank.drawn_loan ELSE 0 END) volume_drawn_regular_loan,
               SUM(CASE WHEN bank.lvl3_category in (1550, 1930) THEN bank.drawn_loan ELSE 0 END) volume_drawn_mortgage,
               
               SUM(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) THEN bank.interest_rate*bank.drawn_loan ELSE 0 END)/
                   GREATEST(SUM(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) THEN bank.drawn_loan ELSE 0 END), 1) avg_loan_interest_rate,  
               SUM(CASE WHEN bank.lvl3_category in (1550, 1930) THEN bank.interest_rate*bank.drawn_loan ELSE 0 END)/
                   GREATEST(SUM(CASE WHEN bank.lvl3_category in (1550, 1930) THEN bank.drawn_loan ELSE 0 END), 1) avg_mortgage_interest_rate,
               COALESCE(MIN(CASE WHEN bank.lvl3_category in (1550, 1930) THEN bank.c_days_interest_fix_end ELSE NULL END), 0) min_time_mortgage_interest_fix,    
               COALESCE(MIN(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) THEN bank.c_days_interest_fix_end ELSE NULL END), 0) min_time_loan_interest_fix,
               MAX(bank.c_codebtor) i_codebtor,
               SUM(CASE WHEN bank.lvl3_category in (1550, 1930) THEN bank.payback_principal ELSE 1 END)/
                   GREATEST(SUM(CASE WHEN bank.lvl3_category in (1550, 1930) THEN bank.drawn_loan ELSE 0 END), 1) ratio_mortgage_payback,
               SUM(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) THEN bank.payback_principal ELSE 1 END)/
                   GREATEST(SUM(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) THEN bank.drawn_loan ELSE 0 END), 1) ratio_loan_payback,
               SUM(bank.total_debt) sum_total_debt,
               
               SUM(CASE WHEN bank.lvl2_category in (1100) THEN bank.end_of_month_balance ELSE 0 END) sum_dda_balance,
               SUM(CASE WHEN bank.lvl2_category in (1400) THEN bank.end_of_month_balance ELSE 0 END) sum_savings_balance,
               SUM(CASE WHEN bank.lvl2_category in (1100, 1400) THEN bank.end_of_month_balance ELSE 0 END) sum_balance,
               SUM(CASE WHEN bank.lvl2_category in (1500, 1600) THEN bank.end_of_month_balance ELSE 0 END) sum_loaned_money,
               SUM(bank.end_of_month_balance) sum_total_balance,
           
               SUM(CASE WHEN bank.lvl2_category in (1100) THEN bank.avg_balance ELSE 0 END) avg_dda_balance_m,
               SUM(CASE WHEN bank.lvl2_category in (1400) THEN bank.avg_balance ELSE 0 END) avg_savings_balance_m,
               SUM(CASE WHEN bank.lvl2_category in (1100, 1400) THEN bank.avg_balance ELSE 0 END) avg_balance_m,
               SUM(CASE WHEN bank.lvl2_category in (1500, 1600) THEN bank.avg_balance ELSE 0 END) avg_loaned_money_m,
               SUM(bank.avg_balance) avg_total_balance_m,
           
               SUM(CASE WHEN bank.lvl2_category in (1100) THEN bank.max_balance ELSE 0 END)
                   - SUM(CASE WHEN bank.lvl2_category in (1100) THEN bank.min_balance ELSE 0 END) max_min_dda_balance_m,
               SUM(CASE WHEN bank.lvl2_category in (1400) THEN bank.max_balance ELSE 0 END)
                   - SUM(CASE WHEN bank.lvl2_category in (1400) THEN bank.min_balance ELSE 0 END) max_min_savings_balance_m,        
               SUM(CASE WHEN bank.lvl2_category in (1100, 1400) THEN bank.max_balance ELSE 0 END) 
                   - SUM(CASE WHEN bank.lvl2_category in (1100, 1400) THEN bank.min_balance ELSE 0 END) max_min_balance_m,
            
               SUM(bank.sum_credit_turnover) sum_credit_turnover_m,
               SUM(bank.c_credit_turnover) c_credit_turnover_m,
              
               -- actions attributes 
               MAX(CASE WHEN bank.open_dt > add_months(process_dt, -1) AND bank.open_dt <= process_dt THEN 1 ELSE 0 END) i_opened_total_account_m,    
               MAX(CASE WHEN bank.lvl2_category = 1100 AND bank.open_dt > add_months(process_dt, -1) AND bank.open_dt <= process_dt THEN 1 ELSE 0 END) i_opened_package_and_dda_m,
               MAX(CASE WHEN bank.lvl3_category = 1410 AND bank.open_dt > add_months(process_dt, -1) AND bank.open_dt <= process_dt THEN 1 ELSE 0 END) i_opened_savings_account_m,
               MAX(CASE WHEN bank.lvl3_category = 1420 AND bank.open_dt > add_months(process_dt, -1) AND bank.open_dt <= process_dt THEN 1 ELSE 0 END) i_opened_term_deposit_m,
               MAX(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) AND bank.open_dt > add_months(process_dt, -1) AND bank.open_dt <= process_dt THEN 1 ELSE 0 END) i_opened_loan_m,
               MAX(CASE WHEN bank.lvl3_category in (1550, 1930) AND bank.open_dt > add_months(process_dt, -1) AND bank.open_dt <= process_dt THEN 1 ELSE 0 END) i_opened_mortgage_m,
               MAX(CASE WHEN bank.lvl2_category = 1600 AND open_dt > add_months(process_dt, -1) AND bank.open_dt <= process_dt THEN 1 ELSE 0 END) i_opened_business_finance_m,
               MAX(CASE WHEN bank.interest_fix_dt > add_months(process_dt, -1) AND bank.interest_fix_dt <= process_dt THEN 1 ELSE 0 END) i_refix_interest_m 
           FROM (
               SELECT
                   bank.*, 
                   cat.lvl4_category,
                   cat.lvl4_category_dsc,
                   cat.lvl3_category,
                   cat.lvl3_category_dsc,
                   cat.lvl2_category,
                   cat.lvl2_category_dsc
              
               FROM pm_owner.bank_account_detail bank
                  
               LEFT JOIN pm_owner.product_level_category cat
                   ON cat.prod_src_val = bank.prod_src_val
                   AND cat.src_syst_cd = bank.src_syst_cd
               WHERE bank.month_id = to_char(process_dt, 'YYYYMM')) bank
           GROUP BY bank.customerid, bank.month_id) bank
               ON main.customerid = bank.customerid
               
       LEFT JOIN (
           SELECT
               bank.month_id,
               bank.customerid,
               
               -- status attributes
               COUNT(*) c_closed_account,
               SUM(CASE WHEN bank.lvl2_category = 1100 THEN 1 ELSE 0 END) c_closed_package_and_dda,
               SUM(CASE WHEN bank.lvl2_category = 1400 THEN 1 ELSE 0 END) c_closed_saving,
               SUM(CASE WHEN bank.lvl2_category = 1500 THEN 1 ELSE 0 END) c_closed_loan,
               SUM(CASE WHEN bank.lvl3_category in (1940, 1560, 1580 ,1950) THEN 1 ELSE 0 END) c_closed_regular_loan,
               SUM(CASE WHEN bank.lvl3_category in (1550, 1930) THEN 1 ELSE 0 END) c_closed_mortgage,
               SUM(CASE WHEN bank.lvl3_category in (1960, 1910) THEN 1 ELSE 0 END) c_closed_credit_card_account,
               SUM(CASE WHEN bank.lvl2_category = 1600 THEN 1 ELSE 0 END) c_closed_business_finance,
               
               SUM(bank.i_premature_repay) c_early_paid_off_loan,
               SUM(bank.drawn_loan) volume_closed_loan,
               SUM(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) THEN bank.drawn_loan ELSE 0 END) volume_closed_regular_loan,
               SUM(CASE WHEN bank.lvl3_category in (1550, 1930) THEN bank.drawn_loan ELSE 0 END) volume_closed_mortgage,
           
               LEAST(COALESCE(MIN(CASE WHEN bank.lvl2_category = 1100 THEN bank.c_days_since_closing ELSE NULL END), process_dt - process_dt_hist), process_dt - process_dt_hist) recency_dda_closed,
               LEAST(COALESCE(MIN(CASE WHEN bank.lvl2_category = 1400 THEN bank.c_days_since_closing ELSE NULL END), process_dt - process_dt_hist), process_dt - process_dt_hist) recency_savings_closed,
               LEAST(COALESCE(MIN(CASE WHEN bank.lvl2_category = 1500 THEN bank.c_days_since_closing ELSE NULL END), process_dt - process_dt_hist), process_dt - process_dt_hist) recency_loan_closed,
               LEAST(COALESCE(MIN(CASE WHEN bank.lvl2_category = 1600 THEN bank.c_days_since_closing ELSE NULL END), process_dt - process_dt_hist), process_dt - process_dt_hist) recency_businessfinance_closed, 
           
               LEAST(COALESCE(MAX(CASE WHEN bank.lvl2_category = 1100 THEN bank.c_days_since_closing ELSE NULL END), process_dt - process_dt_hist), process_dt - process_dt_hist) maxtime_dda_closed,
               LEAST(COALESCE(MAX(CASE WHEN bank.lvl2_category = 1400 THEN bank.c_days_since_closing ELSE NULL END), process_dt - process_dt_hist), process_dt - process_dt_hist) maxtime_savings_closed,
               LEAST(COALESCE(MAX(CASE WHEN bank.lvl2_category = 1500 THEN bank.c_days_since_closing ELSE NULL END), process_dt - process_dt_hist), process_dt - process_dt_hist) maxtime_loan_closed,
               LEAST(COALESCE(MAX(CASE WHEN bank.lvl2_category = 1600 THEN bank.c_days_since_closing ELSE NULL END), process_dt - process_dt_hist), process_dt - process_dt_hist) maxtime_businessfinance_closed, 
              
               -- actions attributes
               MAX(CASE WHEN bank.close_dt > add_months(process_dt, -1) AND bank.close_dt <= process_dt THEN 1 ELSE 0 END) i_closed_total_account,
               MAX(CASE WHEN bank.lvl2_category = 1100 AND bank.close_dt > add_months(process_dt, -1) AND bank.close_dt <= process_dt THEN 1 ELSE 0 END) i_closed_package_and_dda,
               MAX(CASE WHEN bank.lvl3_category = 1410 AND bank.close_dt > add_months(process_dt, -1) AND bank.close_dt <= process_dt THEN 1 ELSE 0 END) i_closed_savings_account,
               MAX(CASE WHEN bank.lvl3_category = 1420 AND bank.close_dt > add_months(process_dt, -1) AND bank.close_dt <= process_dt THEN 1 ELSE 0 END) i_closed_term_deposit,
               MAX(CASE WHEN bank.lvl3_category in (1940, 1560, 1580, 1950) AND bank.close_dt > add_months(process_dt, -1) AND bank.close_dt <= process_dt THEN 1 ELSE 0 END) i_closed_loan,
               MAX(CASE WHEN bank.lvl3_category in (1550, 1930) AND bank.close_dt > add_months(process_dt, -1) AND bank.close_dt <= process_dt THEN 1 ELSE 0 END) i_closed_mortgage,
               MAX(CASE WHEN bank.lvl2_category = 1600 AND close_dt > add_months(process_dt, -1) AND bank.close_dt <= process_dt THEN 1 ELSE 0 END) i_closed_business_finance 
           FROM (
               SELECT
                   bank.*, 
                   cat.lvl4_category,
                   cat.lvl4_category_dsc,
                   cat.lvl3_category,
                   cat.lvl3_category_dsc,
                   cat.lvl2_category,
                   cat.lvl2_category_dsc
              
               FROM pm_owner.bank_account_closed bank
                  
               LEFT JOIN pm_owner.product_level_category cat
                   ON cat.prod_src_val = bank.prod_src_val
                   AND cat.src_syst_cd = bank.src_syst_cd
               WHERE bank.month_id = to_char(process_dt, 'YYYYMM')) bank
           
           --left join pm_owner.attr_bank_account
           
           GROUP BY bank.customerid, bank.month_id) bank_closed
               ON main.customerid = bank_closed.customerid
       
       LEFT JOIN (
           SELECT
               customerid,
               COUNT(DISTINCT month_id) + 1 c_month_id,
               
               MAX(i_opened_total_account_m) i_opened_total_account_q, 
               MAX(i_opened_package_and_dda_m) i_opened_package_and_dda_q, 
               MAX(i_opened_savings_account_m) i_opened_savings_account_q, 
               MAX(i_opened_term_deposit_m) i_opened_term_deposit_q, 
               MAX(i_opened_loan_m) i_opened_loan_q, 
               MAX(i_opened_mortgage_m) i_opened_mortgage_q, 
               MAX(i_opened_business_finance_m) i_opened_business_finance_q, 
               MAX(i_refix_interest_m) i_refix_interest_q, 
       
               SUM(sum_dda_balance) avg_dda_balance_q, 
               SUM(sum_savings_balance) avg_savings_balance_q, 
               SUM(sum_balance) avg_balance_q, 
               SUM(sum_loaned_money) avg_loaned_money_q, 
               SUM(sum_total_balance) avg_total_balance_q, 
       
               SUM(sum_total_debt) avg_total_debt_q, 
               SUM(sum_credit_turnover_m) avg_credit_turnover_q, 
               SUM(c_credit_turnover_m) c_credit_turnover_q, 
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag1,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag1,  
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag2,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag2,
              
               MAX(i_closed_total_account) i_closed_total_account_q, 
               MAX(i_closed_package_and_dda) i_closed_package_and_dda_q, 
               MAX(i_closed_savings_account) i_closed_savings_account_q, 
               MAX(i_closed_term_deposit) i_closed_term_deposit_q, 
               MAX(i_closed_loan) i_closed_loan_q, 
               MAX(i_closed_mortgage) i_closed_mortgage_q, 
               MAX(i_closed_business_finance) i_closed_business_finance_q
       
           FROM pm_owner.attr_bank_account
           WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -2), 'YYYYMM'))
                          AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
           GROUP BY customerid) bank_q
           ON main.customerid = bank_q.customerid
       
       LEFT JOIN (SELECT
               customerid,
               COUNT(DISTINCT month_id) + 1 c_month_id,
       
               MAX(i_opened_total_account_m) i_opened_total_account_y, 
               MAX(i_opened_package_and_dda_m) i_opened_package_and_dda_y, 
               MAX(i_opened_savings_account_m) i_opened_savings_account_y, 
               MAX(i_opened_term_deposit_m) i_opened_term_deposit_y, 
               MAX(i_opened_loan_m) i_opened_loan_y, 
               MAX(i_opened_mortgage_m) i_opened_mortgage_y, 
               MAX(i_opened_business_finance_m) i_opened_business_finance_y, 
               MAX(i_refix_interest_m) i_refix_interest_y, 
       
               SUM(sum_dda_balance) avg_dda_balance_y, 
               SUM(sum_savings_balance) avg_savings_balance_y, 
               SUM(sum_balance) avg_balance_y, 
               SUM(sum_loaned_money) avg_loaned_money_y, 
               SUM(sum_total_balance) avg_total_balance_y, 
       
               SUM(sum_total_debt) avg_total_debt_y, 
               SUM(sum_credit_turnover_m) avg_credit_turnover_y, 
               SUM(c_credit_turnover_m) c_credit_turnover_y, 
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag1,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag1,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag1,  
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag2,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag2,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag2,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag3,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag3,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag3,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag3,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag3,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag3,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag3,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag3,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -4), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag4,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -4), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag4,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -4), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag4,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -4), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag4,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -4), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag4,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -4), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag4,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -4), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag4,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -4), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag4,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -5), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag5,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -5), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag5,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -5), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag5,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -5), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag5,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -5), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag5,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -5), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag5,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -5), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag5,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -5), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag5,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -6), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag6,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -6), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag6,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -6), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag6,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -6), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag6,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -6), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag6,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -6), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag6,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -6), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag6,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -6), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag6,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -7), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag7,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -7), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag7,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -7), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag7,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -7), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag7,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -7), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag7,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -7), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag7,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -7), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag7,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -7), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag7,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -8), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag8,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -8), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag8,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -8), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag8,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -8), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag8,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -8), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag8,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -8), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag8,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -8), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag8,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -8), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag8,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -9), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag9,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -9), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag9,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -9), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag9,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -9), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag9,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -9), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag9,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -9), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag9,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -9), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag9,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -9), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag9,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -10), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag10,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -10), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag10,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -10), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag10,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -10), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag10,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -10), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag10,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -10), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag10,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -10), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag10,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -10), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag10,
               
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM')) THEN sum_dda_balance ELSE 0 END) sum_dda_balance_lag11,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM')) THEN sum_savings_balance ELSE 0 END) sum_savings_balance_lag11,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM')) THEN sum_balance ELSE 0 END) sum_balance_lag11,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM')) THEN sum_loaned_money ELSE 0 END) sum_loaned_money_lag11,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM')) THEN sum_total_balance ELSE 0 END) sum_total_balance_lag11,             
       
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM')) THEN sum_total_debt ELSE 0 END) sum_total_debt_lag11,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM')) THEN sum_credit_turnover_m ELSE 0 END) sum_credit_turnover_lag11,
               SUM(CASE WHEN month_id = to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM')) THEN c_credit_turnover_m ELSE 0 END) c_credit_turnover_lag11,
               
               MAX(i_closed_total_account) i_closed_total_account_y, 
               MAX(i_closed_package_and_dda) i_closed_package_and_dda_y, 
               MAX(i_closed_savings_account) i_closed_savings_account_y, 
               MAX(i_closed_term_deposit) i_closed_term_deposit_y, 
               MAX(i_closed_loan) i_closed_loan_y, 
               MAX(i_closed_mortgage) i_closed_mortgage_y, 
               MAX(i_closed_business_finance) i_closed_business_finance_y
               
           FROM pm_owner.attr_bank_account
           WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -11), 'YYYYMM'))
                          AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
           GROUP BY customerid) bank_y
           ON main.customerid = bank_y.customerid;


    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);
    
    END;

  PROCEDURE append_attr_txn(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_attr_txn';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_TXN';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);

  BEGIN
    
    -- ATTR_TXN
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

	-- stage txn table
	ETL_OWNER.etl_dbms_util.truncate_table('STG_TXN',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_TXN 
        SELECT
            TCI.card_id                     AS CARD_ID,
            card_detail.card_type,
            card_detail.authorized_user,
            card_detail.account_owner,
            card_detail.i_authorized_user,
            TCI.txn_card_isr_id             AS TXN_CARD_ISR_ID,
            TCI.txn_card_isr_dt             AS TXN_CARD_ISR_DT,
            TCI.txn_cd_cd                   AS TXN_CD_CD,
            TCIAM.curr_cd                   AS CURR_CD,
            TCIAM.txn_card_isr_am_crdb_in crdb_in,
            TCIAM.txn_card_isr_am_am        AS TXN_CARD_ISR_AM_AM,
            TCIAM.txn_card_isr_am_bc_am     AS TXN_CARD_ISR_AM_BC_AM,
            TCIA.txn_card_isr_merch_src_key,
            GG.geo_gen_iso_cntry,
            CASE WHEN TCI.TOKEN_CARD_ID is not null THEN 1 ELSE 0 END i_token,
            COALESCE(token.i_applepay, 0) i_applepay,
            COALESCE(token.i_googlepay, 0) i_googlepay,
            COALESCE(token.i_merchant, 0) i_merchant
        
        FROM L0_OWNER.TXN_CARD_ISR TCI
            
        JOIN L0_OWNER.TXN_CARD_ISR_AM TCIAM
            ON  TCIAM.txn_card_isr_id = TCI.txn_card_isr_id
            AND TCIAM.txn_card_isr_postg_dt = TCI.txn_card_isr_postg_dt
            AND TCIAM.meas_cd = 11000
            
        JOIN L0_OWNER.TXN_CARD_ISR_ADDR TCIA
            ON TCIA.txn_card_isr_id = TCI.txn_card_isr_id
            AND TCIA.txn_card_isr_postg_dt = TCI.txn_card_isr_postg_dt           
        
        JOIN pm_owner.card card_detail
            ON card_detail.card_id = tci.card_id
            AND card_detail.month_id = to_number(to_char(process_dt, 'YYYYMM'))
            
        LEFT JOIN L0_OWNER.GEO_GEN GG
            ON  GG.geo_id = TCIA.geo_id--TCIA.geo_id
            AND GG.geo_gen_start_dt <= process_dt
            AND GG.geo_gen_end_dt > process_dt
            
        LEFT JOIN (
            select
                max(card.card_id) card_id,
                max(token.card_id) card_id_token,
                max(CASE WHEN CPC.CARD_PARM_CD_SRC_VAL='103' THEN 1 ELSE 0 END) i_applepay,
                max(CASE WHEN CPC.CARD_PARM_CD_SRC_VAL='216' THEN 1 ELSE 0 END) i_googlepay,
                max(CASE WHEN CPC.CARD_PARM_CD_SRC_VAL='327' THEN 1 ELSE 0 END) i_merchant
                
            FROM L0_OWNER.CARD card
            JOIN L0_OWNER.CARD_RLTD rltd
                ON card.card_id=rltd.card_id
                AND rltd.card_rltd_start_dt <= process_dt
                AND rltd.card_rltd_end_dt > process_dt
            
            JOIN L0_OWNER.CARD token
                ON token.card_id=rltd.child_card_id
                
            JOIN L0_owner.CARD_PARM CP
                ON CP.CARD_ID = token.card_id
                AND CP.CARD_PARM_END_DT = date'9999-12-31'
            
            JOIN L0_OWNER.CARD_PARM_CD CPC
                ON CPC.CARD_PARM_CD_CD=CP.CARD_PARM_CD
                AND CPC.CARD_PARM_TYPE_CD=197
                
            WHERE 1=1
                AND card.src_syst_cd=650
                AND SUBSTR(token.card_src_id,1,3)='ATO'
            GROUP BY card.card_id, token.card_id) token
        ON token.card_id = TCI.card_id
        AND token.card_id_token = tci.token_card_id
        
        WHERE 1=1
            AND TCI.txn_card_isr_dt > ADD_MONTHS(process_dt, -1)
            AND TCI.txn_card_isr_dt <= process_dt
            AND TCI.src_syst_cd = 650;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

	-- stage mps table
	ETL_OWNER.etl_dbms_util.truncate_table('STG_MPS',l_owner_name);
    INSERT INTO PM_OWNER.STG_MPS
        SELECT
            mps.mps_ord_id,
            mps.mps_ord_dt,
            mps.chanl_id,
            mps.mps_ord_type_cd,
            mps.txn_cd_cd,
            mps.mps_ord_cross_border_in,
            mps.mps_ord_am,
            mps.mps_ord_bc_am,
            mps.curr_cd,
            mps.curr_rate_type_cd,
            mps.cred_cont_id,
            PR_CRED.party_id credit_customerid,
            mps.deb_cont_id,
            PR_DEB.party_id debit_customerid,
            mps.mps_ord_deb_curr_rate_am,
            mps.mps_ord_deb_indiv_rate_in,
            mps.mps_ord_cred_curr_rate_am,
            mps.mps_ord_cred_indiv_rate_in
            
        FROM l0_owner.mps_ord mps
        
        LEFT JOIN L0_OWNER.PARTY_CONT PC_CRED
            ON  PC_CRED.cont_id = mps.cred_cont_id
            AND PC_CRED.party_cont_start_dt <= process_dt
            AND PC_CRED.party_cont_end_dt > process_dt
            AND PC_CRED.party_role_cd = 1 -- client
        
        LEFT JOIN L0_OWNER.PARTY_RLTD PR_CRED
            ON  PR_CRED.child_party_id = PC_CRED.party_id
            AND PR_CRED.party_rltd_start_dt <= process_dt
            AND PR_CRED.party_rltd_end_dt > process_dt
            AND PR_CRED.party_rltd_rsn_cd = 4 -- customerid
        
        LEFT JOIN L0_OWNER.PARTY_CONT PC_DEB
            ON  PC_DEB.cont_id = mps.deb_cont_id
            AND PC_DEB.party_cont_start_dt <= process_dt
            AND PC_DEB.party_cont_end_dt > process_dt
            AND PC_DEB.party_role_cd = 1 -- client
        
        LEFT JOIN L0_OWNER.PARTY_RLTD PR_DEB
            ON  PR_DEB.child_party_id = PC_CRED.party_id
            AND PR_DEB.party_rltd_start_dt <= process_dt
            AND PR_DEB.party_rltd_end_dt > process_dt
            AND PR_DEB.party_rltd_rsn_cd = 4 -- customerid
        
        WHERE 1=1
            AND mps.mps_ord_dt <= process_dt
            AND mps.mps_ord_dt > ADD_MONTHS(process_dt, -1)
            AND mps.mps_ord_canc_in != 'Y';
            
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    INSERT INTO PM_OWNER.ATTR_TXN
        SELECT
            main.month_id,
            main.customerid,   
            COALESCE(all_txn.sum_credit_txn_m, 0) - COALESCE(all_txn.sum_debit_txn_m, 0) sum_txn_m,
            COALESCE(all_txn.median_credit_txn_m, 0) median_credit_txn_m,
            COALESCE(all_txn.c_credit_txn_m, 0) c_credit_txn_m,
            COALESCE(all_txn.sum_credit_txn_m, 0) sum_credit_txn_m,
            COALESCE(all_txn.median_debit_txn_m, 0) median_debit_txn_m,
            COALESCE(all_txn.c_debit_txn_m, 0) c_debit_txn_m,
            COALESCE(all_txn.sum_debit_txn_m, 0) sum_debit_txn_m,
            
            (COALESCE(all_txn.sum_credit_txn_m, 0) - COALESCE(all_txn.sum_debit_txn_m, 0) + COALESCE(txn_q.sum_txn_q, 0))/main.c_month_q sum_txn_q,
            (COALESCE(all_txn.median_credit_txn_m, 0) + COALESCE(txn_q.median_credit_txn_q, 0))/main.c_month_q median_credit_txn_q,
            (COALESCE(all_txn.c_credit_txn_m, 0) + COALESCE(txn_q.c_credit_txn_q, 0))/main.c_month_q c_credit_txn_q,
            (COALESCE(all_txn.sum_credit_txn_m, 0) + COALESCE(txn_q.sum_credit_txn_q, 0))/main.c_month_q sum_credit_txn_q,
            (COALESCE(all_txn.median_debit_txn_m, 0) + COALESCE(txn_q.median_debit_txn_q, 0))/main.c_month_q median_debit_txn_q,
            (COALESCE(all_txn.c_debit_txn_m, 0) + COALESCE(txn_q.c_debit_txn_q, 0))/main.c_month_q c_debit_txn_q,
            (COALESCE(all_txn.sum_debit_txn_m, 0) + COALESCE(txn_q.sum_debit_txn_q, 0))/main.c_month_q sum_debit_txn_q,
            
            (COALESCE(all_txn.sum_credit_txn_m, 0) - COALESCE(all_txn.sum_debit_txn_m, 0) + COALESCE(txn_y.sum_txn_y, 0))/main.c_month_y sum_txn_y,
            (COALESCE(all_txn.median_credit_txn_m, 0) + COALESCE(txn_y.median_credit_txn_y, 0))/main.c_month_y median_credit_txn_y,
            (COALESCE(all_txn.c_credit_txn_m, 0) + COALESCE(txn_y.c_credit_txn_y, 0))/main.c_month_y c_credit_txn_y,
            (COALESCE(all_txn.sum_credit_txn_m, 0) + COALESCE(txn_y.sum_credit_txn_y, 0))/main.c_month_y sum_credit_txn_y,
            (COALESCE(all_txn.median_debit_txn_m, 0) + COALESCE(txn_y.median_debit_txn_y, 0))/main.c_month_y median_debit_txn_y,
            (COALESCE(all_txn.c_debit_txn_m, 0) + COALESCE(txn_y.c_debit_txn_y, 0))/main.c_month_y c_debit_txn_y,
            (COALESCE(all_txn.sum_debit_txn_m, 0) + COALESCE(txn_y.sum_debit_txn_y, 0))/main.c_month_y sum_debit_txn_y, 
        
            COALESCE(txn_card_owner.i_txn_card_m, 0) i_txn_card_m,
            COALESCE(txn_card_owner.i_txn_card_authorized_user_m, 0) i_txn_card_authorized_user_m,
            COALESCE(txn_card_owner.i_txn_atm_withdrawal_m, 0) i_txn_atm_withdrawal_m,
            COALESCE(txn_card_owner.i_debit_txn_card_m, 0) i_debit_txn_card_m,
            COALESCE(txn_card_owner.i_credit_txn_card_m, 0) i_credit_txn_card_m,
            COALESCE(txn_card_owner.i_txn_card_token_m, 0) i_txn_card_token_m,        
            COALESCE(txn_card_owner.sum_txn_card_m, 0) sum_txn_card_m,
            COALESCE(txn_card_owner.sum_txn_card_authorized_user_m, 0) sum_txn_card_authorized_user_m,
            COALESCE(txn_card_owner.sum_txn_atm_withdrawal_m, 0) sum_txn_atm_withdrawal_m,
            COALESCE(txn_card_owner.sum_debit_txn_card_m, 0) sum_debit_txn_card_m,
            COALESCE(txn_card_owner.sum_credit_txn_card_m, 0) sum_credit_txn_card_m,
            COALESCE(txn_card_owner.sum_txn_card_token_m, 0) sum_txn_card_token_m,
            COALESCE(txn_card_owner.sum_txn_card_token_apple_m, 0) sum_txn_card_token_apple_m,
            COALESCE(txn_card_owner.sum_txn_card_token_google_m, 0) sum_txn_card_token_google_m,
            COALESCE(txn_card_owner.sum_txn_card_token_merchant_m, 0) sum_txn_card_token_merchant_m,
            
            GREATEST(COALESCE(txn_card_owner.i_txn_card_m, 0), COALESCE(txn_q.i_txn_card_q, 0)) i_txn_card_q,
            GREATEST(COALESCE(txn_card_owner.i_txn_card_authorized_user_m, 0), COALESCE(txn_q.i_txn_card_authorized_user_q, 0)) i_txn_card_authorized_user_q,
            GREATEST(COALESCE(txn_card_owner.i_txn_atm_withdrawal_m, 0), COALESCE(txn_q.i_txn_atm_withdrawal_q, 0)) i_txn_atm_withdrawal_q,
            GREATEST(COALESCE(txn_card_owner.i_debit_txn_card_m, 0), COALESCE(txn_q.i_debit_txn_card_q, 0)) i_debit_txn_card_q,
            GREATEST(COALESCE(txn_card_owner.i_credit_txn_card_m, 0), COALESCE(txn_q.i_credit_txn_card_q, 0)) i_credit_txn_card_q,
            GREATEST(COALESCE(txn_card_owner.i_txn_card_token_m, 0), COALESCE(txn_q.i_txn_card_token_q, 0)) i_txn_card_token_q,        
            (COALESCE(txn_card_owner.sum_txn_card_m, 0) + COALESCE(txn_q.sum_txn_card_q, 0))/main.c_month_q sum_txn_card_q,
            (COALESCE(txn_card_owner.sum_txn_card_authorized_user_m, 0) + COALESCE(txn_q.sum_txn_card_authorized_user_q, 0))/main.c_month_q sum_txn_card_authorized_user_q,
            (COALESCE(txn_card_owner.sum_txn_atm_withdrawal_m, 0) + COALESCE(txn_q.sum_txn_atm_withdrawal_q, 0))/main.c_month_q sum_txn_atm_withdrawal_q,
            (COALESCE(txn_card_owner.sum_debit_txn_card_m, 0) + COALESCE(txn_q.sum_debit_txn_card_q, 0))/main.c_month_q sum_debit_txn_card_q,
            (COALESCE(txn_card_owner.sum_credit_txn_card_m, 0) + COALESCE(txn_q.sum_credit_txn_card_q, 0))/main.c_month_q sum_credit_txn_card_q,
            (COALESCE(txn_card_owner.sum_txn_card_token_m, 0) + COALESCE(txn_q.sum_txn_card_token_q, 0))/main.c_month_q sum_txn_card_token_q,
            (COALESCE(txn_card_owner.sum_txn_card_token_apple_m, 0) + COALESCE(txn_q.sum_txn_card_token_apple_q, 0))/main.c_month_q sum_txn_card_token_apple_q,
            (COALESCE(txn_card_owner.sum_txn_card_token_google_m, 0) + COALESCE(txn_q.sum_txn_card_token_google_q, 0))/main.c_month_q sum_txn_card_token_google_q,
            (COALESCE(txn_card_owner.sum_txn_card_token_merchant_m, 0) + COALESCE(txn_q.sum_txn_card_token_merchant_q, 0))/main.c_month_q sum_txn_card_token_merchant_q,    
            
            GREATEST(COALESCE(txn_card_owner.i_txn_card_m, 0), COALESCE(txn_y.i_txn_card_y, 0)) i_txn_card_y,
            GREATEST(COALESCE(txn_card_owner.i_txn_card_authorized_user_m, 0), COALESCE(txn_y.i_txn_card_authorized_user_y, 0)) i_txn_card_authorized_user_y,
            GREATEST(COALESCE(txn_card_owner.i_txn_atm_withdrawal_m, 0), COALESCE(txn_y.i_txn_atm_withdrawal_y, 0)) i_txn_atm_withdrawal_y,
            GREATEST(COALESCE(txn_card_owner.i_debit_txn_card_m, 0), COALESCE(txn_y.i_debit_txn_card_y, 0)) i_debit_txn_card_y,
            GREATEST(COALESCE(txn_card_owner.i_credit_txn_card_m, 0), COALESCE(txn_y.i_credit_txn_card_y, 0)) i_credit_txn_card_y,
            GREATEST(COALESCE(txn_card_owner.i_txn_card_token_m, 0), COALESCE(txn_y.i_txn_card_token_y, 0)) i_txn_card_token_y,        
            (COALESCE(txn_card_owner.sum_txn_card_m, 0) + COALESCE(txn_y.sum_txn_card_y, 0))/main.c_month_y sum_txn_card_y,
            (COALESCE(txn_card_owner.sum_txn_card_authorized_user_m, 0) + COALESCE(txn_y.sum_txn_card_authorized_user_y, 0))/main.c_month_y sum_txn_card_authorized_user_y,
            (COALESCE(txn_card_owner.sum_txn_atm_withdrawal_m, 0) + COALESCE(txn_y.sum_txn_atm_withdrawal_y, 0))/main.c_month_y sum_txn_atm_withdrawal_y,
            (COALESCE(txn_card_owner.sum_debit_txn_card_m, 0) + COALESCE(txn_y.sum_debit_txn_card_y, 0))/main.c_month_y sum_debit_txn_card_y,
            (COALESCE(txn_card_owner.sum_credit_txn_card_m, 0) + COALESCE(txn_y.sum_credit_txn_card_y, 0))/main.c_month_y sum_credit_txn_card_y,
            (COALESCE(txn_card_owner.sum_txn_card_token_m, 0) + COALESCE(txn_y.sum_txn_card_token_y, 0))/main.c_month_y sum_txn_card_token_y,
            (COALESCE(txn_card_owner.sum_txn_card_token_apple_m, 0) + COALESCE(txn_y.sum_txn_card_token_apple_y, 0))/main.c_month_y sum_txn_card_token_apple_y,
            (COALESCE(txn_card_owner.sum_txn_card_token_google_m, 0) + COALESCE(txn_y.sum_txn_card_token_google_y, 0))/main.c_month_y sum_txn_card_token_google_y,
            (COALESCE(txn_card_owner.sum_txn_card_token_merchant_m, 0) + COALESCE(txn_y.sum_txn_card_token_merchant_y, 0))/main.c_month_y sum_txn_card_token_merchant_y,    
            
            COALESCE(txn_card_owner.c_txn_card_m, 0) c_txn_card_m,
            COALESCE(txn_card_owner.c_txn_card_authorized_user_m, 0) c_txn_card_authorized_user_m,
            COALESCE(txn_card_owner.c_txn_atm_withdrawal_m, 0) c_txn_atm_withdrawal_m,
            COALESCE(txn_card_owner.c_debit_txn_card_m, 0) c_debit_txn_card_m,
            COALESCE(txn_card_owner.c_credit_txn_card_m, 0) c_credit_txn_card_m,
            COALESCE(txn_card_owner.c_txn_card_token_m, 0) c_txn_card_token_m,
            COALESCE(txn_card_owner.c_txn_card_token_apple_m, 0) c_txn_card_token_apple_m,
            COALESCE(txn_card_owner.c_txn_card_token_google_m, 0) c_txn_card_token_google_m,
            COALESCE(txn_card_owner.c_txn_card_token_merchant_m, 0) c_txn_card_token_merchant_m,  
            COALESCE(txn_card_owner.median_txn_card_m, 0) median_txn_card_m,
            COALESCE(txn_card_owner.median_txn_atm_withdrawal_m, 0) median_txn_atm_withdrawal_m,
            COALESCE(txn_card_owner.median_debit_txn_card_m, 0) median_debit_txn_card_m,
            COALESCE(txn_card_owner.median_credit_txn_card_m, 0) median_credit_txn_card_m,
            COALESCE(txn_card_owner.median_txn_card_token_m, 0) median_txn_card_token_m,
            COALESCE(txn_card_owner.sum_returned_txn_card_m, 0) sum_returned_txn_card_m,
            COALESCE(txn_card_owner.c_returned_txn_card_m, 0) c_returned_txn_card_m,
            COALESCE(txn_card_owner.c_merchant_txn_card_m, 0) c_merchant_txn_card_m,            
            COALESCE(txn_card_owner.c_country_txn_card_m, 0) c_country_txn_card_m,             
        
            (COALESCE(txn_card_owner.c_txn_card_m, 0) + COALESCE(txn_q.c_txn_card_q, 0))/main.c_month_q c_txn_card_q,
            (COALESCE(txn_card_owner.c_txn_card_authorized_user_m, 0) + COALESCE(txn_q.c_txn_card_authorized_user_q, 0))/main.c_month_q c_txn_card_authorized_user_q,
            (COALESCE(txn_card_owner.c_txn_atm_withdrawal_m, 0) + COALESCE(txn_q.c_txn_atm_withdrawal_q, 0))/main.c_month_q c_txn_atm_withdrawal_q,
            (COALESCE(txn_card_owner.c_debit_txn_card_m, 0) + COALESCE(txn_q.c_debit_txn_card_q, 0))/main.c_month_q c_debit_txn_card_q,
            (COALESCE(txn_card_owner.c_credit_txn_card_m, 0) + COALESCE(txn_q.c_credit_txn_card_q, 0))/main.c_month_q c_credit_txn_card_q,
            (COALESCE(txn_card_owner.c_txn_card_token_m, 0) + COALESCE(txn_q.c_txn_card_token_q, 0))/main.c_month_q c_txn_card_token_q,
            (COALESCE(txn_card_owner.c_txn_card_token_apple_m, 0) + COALESCE(txn_q.c_txn_card_token_apple_q, 0))/main.c_month_q c_txn_card_token_apple_q,
            (COALESCE(txn_card_owner.c_txn_card_token_google_m, 0) + COALESCE(txn_q.c_txn_card_token_google_q, 0))/main.c_month_q c_txn_card_token_google_q,
            (COALESCE(txn_card_owner.c_txn_card_token_merchant_m, 0) + COALESCE(txn_q.c_txn_card_token_merchant_q, 0))/main.c_month_q c_txn_card_token_merchant_q,  
            (COALESCE(txn_card_owner.median_txn_card_m, 0) + COALESCE(txn_q.median_txn_card_q, 0))/main.c_month_q median_txn_card_q,
            (COALESCE(txn_card_owner.median_txn_atm_withdrawal_m, 0) + COALESCE(txn_q.median_txn_atm_withdrawal_q, 0))/main.c_month_q median_txn_atm_withdrawal_q,
            (COALESCE(txn_card_owner.median_debit_txn_card_m, 0) + COALESCE(txn_q.median_debit_txn_card_q, 0))/main.c_month_q median_debit_txn_card_q,
            (COALESCE(txn_card_owner.median_credit_txn_card_m, 0) + COALESCE(txn_q.median_credit_txn_card_q, 0))/main.c_month_q median_credit_txn_card_q,
            (COALESCE(txn_card_owner.median_txn_card_token_m, 0) + COALESCE(txn_q.median_txn_card_token_q, 0))/main.c_month_q median_txn_card_token_q,
            (COALESCE(txn_card_owner.sum_returned_txn_card_m, 0) + COALESCE(txn_q.sum_returned_txn_card_q, 0))/main.c_month_q sum_returned_txn_card_q,
            (COALESCE(txn_card_owner.c_returned_txn_card_m, 0) + COALESCE(txn_q.c_returned_txn_card_q, 0))/main.c_month_q c_returned_txn_card_q,
            (COALESCE(txn_card_owner.c_merchant_txn_card_m, 0) + COALESCE(txn_q.c_merchant_txn_card_q, 0))/main.c_month_q c_merchant_txn_card_q,
            (COALESCE(txn_card_owner.c_country_txn_card_m, 0) + COALESCE(txn_q.c_country_txn_card_q, 0))/main.c_month_q c_country_txn_card_q,
--        
            (COALESCE(txn_card_owner.c_txn_card_m, 0) + COALESCE(txn_y.c_txn_card_y, 0))/main.c_month_y c_txn_card_y,
            (COALESCE(txn_card_owner.c_txn_card_authorized_user_m, 0) + COALESCE(txn_y.c_txn_card_authorized_user_y, 0))/main.c_month_y c_txn_card_authorized_user_y,
            (COALESCE(txn_card_owner.c_txn_atm_withdrawal_m, 0) + COALESCE(txn_y.c_txn_atm_withdrawal_y, 0))/main.c_month_y c_txn_atm_withdrawal_y,
            (COALESCE(txn_card_owner.c_debit_txn_card_m, 0) + COALESCE(txn_y.c_debit_txn_card_y, 0))/main.c_month_y c_debit_txn_card_y,
            (COALESCE(txn_card_owner.c_credit_txn_card_m, 0) + COALESCE(txn_y.c_credit_txn_card_y, 0))/main.c_month_y c_credit_txn_card_y,
            (COALESCE(txn_card_owner.c_txn_card_token_m, 0) + COALESCE(txn_y.c_txn_card_token_y, 0))/main.c_month_y c_txn_card_token_y,
            (COALESCE(txn_card_owner.c_txn_card_token_apple_m, 0) + COALESCE(txn_y.c_txn_card_token_apple_y, 0))/main.c_month_y c_txn_card_token_apple_y,
            (COALESCE(txn_card_owner.c_txn_card_token_google_m, 0) + COALESCE(txn_y.c_txn_card_token_google_y, 0))/main.c_month_y c_txn_card_token_google_y,
            (COALESCE(txn_card_owner.c_txn_card_token_merchant_m, 0) + COALESCE(txn_y.c_txn_card_token_merchant_y, 0))/main.c_month_y c_txn_card_token_merchant_y,  
            (COALESCE(txn_card_owner.median_txn_card_m, 0) + COALESCE(txn_y.median_txn_card_y, 0))/main.c_month_y median_txn_card_y,
            (COALESCE(txn_card_owner.median_txn_atm_withdrawal_m, 0) + COALESCE(txn_y.median_txn_atm_withdrawal_y, 0))/main.c_month_y median_txn_atm_withdrawal_y,
            (COALESCE(txn_card_owner.median_debit_txn_card_m, 0) + COALESCE(txn_y.median_debit_txn_card_y, 0))/main.c_month_y median_debit_txn_card_y,
            (COALESCE(txn_card_owner.median_credit_txn_card_m, 0) + COALESCE(txn_y.median_credit_txn_card_y, 0))/main.c_month_y median_credit_txn_card_y,
            (COALESCE(txn_card_owner.median_txn_card_token_m, 0) + COALESCE(txn_y.median_txn_card_token_y, 0))/main.c_month_y median_txn_card_token_y,
            (COALESCE(txn_card_owner.sum_returned_txn_card_m, 0) + COALESCE(txn_y.sum_returned_txn_card_y, 0))/main.c_month_y sum_returned_txn_card_y,
            (COALESCE(txn_card_owner.c_returned_txn_card_m, 0) + COALESCE(txn_y.c_returned_txn_card_y, 0))/main.c_month_y c_returned_txn_card_y,
            (COALESCE(txn_card_owner.c_merchant_txn_card_m, 0) + COALESCE(txn_y.c_merchant_txn_card_y, 0))/main.c_month_y c_merchant_txn_card_y,
            (COALESCE(txn_card_owner.c_country_txn_card_m, 0) + COALESCE(txn_y.c_country_txn_card_y, 0))/main.c_month_y c_country_txn_card_y,
            
            COALESCE(txn_card_holder.i_txn_card_holder_m, 0) i_txn_card_holder_m,
            COALESCE(txn_card_holder.i_credit_txn_card_holder_m, 0) i_credit_txn_card_holder_m,
            COALESCE(txn_card_holder.i_txn_card_token_holder_m, 0) i_txn_card_token_holder_m,       
            COALESCE(txn_card_holder.sum_txn_card_holder_m, 0) sum_txn_card_holder_m,
            COALESCE(txn_card_holder.sum_credit_txn_card_holder_m, 0) sum_credit_txn_card_holder_m,
            COALESCE(txn_card_holder.sum_txn_card_token_holder_m, 0) sum_txn_card_token_holder_m,        
            COALESCE(txn_card_holder.c_txn_card_holder_m, 0) c_txn_card_holder_m,
            COALESCE(txn_card_holder.c_credit_txn_card_holder_m, 0) c_credit_txn_card_holder_m,
            COALESCE(txn_card_holder.c_txn_card_token_holder_m, 0) c_txn_card_token_holder_m,       
            COALESCE(txn_card_holder.median_txn_card_holder_m, 0) median_txn_card_holder_m,          
        
            GREATEST(COALESCE(txn_card_holder.i_txn_card_holder_m, 0), COALESCE(txn_q.i_txn_card_holder_q, 0)) i_txn_card_holder_q,
            GREATEST(COALESCE(txn_card_holder.i_credit_txn_card_holder_m, 0), COALESCE(txn_q.i_credit_txn_card_holder_q, 0)) i_credit_txn_card_holder_q,
            GREATEST(COALESCE(txn_card_holder.i_txn_card_token_holder_m, 0), COALESCE(txn_q.i_txn_card_token_holder_q, 0)) i_txn_card_token_holder_q,       
            (COALESCE(txn_card_holder.sum_txn_card_holder_m, 0) + COALESCE(txn_q.sum_txn_card_holder_q, 0))/main.c_month_q sum_txn_card_holder_q,
            (COALESCE(txn_card_holder.sum_credit_txn_card_holder_m, 0) + COALESCE(txn_q.sum_credit_txn_card_holder_q, 0))/main.c_month_q sum_credit_txn_card_holder_q,
            (COALESCE(txn_card_holder.sum_txn_card_token_holder_m, 0) + COALESCE(txn_q.sum_txn_card_token_holder_q, 0))/main.c_month_q sum_txn_card_token_holder_q,        
            (COALESCE(txn_card_holder.c_txn_card_holder_m, 0) + COALESCE(txn_q.c_txn_card_holder_q, 0))/main.c_month_q c_txn_card_holder_q,
            (COALESCE(txn_card_holder.c_credit_txn_card_holder_m, 0) + COALESCE(txn_q.c_credit_txn_card_holder_q, 0))/main.c_month_q c_credit_txn_card_holder_q,
            (COALESCE(txn_card_holder.c_txn_card_token_holder_m, 0) + COALESCE(txn_q.c_txn_card_token_holder_q, 0))/main.c_month_q c_txn_card_token_holder_q,       
            (COALESCE(txn_card_holder.median_txn_card_holder_m, 0) + COALESCE(txn_q.median_txn_card_holder_q, 0))/main.c_month_q median_txn_card_holder_q,
            
            GREATEST(COALESCE(txn_card_holder.i_txn_card_holder_m, 0), COALESCE(txn_y.i_txn_card_holder_y, 0)) i_txn_card_holder_y,
            GREATEST(COALESCE(txn_card_holder.i_credit_txn_card_holder_m, 0), COALESCE(txn_y.i_credit_txn_card_holder_y, 0)) i_credit_txn_card_holder_y,
            GREATEST(COALESCE(txn_card_holder.i_txn_card_token_holder_m, 0), COALESCE(txn_y.i_txn_card_token_holder_y, 0)) i_txn_card_token_holder_y,       
            (COALESCE(txn_card_holder.sum_txn_card_holder_m, 0) + COALESCE(txn_y.sum_txn_card_holder_y, 0))/main.c_month_y sum_txn_card_holder_y,
            (COALESCE(txn_card_holder.sum_credit_txn_card_holder_m, 0) + COALESCE(txn_y.sum_credit_txn_card_holder_y, 0))/main.c_month_y sum_credit_txn_card_holder_y,
            (COALESCE(txn_card_holder.sum_txn_card_token_holder_m, 0) + COALESCE(txn_y.sum_txn_card_token_holder_y, 0))/main.c_month_y sum_txn_card_token_holder_y,        
            (COALESCE(txn_card_holder.c_txn_card_holder_m, 0) + COALESCE(txn_y.c_txn_card_holder_y, 0))/main.c_month_y c_txn_card_holder_y,
            (COALESCE(txn_card_holder.c_credit_txn_card_holder_m, 0) + COALESCE(txn_y.c_credit_txn_card_holder_y, 0))/main.c_month_y c_credit_txn_card_holder_y,
            (COALESCE(txn_card_holder.c_txn_card_token_holder_m, 0) + COALESCE(txn_y.c_txn_card_token_holder_y, 0))/main.c_month_y c_txn_card_token_holder_y,       
            (COALESCE(txn_card_holder.median_txn_card_holder_m, 0) + COALESCE(txn_y.median_txn_card_holder_y, 0))/main.c_month_y median_txn_card_holder_y,
            
            -- pozriet withdrawal transakcie
            COALESCE(all_txn.i_txn_deposit_m, 0) i_txn_deposit_m,
            COALESCE(txn_card_owner.i_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.i_txn_withdrawal_m, 0) i_txn_withdrawal_m,
            COALESCE(all_txn.i_txn_branch_deposit_m, 0) i_txn_branch_deposit_m,
            COALESCE(all_txn.i_txn_branch_withdrawal_m, 0) i_txn_branch_withdrawal_m,
            COALESCE(all_txn.i_txn_atm_deposit_m, 0) i_txn_atm_deposit_m,
            COALESCE(all_txn.i_txn_branch_deposit_3p_m, 0) i_txn_branch_deposit_3p_m,               
            COALESCE(all_txn.sum_cash_txn_m, 0) sum_cash_txn_m,
            COALESCE(txn_card_owner.sum_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.sum_txn_withdrawal_m, 0) sum_txn_withdrawal_m,
            COALESCE(all_txn.sum_txn_branch_deposit_m, 0) sum_txn_branch_deposit_m,
            COALESCE(all_txn.sum_txn_branch_withdrawal_m, 0) sum_txn_branch_withdrawal_m,
            COALESCE(all_txn.sum_txn_atm_deposit_m, 0) sum_txn_atm_deposit_m,
            COALESCE(all_txn.sum_txn_branch_deposit_3p_m, 0) sum_txn_branch_deposit_3p_m,
            COALESCE(all_txn.c_txn_deposit_m, 0) c_txn_deposit_m,
            COALESCE(txn_card_owner.c_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.c_txn_withdrawal_m, 0) c_txn_withdrawal_m,
            COALESCE(all_txn.c_txn_branch_deposit_m, 0) c_txn_branch_deposit_m,
            COALESCE(all_txn.c_txn_branch_withdrawal_m, 0) c_txn_branch_withdrawal_m,
            COALESCE(all_txn.c_txn_atm_deposit_m, 0) c_txn_atm_deposit_m,
            COALESCE(all_txn.c_txn_branch_deposit_3p_m, 0) c_txn_branch_deposit_3p_m,
            
            GREATEST(COALESCE(all_txn.i_txn_deposit_m, 0), COALESCE(txn_q.i_txn_deposit_q, 0)) i_txn_deposit_q,
            GREATEST(COALESCE(txn_card_owner.i_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.i_txn_withdrawal_m, 0),
                     COALESCE(txn_q.i_txn_atm_withdrawal_q, 0) + COALESCE(txn_q.i_txn_withdrawal_q, 0)) i_txn_withdrawal_q,
            GREATEST(COALESCE(all_txn.i_txn_branch_deposit_m, 0), COALESCE(txn_q.i_txn_branch_deposit_q, 0)) i_txn_branch_deposit_q,
            GREATEST(COALESCE(all_txn.i_txn_branch_withdrawal_m, 0), COALESCE(txn_q.i_txn_branch_withdrawal_q, 0)) i_txn_branch_withdrawal_q,
            GREATEST(COALESCE(all_txn.i_txn_atm_deposit_m, 0), COALESCE(txn_q.i_txn_atm_deposit_q, 0)) i_txn_atm_deposit_q,
            GREATEST(COALESCE(all_txn.i_txn_branch_deposit_3p_m, 0), COALESCE(txn_q.i_txn_branch_deposit_3p_q, 0)) i_txn_branch_deposit_3p_q,               
            (COALESCE(all_txn.sum_cash_txn_m, 0) + COALESCE(txn_q.sum_cash_txn_q, 0))/main.c_month_q sum_cash_txn_q,
            (COALESCE(txn_card_owner.sum_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.sum_txn_withdrawal_m, 0)
            + COALESCE(txn_q.sum_txn_atm_withdrawal_q, 0) + COALESCE(txn_q.sum_txn_withdrawal_q, 0))/main.c_month_q sum_txn_withdrawal_q,   
            (COALESCE(all_txn.sum_txn_branch_deposit_m, 0) + COALESCE(txn_q.sum_txn_branch_deposit_q, 0))/main.c_month_q sum_txn_branch_deposit_q,
            (COALESCE(all_txn.sum_txn_branch_withdrawal_m, 0) + COALESCE(txn_q.sum_txn_branch_withdrawal_q, 0))/main.c_month_q sum_txn_branch_withdrawal_q,
            (COALESCE(all_txn.sum_txn_atm_deposit_m, 0) + COALESCE(txn_q.sum_txn_atm_deposit_q, 0))/main.c_month_q sum_txn_atm_deposit_q,
            (COALESCE(all_txn.sum_txn_branch_deposit_3p_m, 0) + COALESCE(txn_q.sum_txn_branch_deposit_3p_q, 0))/main.c_month_q sum_txn_branch_deposit_3p_q,
            (COALESCE(all_txn.c_txn_deposit_m, 0) + COALESCE(txn_q.c_txn_deposit_q, 0))/main.c_month_q c_txn_deposit_q,    
            (COALESCE(txn_card_owner.c_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.c_txn_withdrawal_m, 0)
            + COALESCE(txn_q.c_txn_atm_withdrawal_q, 0) + COALESCE(txn_q.c_txn_withdrawal_q, 0))/main.c_month_q c_txn_withdrawal_q,   
            (COALESCE(all_txn.c_txn_branch_deposit_m, 0) + COALESCE(txn_q.c_txn_branch_deposit_q, 0))/main.c_month_q c_txn_branch_deposit_q,
            (COALESCE(all_txn.c_txn_branch_withdrawal_m, 0) + COALESCE(txn_q.c_txn_branch_withdrawal_q, 0))/main.c_month_q c_txn_branch_withdrawal_q,
            (COALESCE(all_txn.c_txn_atm_deposit_m, 0) + COALESCE(txn_q.c_txn_atm_deposit_q, 0))/main.c_month_q c_txn_atm_deposit_q,
            (COALESCE(all_txn.c_txn_branch_deposit_3p_m, 0) + COALESCE(txn_q.c_txn_branch_deposit_3p_q, 0))/main.c_month_q c_txn_branch_deposit_3p_q, 
            
            GREATEST(COALESCE(all_txn.i_txn_deposit_m, 0), COALESCE(txn_y.i_txn_deposit_y, 0)) i_txn_deposit_y,
            GREATEST(COALESCE(txn_card_owner.i_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.i_txn_withdrawal_m, 0),
                     COALESCE(txn_y.i_txn_atm_withdrawal_y, 0) + COALESCE(txn_y.i_txn_withdrawal_y, 0)) i_txn_withdrawal_y,
            GREATEST(COALESCE(all_txn.i_txn_branch_deposit_m, 0), COALESCE(txn_y.i_txn_branch_deposit_y, 0)) i_txn_branch_deposit_y,
            GREATEST(COALESCE(all_txn.i_txn_branch_withdrawal_m, 0), COALESCE(txn_y.i_txn_branch_withdrawal_y, 0)) i_txn_branch_withdrawal_y,
            GREATEST(COALESCE(all_txn.i_txn_atm_deposit_m, 0), COALESCE(txn_y.i_txn_atm_deposit_y, 0)) i_txn_atm_deposit_y,
            GREATEST(COALESCE(all_txn.i_txn_branch_deposit_3p_m, 0), COALESCE(txn_y.i_txn_branch_deposit_3p_y, 0)) i_txn_branch_deposit_3p_y,               
            (COALESCE(all_txn.sum_cash_txn_m, 0) + COALESCE(txn_y.sum_cash_txn_y, 0))/main.c_month_y sum_cash_txn_y,
            (COALESCE(txn_card_owner.sum_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.sum_txn_withdrawal_m, 0)
            + COALESCE(txn_y.sum_txn_atm_withdrawal_y, 0) + COALESCE(txn_y.sum_txn_withdrawal_y, 0))/main.c_month_y sum_txn_withdrawal_y,   
            (COALESCE(all_txn.sum_txn_branch_deposit_m, 0) + COALESCE(txn_y.sum_txn_branch_deposit_y, 0))/main.c_month_y sum_txn_branch_deposit_y,
            (COALESCE(all_txn.sum_txn_branch_withdrawal_m, 0) + COALESCE(txn_y.sum_txn_branch_withdrawal_y, 0))/main.c_month_y sum_txn_branch_withdrawal_y,
            (COALESCE(all_txn.sum_txn_atm_deposit_m, 0) + COALESCE(txn_y.sum_txn_atm_deposit_y, 0))/main.c_month_y sum_txn_atm_deposit_y,
            (COALESCE(all_txn.sum_txn_branch_deposit_3p_m, 0) + COALESCE(txn_y.sum_txn_branch_deposit_3p_y, 0))/main.c_month_y sum_txn_branch_deposit_3p_y,
            (COALESCE(all_txn.c_txn_deposit_m, 0) + COALESCE(txn_y.c_txn_deposit_y, 0))/main.c_month_y c_txn_deposit_y,    
            (COALESCE(txn_card_owner.c_txn_atm_withdrawal_m, 0) + COALESCE(all_txn.c_txn_withdrawal_m, 0)
            + COALESCE(txn_y.c_txn_atm_withdrawal_y, 0) + COALESCE(txn_y.c_txn_withdrawal_y, 0))/main.c_month_y c_txn_withdrawal_y,   
            (COALESCE(all_txn.c_txn_branch_deposit_m, 0) + COALESCE(txn_y.c_txn_branch_deposit_y, 0))/main.c_month_y c_txn_branch_deposit_y,
            (COALESCE(all_txn.c_txn_branch_withdrawal_m, 0) + COALESCE(txn_y.c_txn_branch_withdrawal_y, 0))/main.c_month_y c_txn_branch_withdrawal_y,
            (COALESCE(all_txn.c_txn_atm_deposit_m, 0) + COALESCE(txn_y.c_txn_atm_deposit_y, 0))/main.c_month_y c_txn_atm_deposit_y,
            (COALESCE(all_txn.c_txn_branch_deposit_3p_m, 0) + COALESCE(txn_y.c_txn_branch_deposit_3p_y, 0))/main.c_month_y c_txn_branch_deposit_3p_y,  
               
            COALESCE(all_txn.median_credit_txn_lcy_m, 0) median_credit_txn_lcy_m,
            COALESCE(all_txn.median_credit_txn_fcy_m, 0) median_credit_txn_fcy_m,
            COALESCE(all_txn.median_debit_txn_lcy_m, 0) median_debit_txn_lcy_m,
            COALESCE(all_txn.median_debit_txn_fcy_m, 0) median_debit_txn_fcy_m,
            COALESCE(all_txn.c_txn_fcy_m, 0) c_txn_fcy_m,
            COALESCE(all_txn.c_txn_czk_m, 0) c_txn_czk_m,
            COALESCE(all_txn.c_txn_usd_m, 0) c_txn_usd_m,
            COALESCE(all_txn.c_txn_huf_m, 0) c_txn_huf_m,   
            COALESCE(all_txn.c_txn_gbp_m, 0) c_txn_gbp_m,
            COALESCE(all_txn.c_txn_pln_m, 0) c_txn_pln_m,
            COALESCE(all_txn.sum_txn_fcy_m, 0) sum_txn_fcy_m,
            COALESCE(all_txn.ratio_txn_fcy_m, 0) ratio_txn_fcy_m,
            COALESCE(all_txn.sum_txn_czk_m, 0) sum_txn_czk_m,
            COALESCE(all_txn.sum_txn_usd_m, 0) sum_txn_usd_m,
            COALESCE(all_txn.sum_txn_huf_m, 0) sum_txn_huf_m,   
            COALESCE(all_txn.sum_txn_gbp_m, 0) sum_txn_gbp_m,
            COALESCE(all_txn.sum_txn_pln_m, 0) sum_txn_pln_m,
            
            COALESCE(all_txn.c_txn_cdterminal_m, 0) c_txn_cdterminal_m,
            COALESCE(all_txn.c_txn_retailer_m, 0) c_txn_retailer_m,
            
            (COALESCE(all_txn.median_credit_txn_lcy_m, 0) + COALESCE(txn_q.median_credit_txn_lcy_q, 0))/main.c_month_q median_credit_txn_lcy_q,
            (COALESCE(all_txn.median_credit_txn_fcy_m, 0) + COALESCE(txn_q.median_credit_txn_fcy_q, 0))/main.c_month_q median_credit_txn_fcy_q,
            (COALESCE(all_txn.median_debit_txn_lcy_m, 0) + COALESCE(txn_q.median_debit_txn_lcy_q, 0))/main.c_month_q median_debit_txn_lcy_q,
            (COALESCE(all_txn.median_debit_txn_fcy_m, 0) + COALESCE(txn_q.median_debit_txn_fcy_q, 0))/main.c_month_q median_debit_txn_fcy_q,
            (COALESCE(all_txn.c_txn_fcy_m, 0) + COALESCE(txn_q.c_txn_fcy_q, 0))/main.c_month_q c_txn_fcy_q,
            (COALESCE(all_txn.c_txn_czk_m, 0) + COALESCE(txn_q.c_txn_czk_q, 0))/main.c_month_q c_txn_czk_q,
            (COALESCE(all_txn.c_txn_usd_m, 0) + COALESCE(txn_q.c_txn_usd_q, 0))/main.c_month_q c_txn_usd_q,
            (COALESCE(all_txn.c_txn_huf_m, 0) + COALESCE(txn_q.c_txn_huf_q, 0))/main.c_month_q c_txn_huf_q,   
            (COALESCE(all_txn.c_txn_gbp_m, 0) + COALESCE(txn_q.c_txn_gbp_q, 0))/main.c_month_q c_txn_gbp_q,
            (COALESCE(all_txn.c_txn_pln_m, 0) + COALESCE(txn_q.c_txn_pln_q, 0))/main.c_month_q c_txn_pln_q,
            (COALESCE(all_txn.sum_txn_fcy_m, 0) + COALESCE(txn_q.sum_txn_fcy_q, 0))/main.c_month_q sum_txn_fcy_q,   
            (COALESCE(all_txn.ratio_txn_fcy_m, 0) + COALESCE(txn_q.ratio_txn_fcy_q, 0))/main.c_month_q ratio_txn_fcy_q,        
            (COALESCE(all_txn.sum_txn_czk_m, 0) + COALESCE(txn_q.sum_txn_czk_q, 0))/main.c_month_q sum_txn_czk_q,
            (COALESCE(all_txn.sum_txn_usd_m, 0) + COALESCE(txn_q.sum_txn_usd_q, 0))/main.c_month_q sum_txn_usd_q,
            (COALESCE(all_txn.sum_txn_huf_m, 0) + COALESCE(txn_q.sum_txn_huf_q, 0))/main.c_month_q sum_txn_huf_q,   
            (COALESCE(all_txn.sum_txn_gbp_m, 0) + COALESCE(txn_q.sum_txn_gbp_q, 0))/main.c_month_q sum_txn_gbp_q,
            (COALESCE(all_txn.sum_txn_pln_m, 0) + COALESCE(txn_q.sum_txn_pln_q, 0))/main.c_month_q sum_txn_pln_q,
            
            (COALESCE(all_txn.c_txn_cdterminal_m, 0) + COALESCE(txn_q.c_txn_cdterminal_q, 0))/main.c_month_q c_txn_cdterminal_q,
            (COALESCE(all_txn.c_txn_retailer_m, 0) + COALESCE(txn_q.c_txn_retailer_q, 0))/main.c_month_q c_txn_retailer_q,
--        
            (COALESCE(all_txn.median_credit_txn_lcy_m, 0) + COALESCE(txn_y.median_credit_txn_lcy_y, 0))/main.c_month_y median_credit_txn_lcy_y,
            (COALESCE(all_txn.median_credit_txn_fcy_m, 0) + COALESCE(txn_y.median_credit_txn_fcy_y, 0))/main.c_month_y median_credit_txn_fcy_y,
            (COALESCE(all_txn.median_debit_txn_lcy_m, 0) + COALESCE(txn_y.median_debit_txn_lcy_y, 0))/main.c_month_y median_debit_txn_lcy_y,
            (COALESCE(all_txn.median_debit_txn_fcy_m, 0) + COALESCE(txn_y.median_debit_txn_fcy_y, 0))/main.c_month_y median_debit_txn_fcy_y,
            (COALESCE(all_txn.c_txn_fcy_m, 0) + COALESCE(txn_y.c_txn_fcy_y, 0))/main.c_month_y c_txn_fcy_y,
            (COALESCE(all_txn.c_txn_czk_m, 0) + COALESCE(txn_y.c_txn_czk_y, 0))/main.c_month_y c_txn_czk_y,
            (COALESCE(all_txn.c_txn_usd_m, 0) + COALESCE(txn_y.c_txn_usd_y, 0))/main.c_month_y c_txn_usd_y,
            (COALESCE(all_txn.c_txn_huf_m, 0) + COALESCE(txn_y.c_txn_huf_y, 0))/main.c_month_y c_txn_huf_y,   
            (COALESCE(all_txn.c_txn_gbp_m, 0) + COALESCE(txn_y.c_txn_gbp_y, 0))/main.c_month_y c_txn_gbp_y,
            (COALESCE(all_txn.c_txn_pln_m, 0) + COALESCE(txn_y.c_txn_pln_y, 0))/main.c_month_y c_txn_pln_y,
            (COALESCE(all_txn.sum_txn_fcy_m, 0) + COALESCE(txn_y.sum_txn_fcy_y, 0))/main.c_month_y sum_txn_fcy_y,   
            (COALESCE(all_txn.ratio_txn_fcy_m, 0) + COALESCE(txn_y.ratio_txn_fcy_y, 0))/main.c_month_y ratio_txn_fcy_y,        
            (COALESCE(all_txn.sum_txn_czk_m, 0) + COALESCE(txn_y.sum_txn_czk_y, 0))/main.c_month_y sum_txn_czk_y,
            (COALESCE(all_txn.sum_txn_usd_m, 0) + COALESCE(txn_y.sum_txn_usd_y, 0))/main.c_month_y sum_txn_usd_y,
            (COALESCE(all_txn.sum_txn_huf_m, 0) + COALESCE(txn_y.sum_txn_huf_y, 0))/main.c_month_y sum_txn_huf_y,   
            (COALESCE(all_txn.sum_txn_gbp_m, 0) + COALESCE(txn_y.sum_txn_gbp_y, 0))/main.c_month_y sum_txn_gbp_y,
            (COALESCE(all_txn.sum_txn_pln_m, 0) + COALESCE(txn_y.sum_txn_pln_y, 0))/main.c_month_y sum_txn_pln_y,
            
            (COALESCE(all_txn.c_txn_cdterminal_m, 0) + COALESCE(txn_y.c_txn_cdterminal_y, 0))/main.c_month_y c_txn_cdterminal_y,
            (COALESCE(all_txn.c_txn_retailer_m, 0) + COALESCE(txn_y.c_txn_retailer_y, 0))/main.c_month_y c_txn_retailer_y,
            
            COALESCE(ips_cred.c_ips_txn_credit_m, 0) c_ips_txn_credit_m,
            COALESCE(ips_cred.sum_ips_txn_credit_m, 0) sum_ips_txn_credit_m,
            COALESCE(ips_deb.c_ips_txn_debit_m, 0) c_ips_txn_debit_m,
            COALESCE(ips_deb.sum_ips_txn_debit_m, 0) sum_ips_txn_debit_m,
            LEAST((COALESCE(ips_cred.c_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.c_ips_txn_indiv_exrate_m, 0)), 1) i_ips_txn_indiv_exrate_m,
            (COALESCE(ips_cred.c_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.c_ips_txn_indiv_exrate_m, 0)) c_ips_txn_indiv_exrate_m,
            (COALESCE(ips_cred.sum_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.sum_ips_txn_indiv_exrate_m, 0)) sum_ips_txn_indiv_exrate_m,
            
            (COALESCE(txn_q.c_ips_txn_credit_q, 0) + COALESCE(ips_cred.c_ips_txn_credit_m, 0))/main.c_month_q c_ips_txn_credit_q,
            (COALESCE(txn_q.sum_ips_txn_credit_q, 0) + COALESCE(ips_cred.sum_ips_txn_credit_m, 0))/main.c_month_q sum_ips_txn_credit_q,
            (COALESCE(txn_q.c_ips_txn_debit_q, 0) + COALESCE(ips_deb.c_ips_txn_debit_m, 0))/main.c_month_q c_ips_txn_debit_q,
            (COALESCE(txn_q.sum_ips_txn_debit_q, 0) + COALESCE(ips_deb.c_ips_txn_debit_m, 0))/main.c_month_q sum_ips_txn_debit_q,
            GREATEST(LEAST((COALESCE(ips_cred.i_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.i_ips_txn_indiv_exrate_m, 0)), 1),
                COALESCE(txn_q.i_ips_txn_indiv_exrate_q, 0)) i_ips_txn_indiv_exrate_q,
            ((COALESCE(ips_cred.c_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.c_ips_txn_indiv_exrate_m, 0))
            + COALESCE(txn_q.c_ips_txn_indiv_exrate_q, 0))/main.c_month_q c_ips_txn_indiv_exrate_q,
            ((COALESCE(ips_cred.sum_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.sum_ips_txn_indiv_exrate_m, 0))
            + COALESCE(txn_q.sum_ips_txn_indiv_exrate_q, 0))/main.c_month_q sum_ips_txn_indiv_exrate_q,    
            
            (COALESCE(txn_y.c_ips_txn_credit_y, 0) + COALESCE(ips_cred.c_ips_txn_credit_m, 0))/main.c_month_y c_ips_txn_credit_y,
            (COALESCE(txn_y.sum_ips_txn_credit_y, 0) + COALESCE(ips_cred.sum_ips_txn_credit_m, 0))/main.c_month_y sum_ips_txn_credit_y,
            (COALESCE(txn_y.c_ips_txn_debit_y, 0) + COALESCE(ips_deb.c_ips_txn_debit_m, 0))/main.c_month_y c_ips_txn_debit_y,
            (COALESCE(txn_y.sum_ips_txn_debit_y, 0) + COALESCE(ips_deb.c_ips_txn_debit_m, 0))/main.c_month_y sum_ips_txn_debit_y,
            GREATEST(LEAST((COALESCE(ips_cred.i_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.i_ips_txn_indiv_exrate_m, 0)), 1),
                COALESCE(txn_y.i_ips_txn_indiv_exrate_y,0)) i_ips_txn_indiv_exrate_y,
            ((COALESCE(ips_cred.c_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.c_ips_txn_indiv_exrate_m, 0))
            + COALESCE(txn_y.c_ips_txn_indiv_exrate_y,0))/main.c_month_y c_ips_txn_indiv_exrate_y,
            ((COALESCE(ips_cred.sum_ips_txn_indiv_exrate_m, 0) + COALESCE(ips_deb.sum_ips_txn_indiv_exrate_m, 0))
            + COALESCE(txn_y.sum_ips_txn_indiv_exrate_y, 0))/main.c_month_y sum_ips_txn_indiv_exrate_y
            
        FROM (
            SELECT
                base.month_id,
                base.customerid,
                base.bank_tenure,
                LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -3)),1)*3,
                      COALESCE(txn_q.c_month, 0)+1) c_month_q,
                LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -12)),1)*12,
                      COALESCE(txn_y.c_month, 0)+1) c_month_y       
            FROM pm_owner.attr_client_base base
            LEFT JOIN (
                SELECT
                    customerid,
                    COUNT(DISTINCT month_id) c_month
                FROM pm_owner.attr_txn
                WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-2), 'YYYYMM'))
                AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                GROUP BY customerid) txn_q
                ON txn_q.customerid = base.customerid
                
            LEFT JOIN (
                SELECT
                    customerid,
                    COUNT(DISTINCT month_id) c_month
                FROM pm_owner.attr_txn
                WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-11), 'YYYYMM'))
                AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                GROUP BY customerid) txn_y
                ON txn_y.customerid = base.customerid                   
            WHERE base.month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))) main
        
        LEFT JOIN (
            -- card txn - accoutn owner
            SELECT
                account_owner customerid,
                MAX(CASE WHEN txn_card_isr_am_bc_am > 0 THEN 1 ELSE 0 END) i_txn_card_m,
                MAX(CASE WHEN txn_card_isr_am_bc_am*i_authorized_user > 0 THEN 1 ELSE 0 END) i_txn_card_authorized_user_m,
                MAX(CASE WHEN txn_cd_cd = 2717 THEN 1 ELSE 0 END) i_txn_atm_withdrawal_m,
                MAX(CASE WHEN card_type = 'debit' THEN 1 ELSE 0 END) i_debit_txn_card_m,
                MAX(CASE WHEN card_type = 'credit' THEN 1 ELSE 0 END) i_credit_txn_card_m,
                MAX(CASE WHEN txn_card_isr_am_bc_am*i_token > 0 THEN 1 ELSE 0 END) i_txn_card_token_m, 
                
                SUM(CASE WHEN crdb_in = 'D' THEN txn_card_isr_am_bc_am ELSE 0 END) sum_txn_card_m,
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am*i_authorized_user > 0 THEN txn_card_isr_am_bc_am ELSE 0 END) sum_txn_card_authorized_user_m,
                SUM(CASE WHEN txn_cd_cd = 2717 THEN txn_card_isr_am_bc_am ELSE 0 END) sum_txn_atm_withdrawal_m,
                SUM(CASE WHEN crdb_in = 'D' AND card_type = 'debit' THEN txn_card_isr_am_bc_am ELSE 0 END) sum_debit_txn_card_m,
                SUM(CASE WHEN crdb_in = 'D' AND card_type = 'credit' THEN txn_card_isr_am_bc_am ELSE 0 END) sum_credit_txn_card_m,
                SUM(CASE WHEN crdb_in = 'D' THEN txn_card_isr_am_bc_am*i_token ELSE 0 END) sum_txn_card_token_m,
                SUM(CASE WHEN crdb_in = 'D' THEN txn_card_isr_am_bc_am*i_applepay ELSE 0 END) sum_txn_card_token_apple_m,
                SUM(CASE WHEN crdb_in = 'D' THEN txn_card_isr_am_bc_am*i_googlepay ELSE 0 END) sum_txn_card_token_google_m,
                SUM(CASE WHEN crdb_in = 'D' THEN txn_card_isr_am_bc_am*i_merchant ELSE 0 END) sum_txn_card_token_merchant_m,   
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am > 0 THEN 1 ELSE 0 END) c_txn_card_m,
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am*i_authorized_user > 0 THEN 1 ELSE 0 END) c_txn_card_authorized_user_m,
                SUM(CASE WHEN txn_cd_cd = 2717 AND txn_card_isr_am_bc_am > 0 THEN 1 ELSE 0 END) c_txn_atm_withdrawal_m,
                SUM(CASE WHEN crdb_in = 'D' AND card_type = 'debit' AND txn_card_isr_am_bc_am > 0 THEN 1 ELSE 0 END) c_debit_txn_card_m,
                SUM(CASE WHEN crdb_in = 'D' AND card_type = 'credit' AND txn_card_isr_am_bc_am > 0 THEN 1 ELSE 0 END) c_credit_txn_card_m,
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am*i_token > 0 THEN 1 ELSE 0 END) c_txn_card_token_m,
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am*i_applepay > 0 THEN 1 ELSE 0 END) c_txn_card_token_apple_m,
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am*i_googlepay > 0 THEN 1 ELSE 0 END) c_txn_card_token_google_m,
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am*i_merchant > 0 THEN 1 ELSE 0 END) c_txn_card_token_merchant_m,  
                MEDIAN(CASE WHEN crdb_in = 'D' THEN txn_card_isr_am_bc_am ELSE NULL END) median_txn_card_m,
                MEDIAN(CASE WHEN txn_cd_cd = 2717 THEN txn_card_isr_am_bc_am ELSE NULL END) median_txn_atm_withdrawal_m,
                MEDIAN(CASE WHEN crdb_in = 'D' AND card_type = 'debit' THEN txn_card_isr_am_bc_am ELSE NULL END) median_debit_txn_card_m,
                MEDIAN(CASE WHEN crdb_in = 'D' AND card_type = 'credit' THEN txn_card_isr_am_bc_am ELSE NULL END) median_credit_txn_card_m,
                MEDIAN(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am*i_token > 0 THEN txn_card_isr_am_bc_am ELSE NULL END) median_txn_card_token_m,
                SUM(CASE WHEN crdb_in = 'C' THEN txn_card_isr_am_bc_am ELSE NULL END) sum_returned_txn_card_m,
                SUM(CASE WHEN crdb_in = 'C' THEN 1 ELSE 0 END) c_returned_txn_card_m,
                
                COUNT(DISTINCT geo_gen_iso_cntry) c_country_txn_card_m,
                COUNT(DISTINCT txn_card_isr_merch_src_key) c_merchant_txn_card_m                  
                
            FROM pm_owner.stg_txn
            GROUP BY account_owner) txn_card_owner
            ON main.customerid = txn_card_owner.customerid
        
        LEFT JOIN (
            -- card txn - authorized user
            SELECT
                authorized_user customerid,
                MAX(CASE WHEN txn_card_isr_am_bc_am > 0 THEN 1 ELSE 0 END) i_txn_card_holder_m,
                MAX(CASE WHEN card_type = 'credit' THEN 1 ELSE 0 END) i_credit_txn_card_holder_m,
                MAX(CASE WHEN txn_card_isr_am_bc_am*i_token > 0 THEN 1 ELSE 0 END) i_txn_card_token_holder_m,
                
                SUM(CASE WHEN crdb_in = 'D' THEN txn_card_isr_am_bc_am ELSE 0 END) sum_txn_card_holder_m,
                SUM(CASE WHEN crdb_in = 'D' AND card_type = 'credit' THEN txn_card_isr_am_bc_am ELSE 0 END) sum_credit_txn_card_holder_m,
                SUM(CASE WHEN crdb_in = 'D' THEN txn_card_isr_am_bc_am*i_token ELSE 0 END) sum_txn_card_token_holder_m,
                
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am > 0 THEN 1 ELSE 0 END) c_txn_card_holder_m,
                SUM(CASE WHEN crdb_in = 'D' AND card_type = 'credit' THEN 1 ELSE 0 END) c_credit_txn_card_holder_m,
                SUM(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am*i_token > 0 THEN 1 ELSE 0 END) c_txn_card_token_holder_m,
                
                MEDIAN(CASE WHEN crdb_in = 'D' AND txn_card_isr_am_bc_am > 0 THEN 1 ELSE NULL END) median_txn_card_holder_m    
            FROM pm_owner.stg_txn
            GROUP BY authorized_user) txn_card_holder
            ON main.customerid = txn_card_holder.customerid   
        
        LEFT JOIN (
            SELECT
                customerid_acct_owner customerid,
                MEDIAN(CASE WHEN src_syst_cd != 642 AND txn_crdb_in = 'C' THEN txn_bc_am ELSE NULL END) median_credit_txn_m,
                COUNT(CASE WHEN src_syst_cd != 642 AND txn_crdb_in = 'C' THEN txn_bc_am ELSE NULL END) c_credit_txn_m,
                SUM(CASE WHEN src_syst_cd != 642 AND txn_crdb_in = 'C' THEN txn_bc_am ELSE NULL END) sum_credit_txn_m,              
                MEDIAN(CASE WHEN src_syst_cd != 642 AND txn_crdb_in = 'D' THEN ABS(txn_bc_am)
                            WHEN src_syst_cd != 642 AND txn_crdb_in IS NULL AND src_syst_cd in (520) THEN ABS(txn_bc_am)
                            ELSE NULL END) median_debit_txn_m,
                COUNT(CASE WHEN src_syst_cd != 642 AND txn_crdb_in = 'D' THEN ABS(txn_bc_am)
                            WHEN src_syst_cd != 642 AND txn_crdb_in IS NULL AND src_syst_cd in (520) THEN ABS(txn_bc_am)
                            ELSE NULL END) c_debit_txn_m,
                SUM(CASE WHEN src_syst_cd != 642 AND txn_crdb_in = 'D' THEN ABS(txn_bc_am)
                            WHEN src_syst_cd != 642 AND txn_crdb_in IS NULL AND src_syst_cd in (520) THEN ABS(txn_bc_am)
                            ELSE NULL END) sum_debit_txn_m,           
                -- cash transactions
                MAX(CASE WHEN src_syst_cd = 642 AND txn_crdb_in = 'D' THEN 1 ELSE 0 END) i_txn_withdrawal_m,
                MAX(CASE WHEN src_syst_cd = 642 AND txn_crdb_in = 'C' THEN 1 ELSE 0 END) i_txn_deposit_m,
                MAX(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4368, 4433, 4401) THEN 1 ELSE 0 END) i_txn_branch_deposit_m,
                MAX(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4361, 4422) THEN 1 ELSE 0 END) i_txn_branch_withdrawal_m,
                MAX(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4512) THEN 1 ELSE 0 END) i_txn_atm_deposit_m,
                MAX(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4379) THEN 1 ELSE 0 END) i_txn_branch_deposit_3p_m,        
                
                SUM(CASE WHEN src_syst_cd = 642 AND txn_crdb_in = 'D' THEN -1*txn_bc_am ELSE txn_bc_am END) sum_cash_txn_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_crdb_in = 'D' THEN txn_bc_am ELSE 0 END) sum_txn_withdrawal_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4368, 4433, 4401) THEN txn_bc_am ELSE 0 END) sum_txn_branch_deposit_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4361, 4422) THEN txn_bc_am ELSE 0 END) sum_txn_branch_withdrawal_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4512) THEN txn_bc_am ELSE 0 END) sum_txn_atm_deposit_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4379) THEN txn_bc_am ELSE 0 END) sum_txn_branch_deposit_3p_m,
                
                SUM(CASE WHEN src_syst_cd = 642 AND txn_crdb_in = 'D' THEN 1 ELSE 0 END) c_txn_withdrawal_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_crdb_in = 'C' THEN 1 ELSE 0 END) c_txn_deposit_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4368, 4433, 4401) THEN 1 ELSE 0 END) c_txn_branch_deposit_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4361, 4422) THEN 1 ELSE 0 END) c_txn_branch_withdrawal_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4512) THEN 1 ELSE 0 END) c_txn_atm_deposit_m,
                SUM(CASE WHEN src_syst_cd = 642 AND txn_cd_cd in (4379) THEN 1 ELSE 0 END) c_txn_branch_deposit_3p_m,  
                -- transactions with exchange         
                MEDIAN(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'EUR' AND txn_crdb_in = 'C' THEN txn_bc_am ELSE NULL END) median_credit_txn_lcy_m,
                MEDIAN(CASE WHEN src_syst_cd != 642 AND curr_src_val != 'EUR' AND txn_crdb_in = 'C' THEN txn_bc_am ELSE NULL END) median_credit_txn_fcy_m,   
                MEDIAN(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'EUR' AND txn_crdb_in = 'D' THEN txn_bc_am ELSE NULL END) median_debit_txn_lcy_m,
                MEDIAN(CASE WHEN src_syst_cd != 642 AND curr_src_val != 'EUR' AND txn_crdb_in = 'D' THEN txn_bc_am ELSE NULL END) median_debit_txn_fcy_m,
                
                (SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val != 'EUR' THEN ABS(txn_bc_am) ELSE NULL END))
                /GREATEST(SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val != 'EUR' THEN ABS(txn_bc_am) ELSE NULL END)
                  + SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'EUR' THEN ABS(txn_bc_am) ELSE NULL END), 1) ratio_txn_fcy_m,        
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val != 'EUR' THEN 1 ELSE NULL END) c_txn_fcy_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'CZK' THEN 1 ELSE NULL END) c_txn_czk_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'USD' THEN 1 ELSE NULL END) c_txn_usd_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'HUF' THEN 1 ELSE NULL END) c_txn_huf_m,   
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'GBP' THEN 1 ELSE NULL END) c_txn_gbp_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'PLN' THEN 1 ELSE NULL END) c_txn_pln_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val != 'EUR' THEN ABS(txn_bc_am) ELSE NULL END) sum_txn_fcy_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'CZK' THEN ABS(txn_bc_am) ELSE NULL END) sum_txn_czk_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'USD' THEN ABS(txn_bc_am) ELSE NULL END) sum_txn_usd_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'HUF' THEN ABS(txn_bc_am) ELSE NULL END) sum_txn_huf_m,   
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'GBP' THEN ABS(txn_bc_am) ELSE NULL END) sum_txn_gbp_m,
                SUM(CASE WHEN src_syst_cd != 642 AND curr_src_val  = 'PLN' THEN ABS(txn_bc_am) ELSE NULL END) sum_txn_pln_m,
                
                SUM(CASE WHEN txn_crdb_in = 'D' and txn_dirctn_cd not in (9,10) and SUBSTR(txn_parm_val,1,3) in ('401','201') then 1 
                    ELSE 0 END) c_txn_cdterminal_m,               
                SUM(CASE WHEN txn_crdb_in = 'D' and txn_dirctn_cd not in (9,10) and SUBSTR(txn_parm_val,1,3) in ('403','405','203','205')  then 1 
                    ELSE 0 END) c_txn_retailer_m
            
            -- skontrolovať 642
            FROM (
                SELECT 
                    txn.txn_id,
                    cust.party_id customerid_acct_owner, 
                    txn_desc.txn_cd_gen_ds desc_txn,
                    txn.txn_cd_cd,
                    txn.txn_dt,
                    txn.chanl_id,
                    txn.txn_crdb_in,
                    txn.txn_dirctn_cd,
                    cg.chanl_gen_nm,
                    txn.txn_bc_am,
                    txn.txn_am,
                    curr.curr_src_val,
                    TP2.txn_parm_val,
                    txn.src_syst_cd
                
                FROM l0_owner.txn txn
                
                JOIN l0_owner.txn_cd_gen txn_desc
                    ON txn_desc.txn_cd_cd = txn.txn_cd_cd    
                    AND txn_desc.txn_cd_gen_end_dt = DATE '9999-12-31'   
                    AND txn.txn_dt <= process_dt 
                    AND txn.txn_dt > ADD_MONTHS(process_dt, -1)
                    AND txn.acct_id IS NOT NULL   
                
                JOIN l0_owner.cont cont
                    ON cont.cont_id = txn.acct_id
                
                JOIN l0_owner.party_cont pcont
                    ON pcont.cont_id = cont.cont_id 
                    AND pcont.party_cont_start_dt <= process_dt
                    AND pcont.party_cont_end_dt > process_dt
                    AND pcont.party_role_cd = 1 
                
                JOIN l0_owner.PARTY_RLTD cust
                    ON pcont.party_id = cust.child_party_id
                    AND cust.party_rltd_start_dt <= process_dt
                    AND cust.party_rltd_end_dt > process_dt
                    AND cust.party_rltd_rsn_cd = 4
                    
                LEFT JOIN L0_OWNER.TXN_PARM TP2
                    ON  TP2.txn_id = txn.txn_id
                    AND TP2.txn_dt = txn.txn_dt
                    AND TP2.txn_parm_type_cd = 68
                    
                LEFT JOIN l0_owner.curr curr
                    ON txn.curr_cd = curr.curr_cd
                    
                LEFT JOIN L0_OWNER.CHANL_GEN CG
                    ON  CG.chanl_id=txn.chanl_id
                    AND CG.chanl_gen_start_dt <= process_dt
                    AND CG.chanl_gen_end_dt > process_dt
                    
                LEFT JOIN l0_owner.txn_key tkey
                    ON tkey.txn_id = txn.txn_id
                    AND tkey.txn_dt <= process_dt 
                    AND tkey.txn_dt > ADD_MONTHS(process_dt, -1)
                    
                WHERE 1=1      
                    AND ((CASE WHEN txn.src_syst_cd = 501 AND txn.txn_cnt = 1 AND txn.txn_deal_no is not null and txn.paym_type_cd in (1, 3, 16, 19) THEN 1
                               WHEN txn.src_syst_cd = 510 AND txn.txn_cnt > 0 AND txn.txn_deal_no is not null and txn.txn_stat_cd = 0 and txn.paym_type_cd in (1, 3, 16, 19) THEN 1
                               WHEN txn.src_syst_cd = 520 AND COALESCE(txn.txn_cnt, 1) > 0 AND txn.txn_deal_no is not null and txn.paym_type_cd in (1, 3, 16, 19, 29) THEN 1
							   WHEN txn.src_syst_cd = 642 THEN 1
                               ELSE 0 END) = 1)
                )
            GROUP BY customerid_acct_owner) all_txn
            ON main.customerid = all_txn.customerid
        
        LEFT JOIN (
            SELECT
                credit_customerid customerid,
                COUNT(mps_ord_id) c_ips_txn_credit_m,
                SUM(mps_ord_bc_am) sum_ips_txn_credit_m,
                MAX(CASE WHEN mps_ord_cred_indiv_rate_in = 'I' THEN 1 ELSE 0 END) i_ips_txn_indiv_exrate_m,
                SUM(CASE WHEN mps_ord_cred_indiv_rate_in = 'I' THEN 1 ELSE 0 END) c_ips_txn_indiv_exrate_m,
                SUM(CASE WHEN mps_ord_cred_indiv_rate_in = 'I' THEN mps_ord_bc_am ELSE 0 END) sum_ips_txn_indiv_exrate_m
            FROM pm_owner.stg_mps
            GROUP BY credit_customerid) ips_cred
            ON main.customerid = ips_cred.customerid 
        
        LEFT JOIN (
            SELECT
                debit_customerid customerid,
                COUNT(mps_ord_id) c_ips_txn_debit_m,
                SUM(mps_ord_bc_am) sum_ips_txn_debit_m,
                MAX(CASE WHEN mps_ord_deb_indiv_rate_in = 'I' THEN 1 ELSE 0 END) i_ips_txn_indiv_exrate_m,
                SUM(CASE WHEN mps_ord_deb_indiv_rate_in = 'I' THEN 1 ELSE 0 END) c_ips_txn_indiv_exrate_m,
                SUM(CASE WHEN mps_ord_deb_indiv_rate_in = 'I' THEN mps_ord_bc_am ELSE 0 END) sum_ips_txn_indiv_exrate_m
            FROM pm_owner.stg_mps
            GROUP BY debit_customerid) ips_deb
            ON main.customerid = ips_deb.customerid 
            
        LEFT JOIN (
            SELECT
                customerid,
                COUNT(DISTINCT month_id) c_month_id_q,
                SUM(sum_txn_m) sum_txn_q,
                SUM(median_credit_txn_m) median_credit_txn_q,
                SUM(c_credit_txn_m) c_credit_txn_q,
                SUM(sum_credit_txn_m) sum_credit_txn_q,
                SUM(median_debit_txn_m) median_debit_txn_q,
                SUM(c_debit_txn_m) c_debit_txn_q,
                SUM(sum_debit_txn_m) sum_debit_txn_q,
            
                MAX(i_txn_card_m) i_txn_card_q,
                MAX(i_txn_card_authorized_user_m) i_txn_card_authorized_user_q,
                MAX(i_txn_atm_withdrawal_m) i_txn_atm_withdrawal_q,
                MAX(i_debit_txn_card_m) i_debit_txn_card_q,
                MAX(i_credit_txn_card_m) i_credit_txn_card_q,
                MAX(i_txn_card_token_m) i_txn_card_token_q,
                SUM(sum_txn_card_m) sum_txn_card_q,
                SUM(sum_txn_card_authorized_user_m) sum_txn_card_authorized_user_q,
                SUM(sum_txn_atm_withdrawal_m) sum_txn_atm_withdrawal_q,
                SUM(sum_debit_txn_card_m) sum_debit_txn_card_q,
                SUM(sum_credit_txn_card_m) sum_credit_txn_card_q,
                SUM(sum_txn_card_token_m) sum_txn_card_token_q,
                SUM(sum_txn_card_token_apple_m) sum_txn_card_token_apple_q,
                SUM(sum_txn_card_token_google_m) sum_txn_card_token_google_q,
                SUM(sum_txn_card_token_merchant_m) sum_txn_card_token_merchant_q,
                SUM(c_txn_card_m) c_txn_card_q,
                SUM(c_txn_card_authorized_user_m) c_txn_card_authorized_user_q,
                SUM(c_txn_atm_withdrawal_m) c_txn_atm_withdrawal_q,
                SUM(c_debit_txn_card_m) c_debit_txn_card_q,
                SUM(c_credit_txn_card_m) c_credit_txn_card_q,
                SUM(c_txn_card_token_m) c_txn_card_token_q,
                SUM(c_txn_card_token_apple_m) c_txn_card_token_apple_q,
                SUM(c_txn_card_token_google_m) c_txn_card_token_google_q,
                SUM(c_txn_card_token_merchant_m) c_txn_card_token_merchant_q,
                SUM(median_txn_card_m) median_txn_card_q,
                SUM(median_txn_atm_withdrawal_m) median_txn_atm_withdrawal_q,
                SUM(median_debit_txn_card_m) median_debit_txn_card_q,
                SUM(median_credit_txn_card_m) median_credit_txn_card_q,
                SUM(median_txn_card_token_m) median_txn_card_token_q,
                SUM(sum_returned_txn_card_m) sum_returned_txn_card_q,
                SUM(c_returned_txn_card_m) c_returned_txn_card_q,
                SUM(c_merchant_txn_card_m) c_merchant_txn_card_q,
                SUM(c_country_txn_card_m) c_country_txn_card_q,
                
            
                MAX(i_txn_card_holder_m) i_txn_card_holder_q,
                MAX(i_credit_txn_card_holder_m) i_credit_txn_card_holder_q,
                MAX(i_txn_card_token_holder_m) i_txn_card_token_holder_q,
                SUM(sum_txn_card_holder_m) sum_txn_card_holder_q,
                SUM(sum_credit_txn_card_holder_m) sum_credit_txn_card_holder_q,
                SUM(sum_txn_card_token_holder_m) sum_txn_card_token_holder_q,
                SUM(c_txn_card_holder_m) c_txn_card_holder_q,
                SUM(c_credit_txn_card_holder_m) c_credit_txn_card_holder_q,
                SUM(c_txn_card_token_holder_m) c_txn_card_token_holder_q,
                SUM(median_txn_card_holder_m) median_txn_card_holder_q,
            
                MAX(i_txn_deposit_m) i_txn_deposit_q,
                MAX(i_txn_withdrawal_m) i_txn_withdrawal_q,
                MAX(i_txn_branch_deposit_m) i_txn_branch_deposit_q,
                MAX(i_txn_branch_withdrawal_m) i_txn_branch_withdrawal_q,
                MAX(i_txn_atm_deposit_m) i_txn_atm_deposit_q,
                MAX(i_txn_branch_deposit_3p_m) i_txn_branch_deposit_3p_q,
                SUM(sum_cash_txn_m) sum_cash_txn_q,
                SUM(sum_txn_withdrawal_m) sum_txn_withdrawal_q,
                SUM(sum_txn_branch_deposit_m) sum_txn_branch_deposit_q,
                SUM(sum_txn_branch_withdrawal_m) sum_txn_branch_withdrawal_q,
                SUM(sum_txn_atm_deposit_m) sum_txn_atm_deposit_q,
                SUM(sum_txn_branch_deposit_3p_m) sum_txn_branch_deposit_3p_q,
                SUM(c_txn_deposit_m) c_txn_deposit_q,
                SUM(c_txn_withdrawal_m) c_txn_withdrawal_q,
                SUM(c_txn_branch_deposit_m) c_txn_branch_deposit_q,
                SUM(c_txn_branch_withdrawal_m) c_txn_branch_withdrawal_q,
                SUM(c_txn_atm_deposit_m) c_txn_atm_deposit_q,
                SUM(c_txn_branch_deposit_3p_m) c_txn_branch_deposit_3p_q,
            
                SUM(median_credit_txn_lcy_m) median_credit_txn_lcy_q,
                SUM(median_credit_txn_fcy_m) median_credit_txn_fcy_q,
                SUM(median_debit_txn_lcy_m) median_debit_txn_lcy_q,
                SUM(median_debit_txn_fcy_m) median_debit_txn_fcy_q,
                SUM(c_txn_fcy_m) c_txn_fcy_q,
                SUM(c_txn_czk_m) c_txn_czk_q,
                SUM(c_txn_usd_m) c_txn_usd_q,
                SUM(c_txn_huf_m) c_txn_huf_q,
                SUM(c_txn_gbp_m) c_txn_gbp_q,
                SUM(c_txn_pln_m) c_txn_pln_q,
                SUM(sum_txn_fcy_m) sum_txn_fcy_q,
                SUM(ratio_txn_fcy_m) ratio_txn_fcy_q,
                SUM(sum_txn_czk_m) sum_txn_czk_q,
                SUM(sum_txn_usd_m) sum_txn_usd_q,
                SUM(sum_txn_huf_m) sum_txn_huf_q,
                SUM(sum_txn_gbp_m) sum_txn_gbp_q,
                SUM(sum_txn_pln_m) sum_txn_pln_q,
                
                SUM(c_txn_cdterminal_m) c_txn_cdterminal_q,
                SUM(c_txn_retailer_m) c_txn_retailer_q,
                
                SUM(c_ips_txn_credit_m) c_ips_txn_credit_q,
                SUM(sum_ips_txn_credit_m) sum_ips_txn_credit_q,
                SUM(c_ips_txn_debit_m) c_ips_txn_debit_q,
                SUM(sum_ips_txn_debit_m) sum_ips_txn_debit_q,
                MAX(i_ips_txn_indiv_exrate_m) i_ips_txn_indiv_exrate_q,
                SUM(c_ips_txn_indiv_exrate_m) c_ips_txn_indiv_exrate_q,
                SUM(sum_ips_txn_indiv_exrate_m) sum_ips_txn_indiv_exrate_q
                
            
            FROM pm_owner.attr_txn
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -2), 'YYYYMM'))
                           AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) txn_q
            ON main.customerid = txn_q.customerid
            
        LEFT JOIN (SELECT
                customerid,
                COUNT(DISTINCT month_id) c_month_id_y,
                SUM(sum_txn_m) sum_txn_y,
                SUM(median_credit_txn_m) median_credit_txn_y,
                SUM(c_credit_txn_m) c_credit_txn_y,
                SUM(sum_credit_txn_m) sum_credit_txn_y,
                SUM(median_debit_txn_m) median_debit_txn_y,
                SUM(c_debit_txn_m) c_debit_txn_y,
                SUM(sum_debit_txn_m) sum_debit_txn_y,
        
                MAX(i_txn_card_m) i_txn_card_y,
                MAX(i_txn_card_authorized_user_m) i_txn_card_authorized_user_y,
                MAX(i_txn_atm_withdrawal_m) i_txn_atm_withdrawal_y,
                MAX(i_debit_txn_card_m) i_debit_txn_card_y,
                MAX(i_credit_txn_card_m) i_credit_txn_card_y,
                MAX(i_txn_card_token_m) i_txn_card_token_y,
                SUM(sum_txn_card_m) sum_txn_card_y,
                SUM(sum_txn_card_authorized_user_m) sum_txn_card_authorized_user_y,
                SUM(sum_txn_atm_withdrawal_m) sum_txn_atm_withdrawal_y,
                SUM(sum_debit_txn_card_m) sum_debit_txn_card_y,
                SUM(sum_credit_txn_card_m) sum_credit_txn_card_y,
                SUM(sum_txn_card_token_m) sum_txn_card_token_y,
                SUM(sum_txn_card_token_apple_m) sum_txn_card_token_apple_y,
                SUM(sum_txn_card_token_google_m) sum_txn_card_token_google_y,
                SUM(sum_txn_card_token_merchant_m) sum_txn_card_token_merchant_y,
                SUM(c_txn_card_m) c_txn_card_y,
                SUM(c_txn_card_authorized_user_m) c_txn_card_authorized_user_y,
                SUM(c_txn_atm_withdrawal_m) c_txn_atm_withdrawal_y,
                SUM(c_debit_txn_card_m) c_debit_txn_card_y,
                SUM(c_credit_txn_card_m) c_credit_txn_card_y,
                SUM(c_txn_card_token_m) c_txn_card_token_y,
                SUM(c_txn_card_token_apple_m) c_txn_card_token_apple_y,
                SUM(c_txn_card_token_google_m) c_txn_card_token_google_y,
                SUM(c_txn_card_token_merchant_m) c_txn_card_token_merchant_y,
                SUM(median_txn_card_m) median_txn_card_y,
                SUM(median_txn_atm_withdrawal_m) median_txn_atm_withdrawal_y,
                SUM(median_debit_txn_card_m) median_debit_txn_card_y,
                SUM(median_credit_txn_card_m) median_credit_txn_card_y,
                SUM(median_txn_card_token_m) median_txn_card_token_y,
                SUM(sum_returned_txn_card_m) sum_returned_txn_card_y,
                SUM(c_returned_txn_card_m) c_returned_txn_card_y,
                SUM(c_merchant_txn_card_m) c_merchant_txn_card_y,
                SUM(c_country_txn_card_m) c_country_txn_card_y,
        
                MAX(i_txn_card_holder_m) i_txn_card_holder_y,
                MAX(i_credit_txn_card_holder_m) i_credit_txn_card_holder_y,
                MAX(i_txn_card_token_holder_m) i_txn_card_token_holder_y,
                SUM(sum_txn_card_holder_m) sum_txn_card_holder_y,
                SUM(sum_credit_txn_card_holder_m) sum_credit_txn_card_holder_y,
                SUM(sum_txn_card_token_holder_m) sum_txn_card_token_holder_y,
                SUM(c_txn_card_holder_m) c_txn_card_holder_y,
                SUM(c_credit_txn_card_holder_m) c_credit_txn_card_holder_y,
                SUM(c_txn_card_token_holder_m) c_txn_card_token_holder_y,
                SUM(median_txn_card_holder_m) median_txn_card_holder_y,
        
                MAX(i_txn_deposit_m) i_txn_deposit_y,
                MAX(i_txn_withdrawal_m) i_txn_withdrawal_y,
                MAX(i_txn_branch_deposit_m) i_txn_branch_deposit_y,
                MAX(i_txn_branch_withdrawal_m) i_txn_branch_withdrawal_y,
                MAX(i_txn_atm_deposit_m) i_txn_atm_deposit_y,
                MAX(i_txn_branch_deposit_3p_m) i_txn_branch_deposit_3p_y,
                SUM(sum_cash_txn_m) sum_cash_txn_y,
                SUM(sum_txn_withdrawal_m) sum_txn_withdrawal_y,
                SUM(sum_txn_branch_deposit_m) sum_txn_branch_deposit_y,
                SUM(sum_txn_branch_withdrawal_m) sum_txn_branch_withdrawal_y,
                SUM(sum_txn_atm_deposit_m) sum_txn_atm_deposit_y,
                SUM(sum_txn_branch_deposit_3p_m) sum_txn_branch_deposit_3p_y,
                SUM(c_txn_deposit_m) c_txn_deposit_y,
                SUM(c_txn_withdrawal_m) c_txn_withdrawal_y,
                SUM(c_txn_branch_deposit_m) c_txn_branch_deposit_y,
                SUM(c_txn_branch_withdrawal_m) c_txn_branch_withdrawal_y,
                SUM(c_txn_atm_deposit_m) c_txn_atm_deposit_y,
                SUM(c_txn_branch_deposit_3p_m) c_txn_branch_deposit_3p_y,
        
                SUM(median_credit_txn_lcy_m) median_credit_txn_lcy_y,
                SUM(median_credit_txn_fcy_m) median_credit_txn_fcy_y,
                SUM(median_debit_txn_lcy_m) median_debit_txn_lcy_y,
                SUM(median_debit_txn_fcy_m) median_debit_txn_fcy_y,
                SUM(c_txn_fcy_m) c_txn_fcy_y,
                SUM(c_txn_czk_m) c_txn_czk_y,
                SUM(c_txn_usd_m) c_txn_usd_y,
                SUM(c_txn_huf_m) c_txn_huf_y,
                SUM(c_txn_gbp_m) c_txn_gbp_y,
                SUM(c_txn_pln_m) c_txn_pln_y,
                SUM(sum_txn_fcy_m) sum_txn_fcy_y,
                SUM(ratio_txn_fcy_m) ratio_txn_fcy_y,
                SUM(sum_txn_czk_m) sum_txn_czk_y,
                SUM(sum_txn_usd_m) sum_txn_usd_y,
                SUM(sum_txn_huf_m) sum_txn_huf_y,
                SUM(sum_txn_gbp_m) sum_txn_gbp_y,
                SUM(sum_txn_pln_m) sum_txn_pln_y,
                
                SUM(c_txn_cdterminal_m) c_txn_cdterminal_y,
                SUM(c_txn_retailer_m) c_txn_retailer_y,
                
                SUM(c_ips_txn_credit_m) c_ips_txn_credit_y,
                SUM(sum_ips_txn_credit_m) sum_ips_txn_credit_y,
                SUM(c_ips_txn_debit_m) c_ips_txn_debit_y,
                SUM(sum_ips_txn_debit_m) sum_ips_txn_debit_y,
                MAX(i_ips_txn_indiv_exrate_m) i_ips_txn_indiv_exrate_y,
                SUM(c_ips_txn_indiv_exrate_m) c_ips_txn_indiv_exrate_y,
                SUM(sum_ips_txn_indiv_exrate_m) sum_ips_txn_indiv_exrate_y
                
            FROM pm_owner.attr_txn
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -11), 'YYYYMM'))
                           AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) txn_y
            ON main.customerid = txn_y.customerid;
   
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);
    
    END;

  PROCEDURE append_terminal(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_terminal';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'TERMINAL';

    BEGIN 
 	-- TERMINAL
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    INSERT INTO pm_owner.terminal
        SELECT
            DISTINCT
            TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM')) month_id,
            PR.party_id customerid,
            NVL (T.TERMNL_SRC_VAL, EHT.CD_CDS_TERMINAL_SRC_KEY) AS cd_terminal,
            CASE WHEN SUBSTR(NVL(T.TERMNL_SRC_VAL, EHT.CD_CDS_TERMINAL_SRC_KEY), 1, 4) = 'SOFP' THEN 'SOFP' ELSE EHT.CD_TERMINAL_TYPE END type_terminal,
            EHT.D_TERMINAL_CONTRACT_SIGN AS sign_dt,
            EHT.D_TERMINAL_INSTALL AS install_dt,
            EHT.D_TERMINAL_DEINSTALL AS deinstall_dt,
            AG.curr_cd currency,
            ACCT_GEN_IBAN iban
        FROM (
            SELECT
                CD_CDS_TERMINAL_SRC_KEY,
                ID_CDS_TERMINAL,
                CD_TERMINAL_TYPE,
                D_TERMINAL_CONTRACT_SIGN,
                D_TERMINAL_INSTALL,
                D_TERMINAL_DEINSTALL,
                IDH_MERCHANT,
                ROW_NUMBER() OVER(PARTITION BY cd_terminal ORDER BY cd_terminal_status DESC, pdt_upd DESC, pdt_ins DESC) row_num
            FROM L1_OWNER.DM_EH_TERMINAL
            WHERE 1=1
            AND f_csob_terminal = 'Y') EHT
            
        JOIN L1_OWNER.DM_EH_MERCHANT EHM
           ON EHM.IDH_MERCHANT = EHT.IDH_MERCHANT
              
        JOIN L1_OWNER.DM_EH_CONTRACT HC
           ON HC.IDH_CONTRACT = EHM.IDH_CONTRACT
           
        JOIN L0_OWNER.ACCT_GEN AG
           ON AG.ACCT_ID = HC.ID_CDS_CONTRACT
           AND AG.ACCT_GEN_START_DT <= process_dt
           AND AG.ACCT_GEN_END_DT > process_dt
              
        JOIN L0_OWNER.PARTY_CONT PC
           ON PC.cont_id = AG.acct_id
           AND PC.party_cont_end_dt > process_dt
           AND PC.PARTY_ROLE_CD = 1
              
        JOIN L0_OWNER.PARTY P
           ON p.party_id = PC.party_id
            AND P.src_syst_cd in (501, 510, 520)
            
        LEFT JOIN L0_OWNER.TERMNL T
           ON T.TERMNL_ID = EHT.ID_CDS_TERMINAL
                              
        LEFT JOIN L0_OWNER.PARTY_RLTD PR
            ON  PR.child_party_id =  PC.party_id
            AND PR.party_rltd_start_dt <= process_dt
            AND PR.party_rltd_end_dt > process_dt
            AND PR.party_rltd_rsn_cd = 4
           
        WHERE 1=1
        AND EHT.row_num = 1
        AND EHT.D_TERMINAL_DEINSTALL > process_dt_hist;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);

    END;   
    
  PROCEDURE append_terminal_txn_type_list(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_terminal_txn_type_list';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'TERMINAL_TXN_TYPE_LIST';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);

  BEGIN
	
	-- TERMINAL PAYMENT TYPE
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    INSERT /*+ APPEND */ INTO PM_OWNER.TERMINAL_TXN_TYPE_LIST (
        MONTH_ID,
        FROM_DT,
        TO_DT,
        MCC,
        TRANSACTION_CATEGORY,
        CONTACTLESS_INDICATOR,
        MERCHANT_NUMBER,
        CD_TERMINAL,
        MERCHANT_NAME,
        TOWN,
        POSTCODE,
        ACCOUNT,
        BANK,
        CC,
        BIN,
        TXN_TYPE,
        C_MC_TXN,
        SUM_MC_TXN,
        PROVISION_MC,
        C_MC_TXN_CSOB,
        SUM_MC_TXN_CSOB,
        PROVISION_MC_CSOB,
        C_MC_TXN_DOMESTIC,
        SUM_MC_TXN_DOMESTIC,
        PROVISION_MC_DOMESTIC,
        C_MC_TXN_EU,
        SUM_MC_TXN_EU,
        PROVISION_MC_EU,
        C_MC_TXN_NONEU,
        SUM_MC_TXN_NONEU,
        PROVISION_MC_NONEU,
        C_MEASTRO_TXN,
        SUM_MEASTRO_TXN,
        PROVISION_MEASTRO,
        C_MAESTRO_TXN_CSOB,
        SUM_MAESTRO_TXN_CSOB,
        PROVISION_MAESTRO_CSOB,
        C_MAESTRO_TXN_DOMESTIC,
        SUM_MAESTRO_TXN_DOMESTIC,
        PROVISION_MAESTRO_DOMESTIC,
        C_MAESTRO_TXN_EU,
        SUM_MAESTRO_TXN_EU,
        PROVISION_MAESTRO_EU,
        C_MAESTRO_TXN_NONEU,
        SUM_MAESTRO_TXN_NONEU,
        PROVISION_MAESTRO_NONEU,
        C_VISA_TXN,
        SUM_VISA_TXN,
        PROVISION_VISA,
        C_VISA_TXN_CSOB,
        SUM_VISA_TXN_CSOB,
        PROVISION_VISA_CSOB,
        C_VISA_TXN_DOMESTIC,
        SUM_VISA_TXN_DOMESTIC,
        PROVISION_VISA_DOMESTIC,
        C_VISA_TXN_EU,
        SUM_VISA_TXN_EU,
        PROVISION_VISA_EU,
        C_VISA_TXN_NONEU,
        SUM_VISA_TXN_NONEU,
        PROVISION_VISA_NONEU,
        SUM_CASHBACK,
        C_CASHBACK,
        SUM_INTERCHANGE,
        C_INTERCHANGE,
        C_DINERS_TXN,
        SUM_DINERS_TXN,
        PROVISION_DINERS,
        C_TXN,
        SUM_TXN,
        PROVISION)
    SELECT
        TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM')) MOTNH_ID,
        "Datum od",
        "Datum do",
        "MCC",
        "Transaction Category",
        "Contactless indikator",
        "Cislo obchodnika",
        "Terminal ID",
        "Jmeno obchodnika",
        "Mesto",
        "PSC",
        "Ucet",
        "Banka",
        "CC",
        "BIN",
        "Typ transakce",
        SUM("MasterC-Pocet"),
        SUM("MasterC-Objem"),
        SUM("MasterC-Provize"),
        SUM("MasterC ONUS-Pocet"),
        SUM("MasterC ONUS-Objem"),
        SUM("MasterC ONUS-Provize"),
        SUM("MasterC DOMES-Pocet"),
        SUM("MasterC DOMES-Objem"),
        SUM("MasterC DOMES-Provize"),
        SUM("MasterC INTRA-Pocet"),
        SUM("MasterC INTRA-Objem"),
        SUM("MasterC INTRA-Provize"),
        SUM("MasterC INTER-Pocet"),
        SUM("MasterC INTER-Objem"),
        SUM("MasterC INTER-Provize"),
        SUM("Maestro-Pocet"),
        SUM("Maestro-Objem"),
        SUM("Maestro-Provize"),
        SUM("Maestro ONUS-Pocet"),
        SUM("Maestro ONUS-Objem"),
        SUM("Maestro ONUS-Provize"),
        SUM("Maestro DOMES-Pocet"),
        SUM("Maestro DOMES-Objem"),
        SUM("Maestro DOMES-Provize"),
        SUM("Maestro INTRA-Pocet"),
        SUM("Maestro INTRA-Objem"),
        SUM("Maestro INTRA-Provize"),
        SUM("Maestro INTER-Pocet"),
        SUM("Maestro INTER-Objem"),
        SUM("Maestro INTER-Provize"),
        SUM("VISA-Pocet"),
        SUM("VISA-Objem"),
        SUM("VISA-Provize"),
        SUM("VISA ONUS-Pocet"),
        SUM("VISA ONUS-Objem"),
        SUM("VISA ONUS-Provize"),
        SUM("VISA DOMES-Pocet"),
        SUM("VISA DOMES-Objem"),
        SUM("VISA DOMES-Provize"),
        SUM("VISA INTRA-Pocet"),
        SUM("VISA INTRA-Objem"),
        SUM("VISA INTRA-Provize"),
        SUM("VISA INTER-Pocet"),
        SUM("VISA INTER-Objem"),
        SUM("VISA INTER-Provize"),
        SUM("CashBack-Objem"    ),
        SUM("CashBack-Pocet"),
        SUM("Interchange-Objem" ),
        SUM("Interchange-Pocet"),
        SUM("Diners C-Pocet"),
        SUM("Diners C-Objem"),
        SUM("Diners C-Provize"),
        SUM("CELKEM-Pocet"),
        SUM("CELKEM-Objem"),
        SUM("CELKEM-Provize" )
    FROM TABLE (DMSK_PORTAL.P_1221  (l_month_start_dt, l_month_end_dt))
    WHERE CC <> '?'
    GROUP BY
        TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM')),
        "Datum od",
        "Datum do",
        "MCC",
        "Transaction Category",
        "Contactless indikator",
        "Cislo obchodnika",
        "Terminal ID",
        "Jmeno obchodnika",
        "Mesto",
        "PSC",
        "Ucet",
        "Banka",
        "CC",
        "BIN",
        "Typ transakce"          
    ;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;               
    mng_stats(l_owner_name, l_table_name, l_proc_name);
    
    END;

  PROCEDURE append_attr_terminal(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_attr_terminal';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_TERMINAL';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);

  BEGIN
    
    -- ATTR_TERMINAL
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

    INSERT INTO PM_OWNER.ATTR_TERMINAL
        SELECT
            main.month_id,
            main.customerid,
            
            COALESCE(term.i_opened_posterminal_m, 0) i_opened_posterminal,                           
            COALESCE(term.i_closed_posterminal_m, 0) i_closed_posterminal, 
            COALESCE(term.i_active_posterminal_m, 0) i_active_posterminal,              
            COALESCE(term.i_opened_smartterminal_m, 0) i_opened_smartterminal,                             
            COALESCE(term.i_closed_smartterminal_m, 0) i_closed_smartterminal, 
            COALESCE(term.i_active_smartterminal_m, 0) i_active_smartterminal,                    
        
            GREATEST(COALESCE(term_q.i_opened_posterminal_q, 0), COALESCE(term.i_opened_posterminal_m, 0)) i_opened_posterminal_q,
            GREATEST(COALESCE(term_q.i_closed_posterminal_q, 0), COALESCE(term.i_closed_posterminal_m, 0)) i_closed_posterminal_q,
            GREATEST(COALESCE(term_q.i_active_posterminal_q, 0), COALESCE(term.i_active_posterminal_m, 0)) i_active_posterminal_q,
            GREATEST(COALESCE(term_q.i_opened_smartterminal_q, 0), COALESCE(term.i_opened_smartterminal_m, 0)) i_opened_smartterminal_q,
            GREATEST(COALESCE(term_q.i_closed_smartterminal_q, 0), COALESCE(term.i_closed_smartterminal_m, 0)) i_closed_smartterminal_q,
            GREATEST(COALESCE(term_q.i_active_smartterminal_q, 0), COALESCE(term.i_active_smartterminal_m, 0)) i_active_smartterminal_q,
        
            GREATEST(COALESCE(term_y.i_opened_posterminal_y, 0), COALESCE(term.i_opened_posterminal_m, 0)) i_opened_posterminal_y,
            GREATEST(COALESCE(term_y.i_closed_posterminal_y, 0), COALESCE(term.i_closed_posterminal_m, 0)) i_closed_posterminal_y,
            GREATEST(COALESCE(term_y.i_active_posterminal_y, 0), COALESCE(term.i_active_posterminal_m, 0)) i_active_posterminal_y,
            GREATEST(COALESCE(term_y.i_opened_smartterminal_y, 0), COALESCE(term.i_opened_smartterminal_m, 0)) i_opened_smartterminal_y,
            GREATEST(COALESCE(term_y.i_closed_smartterminal_y, 0), COALESCE(term.i_closed_smartterminal_m, 0)) i_closed_smartterminal_y,
            GREATEST(COALESCE(term_y.i_active_smartterminal_y, 0), COALESCE(term.i_active_smartterminal_m, 0)) i_active_smartterminal_y,
            
                -- POS + MPOS 
            COALESCE(term.i_total_terminal, 0) i_total_terminal,     
            COALESCE(term.c_active_terminal, 0) c_active_terminal, 
            COALESCE(term.recency_opened_terminal, process_dt - process_dt_hist) recency_opened_terminal,   
            COALESCE(term.recency_closed_terminal, process_dt - process_dt_hist) recency_closed_terminal,
                
                -- webpay
            COALESCE(term.i_opened_webpay_m, 0) i_opened_webpay_m,                             
            COALESCE(term.i_closed_webpay_m, 0) i_closed_webpay_m, 
            COALESCE(term.i_active_webpay_m, 0) i_active_webpay_m, 
            
            GREATEST(COALESCE(term_q.i_opened_webpay_q, 0), COALESCE(term.i_opened_webpay_m, 0)) i_opened_webpay_q,
            GREATEST(COALESCE(term_q.i_closed_webpay_q, 0), COALESCE(term.i_closed_webpay_m, 0)) i_closed_webpay_q,
            GREATEST(COALESCE(term_q.i_active_webpay_q, 0), COALESCE(term.i_active_webpay_m, 0)) i_active_webpay_q,    
            
            GREATEST(COALESCE(term_y.i_opened_webpay_y, 0), COALESCE(term.i_opened_webpay_m, 0)) i_opened_webpay_y,
            GREATEST(COALESCE(term_y.i_closed_webpay_y, 0), COALESCE(term.i_closed_webpay_m, 0)) i_closed_webpay_y,
            GREATEST(COALESCE(term_y.i_active_webpay_y, 0), COALESCE(term.i_active_webpay_m, 0)) i_active_webpay_y, 
            
            COALESCE(term.i_total_webpay, 0) i_total_webpay, 
            COALESCE(term.c_active_webpay, 0) c_active_webpay, 
            COALESCE(term.recency_opened_webpay, process_dt - process_dt_hist) recency_opened_webpay,   
            COALESCE(term.recency_closed_webpay, process_dt - process_dt_hist) recency_closed_webpay,                       
            
            COALESCE(c_txn_terminal_m, 0) c_txn_terminal_m,   
            COALESCE(avg_txn_terminal_m, 0) avg_txn_terminal_m,
            COALESCE(sum_provision_terminal_m, 0) sum_provision_terminal_m, 
            COALESCE(avg_c_txn_terminal_m, 0) avg_c_txn_terminal_m,
            COALESCE(foreign_ratio_c_terminal_m, 0) foreign_ratio_c_terminal_m, 
            COALESCE(foreign_ratio_sum_terminal_m, 0) foreign_ratio_sum_terminal_m,
        
            (COALESCE(term_q.c_txn_terminal_q, 0) + COALESCE(term.c_txn_terminal_m, 0))/main.c_month_q c_txn_terminal_q,
            (COALESCE(term_q.avg_txn_terminal_q, 0) + COALESCE(term.avg_txn_terminal_m, 0))/main.c_month_q avg_txn_terminal_q,
            (COALESCE(term_q.sum_provision_terminal_q, 0) + COALESCE(term.sum_provision_terminal_m, 0))/main.c_month_q sum_provision_terminal_q,
            (COALESCE(term_q.avg_c_txn_terminal_q, 0) + COALESCE(term.avg_c_txn_terminal_m, 0))/main.c_month_q avg_c_txn_terminal_q,
            (COALESCE(term_q.foreign_ratio_c_terminal_q, 0) + COALESCE(term.foreign_ratio_c_terminal_m, 0))/main.c_month_q foreign_ratio_c_terminal_q,
            (COALESCE(term_q.foreign_ratio_sum_terminal_q, 0) + COALESCE(term.foreign_ratio_sum_terminal_m, 0))/main.c_month_q foreign_ratio_sum_terminal_q,
        
            (COALESCE(term_y.c_txn_terminal_y, 0) + COALESCE(term.c_txn_terminal_m, 0))/main.c_month_y c_txn_terminal_y,
            (COALESCE(term_y.avg_txn_terminal_y, 0) + COALESCE(term.avg_txn_terminal_m, 0))/main.c_month_y avg_txn_terminal_y,
            (COALESCE(term_y.sum_provision_terminal_y, 0) + COALESCE(term.sum_provision_terminal_m, 0))/main.c_month_y sum_provision_terminal_y,
            (COALESCE(term_y.avg_c_txn_terminal_y, 0) + COALESCE(term.avg_c_txn_terminal_m, 0))/main.c_month_y avg_c_txn_terminal_y,
            (COALESCE(term_y.foreign_ratio_c_terminal_y, 0) + COALESCE(term.foreign_ratio_c_terminal_m, 0))/main.c_month_y foreign_ratio_c_terminal_y,
            (COALESCE(term_y.foreign_ratio_sum_terminal_y, 0) + COALESCE(term.foreign_ratio_sum_terminal_m, 0))/main.c_month_y foreign_ratio_sum_terminal_y,
            
            COALESCE(c_txn_webpay_m, 0) c_txn_webpay_m,   
            COALESCE(avg_txn_webpay_m, 0) avg_txn_webpay_m,
            COALESCE(sum_provision_webpay_m, 0) sum_provision_webpay_m, 
            COALESCE(avg_c_txn_webpay_m, 0) avg_c_txn_webpay_m,
            COALESCE(foreign_ratio_c_webpay_m, 0) foreign_ratio_c_webpay_m, 
            COALESCE(foreign_ratio_sum_webpay_m, 0) foreign_ratio_sum_webpay_m,
        
            (COALESCE(term_q.c_txn_webpay_q, 0) + COALESCE(term.c_txn_webpay_m, 0))/main.c_month_q c_txn_webpay_q,
            (COALESCE(term_q.avg_txn_webpay_q, 0) + COALESCE(term.avg_txn_webpay_m, 0))/main.c_month_q avg_txn_webpay_q,
            (COALESCE(term_q.sum_provision_webpay_q, 0) + COALESCE(term.sum_provision_webpay_m, 0))/main.c_month_q sum_provision_webpay_q,
            (COALESCE(term_q.avg_c_txn_webpay_q, 0) + COALESCE(term.avg_c_txn_webpay_m, 0))/main.c_month_q avg_c_txn_webpay_q,
            (COALESCE(term_q.foreign_ratio_c_webpay_q, 0) + COALESCE(term.foreign_ratio_c_webpay_m, 0))/main.c_month_q foreign_ratio_c_webpay_q,
            (COALESCE(term_q.foreign_ratio_sum_webpay_q, 0) + COALESCE(term.foreign_ratio_sum_webpay_m, 0))/main.c_month_q foreign_ratio_sum_webpay_q,
        
            (COALESCE(term_y.c_txn_webpay_y, 0) + COALESCE(term.c_txn_webpay_m, 0))/main.c_month_y c_txn_webpay_y,
            (COALESCE(term_y.avg_txn_webpay_y, 0) + COALESCE(term.avg_txn_webpay_m, 0))/main.c_month_y avg_txn_webpay_y,
            (COALESCE(term_y.sum_provision_webpay_y, 0) + COALESCE(term.sum_provision_webpay_m, 0))/main.c_month_y sum_provision_webpay_y,
            (COALESCE(term_y.avg_c_txn_webpay_y, 0) + COALESCE(term.avg_c_txn_webpay_m, 0))/main.c_month_y avg_c_txn_webpay_y,
            (COALESCE(term_y.foreign_ratio_c_webpay_y, 0) + COALESCE(term.foreign_ratio_c_webpay_m, 0))/main.c_month_y foreign_ratio_c_webpay_y,
            (COALESCE(term_y.foreign_ratio_sum_webpay_y, 0) + COALESCE(term.foreign_ratio_sum_webpay_m, 0))/main.c_month_y foreign_ratio_sum_webpay_y
        
        FROM (
            SELECT
                base.month_id,
                base.customerid,
                base.bank_tenure,
                LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -3)),1)*3,
                      COALESCE(term_q.c_month, 0)+1) c_month_q,
                LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -12)),1)*12,
                      COALESCE(term_y.c_month, 0)+1) c_month_y       
            FROM pm_owner.attr_client_base base
            LEFT JOIN (
                SELECT
                    customerid,
                    COUNT(DISTINCT month_id) c_month
                FROM pm_owner.attr_terminal
                WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-2), 'YYYYMM'))
                AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                GROUP BY customerid) term_q
                ON term_q.customerid = base.customerid
                
            LEFT JOIN (
                SELECT
                    customerid,
                    COUNT(DISTINCT month_id) c_month
                FROM pm_owner.attr_terminal
                WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-11), 'YYYYMM'))
                AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                GROUP BY customerid) term_y
                ON term_y.customerid = base.customerid                   
            WHERE base.month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))) main
        
        LEFT JOIN (
            SELECT
                TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM')) month_id,
                customerid,
                -- POS + MPOS
                MAX(CASE WHEN install_dt <= process_dt AND install_dt > ADD_MONTHS(process_dt, -1)
                              AND type_terminal in ('MPOS', 'POS') THEN 1 ELSE 0 END) i_opened_posterminal_m,                            
                MAX(CASE WHEN deinstall_dt <= process_dt AND deinstall_dt > ADD_MONTHS(process_dt, -1)
                              AND type_terminal in ('MPOS', 'POS') THEN 1 ELSE 0 END) i_closed_posterminal_m,
                MAX(CASE WHEN install_dt <= process_dt AND deinstall_dt > process_dt
                              AND type_terminal in ('MPOS', 'POS') THEN 1 ELSE 0 END) i_active_posterminal_m,
                -- SOFP              
                MAX(CASE WHEN install_dt <= process_dt AND install_dt > ADD_MONTHS(process_dt, -1)
                              AND type_terminal = 'SOFP' THEN 1 ELSE 0 END) i_opened_smartterminal_m,                            
                MAX(CASE WHEN deinstall_dt <= process_dt AND deinstall_dt > ADD_MONTHS(process_dt, -1)
                              AND type_terminal = 'SOFP' THEN 1 ELSE 0 END) i_closed_smartterminal_m,
                MAX(CASE WHEN install_dt <= process_dt AND deinstall_dt > process_dt
                              AND type_terminal = 'SOFP' THEN 1 ELSE 0 END) i_active_smartterminal_m,                     
                -- POS + MPOS + SOFP               
                MAX(CASE WHEN install_dt <= process_dt AND type_terminal in ('SOFP', 'POS', 'MPOS') THEN 1 ELSE 0 END) i_total_terminal, 
                SUM(CASE WHEN install_dt <= process_dt AND deinstall_dt > process_dt
                              AND type_terminal in ('SOFP', 'POS', 'MPOS') THEN 1 ELSE 0 END) c_active_terminal,
                process_dt - COALESCE(MAX(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') AND install_dt <= process_dt AND install_dt > process_dt_hist
                                                           THEN install_dt ELSE NULL END), process_dt_hist) recency_opened_terminal,  
                process_dt - COALESCE(MAX(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') AND deinstall_dt <= process_dt AND install_dt > process_dt_hist
                                                           THEN deinstall_dt ELSE NULL END), process_dt_hist) recency_closed_terminal,
                -- webpay
                MAX(CASE WHEN install_dt <= process_dt AND install_dt > ADD_MONTHS(process_dt, -1)
                              AND type_terminal = 'ECOM' THEN 1 ELSE 0 END) i_opened_webpay_m,                            
                MAX(CASE WHEN deinstall_dt <= process_dt AND deinstall_dt > ADD_MONTHS(process_dt, -1)
                              AND type_terminal = 'ECOM' THEN 1 ELSE 0 END) i_closed_webpay_m,
                MAX(CASE WHEN install_dt <= process_dt AND deinstall_dt > process_dt
                              AND type_terminal = 'ECOM' THEN 1 ELSE 0 END) i_active_webpay_m,
                MAX(CASE WHEN install_dt <= process_dt AND type_terminal = 'ECOM' THEN 1 ELSE 0 END) i_total_webpay, 
                SUM(CASE WHEN install_dt <= process_dt AND deinstall_dt > process_dt
                              AND type_terminal = 'ECOM' THEN 1 ELSE 0 END) c_active_webpay,
                process_dt - COALESCE(MAX(CASE WHEN type_terminal = 'ECOM' AND install_dt <= process_dt AND install_dt > process_dt_hist
                                                           THEN install_dt ELSE process_dt_hist END), process_dt_hist) recency_opened_webpay,  
                process_dt - COALESCE(MAX(CASE WHEN type_terminal = 'ECOM' AND deinstall_dt <= DATE '9999-12-31' AND install_dt > process_dt_hist
                                                           THEN deinstall_dt ELSE process_dt_hist END), process_dt_hist) recency_closed_webpay,                         
                SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN c_txn ELSE 0 END) c_txn_terminal_m,  
                SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN sum_txn ELSE 0 END)/GREATEST(SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN c_txn ELSE 0 END), 1) avg_txn_terminal_m,
                SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN sum_provision ELSE 0 END) sum_provision_terminal_m,
                SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN c_txn ELSE 0 END)/
                        GREATEST(SUM(CASE WHEN install_dt <= process_dt AND deinstall_dt > process_dt
                                          AND type_terminal in ('SOFP', 'POS', 'MPOS') THEN 1 ELSE 0 END), 1) avg_c_txn_terminal_m,
                SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN c_txn_foreign ELSE 0 END)/ 
                GREATEST((SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN c_txn_domestic ELSE 0 END) + SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN c_txn_foreign ELSE 0 END)),1) foreign_ratio_c_terminal_m, 
                SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN abs(sum_txn_foreign) ELSE 0 END)/ 
                GREATEST((SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN abs(sum_txn_domestic) ELSE 0 END) + SUM(CASE WHEN type_terminal in ('SOFP', 'POS', 'MPOS') THEN abs(sum_txn_foreign) ELSE 0 END)),1) foreign_ratio_sum_terminal_m,                          
                                          
                SUM(CASE WHEN type_terminal = 'ECOM' THEN c_txn ELSE 0 END) c_txn_webpay_m,  
                SUM(CASE WHEN type_terminal = 'ECOM' THEN sum_txn ELSE 0 END)/GREATEST(SUM(CASE WHEN type_terminal = 'ECOM' THEN c_txn ELSE 0 END), 1) avg_txn_webpay_m,
                SUM(CASE WHEN type_terminal = 'ECOM' THEN sum_provision ELSE 0 END) sum_provision_webpay_m,
                SUM(CASE WHEN type_terminal = 'ECOM' THEN c_txn ELSE 0 END)/
                        GREATEST(SUM(CASE WHEN install_dt <= process_dt AND deinstall_dt > process_dt
                                          AND type_terminal = 'ECOM' THEN 1 ELSE 0 END), 1) avg_c_txn_webpay_m,
                SUM(CASE WHEN type_terminal = 'ECOM' THEN c_txn_foreign ELSE 0 END)/ 
                GREATEST((SUM(CASE WHEN type_terminal = 'ECOM' THEN c_txn_domestic ELSE 0 END) + SUM(CASE WHEN type_terminal = 'ECOM' THEN c_txn_foreign ELSE 0 END)),1) foreign_ratio_c_webpay_m, 
                SUM(CASE WHEN type_terminal = 'ECOM' THEN abs(sum_txn_foreign) ELSE 0 END)/ 
                GREATEST((SUM(CASE WHEN type_terminal = 'ECOM' THEN abs(sum_txn_domestic) ELSE 0 END) + SUM(CASE WHEN type_terminal = 'ECOM' THEN abs(sum_txn_foreign) ELSE 0 END)),1) foreign_ratio_sum_webpay_m 
            FROM (
                SELECT
                    terminal.customerid,
                    terminal.cd_terminal,
                    terminal.type_terminal,
                    terminal.sign_dt,
                    terminal.install_dt,
                    terminal.deinstall_dt,
                    terminal_pays.c_txn,       
                    terminal_pays.sum_txn,
                    terminal_pays.sum_provision,            
                    terminal_pays.c_txn_domestic, 
                    terminal_pays.sum_txn_domestic, 
                    terminal_pays.provision_domestic, 
                    terminal_pays.c_txn_foreign, 
                    terminal_pays.sum_txn_foreign, 
                    terminal_pays.provision_foreign        
                    
                FROM PM_OWNER.TERMINAL terminal
                   
                LEFT JOIN (
                    SELECT
                        cd_terminal,
                        COALESCE(SUM(c_txn), 0) c_txn,       
                        COALESCE(SUM(sum_txn), 0) sum_txn,
                        COALESCE(SUM(provision), 0) sum_provision,            
                        COALESCE(SUM(C_MC_TXN_DOMESTIC),0) + COALESCE(SUM(C_MAESTRO_TXN_DOMESTIC),0) + COALESCE(SUM(C_VISA_TXN_DOMESTIC),0)
                        + COALESCE(SUM(C_MC_TXN_CSOB),0) + COALESCE(SUM(C_MAESTRO_TXN_CSOB),0) + COALESCE(SUM(C_VISA_TXN_CSOB),0) c_txn_domestic, 
                        COALESCE(SUM(SUM_MC_TXN_DOMESTIC),0) + COALESCE(SUM(SUM_MAESTRO_TXN_DOMESTIC),0) + COALESCE(SUM(SUM_VISA_TXN_DOMESTIC),0)
                        + COALESCE(SUM(SUM_MC_TXN_CSOB),0) + COALESCE(SUM(SUM_MAESTRO_TXN_CSOB),0) + COALESCE(SUM(SUM_VISA_TXN_CSOB),0) sum_txn_domestic, 
                        COALESCE(SUM(PROVISION_MC_DOMESTIC),0) + COALESCE(SUM(PROVISION_MAESTRO_DOMESTIC),0) + COALESCE(SUM(PROVISION_VISA_DOMESTIC),0)
                        + COALESCE(SUM(PROVISION_MC_CSOB),0) + COALESCE(SUM(PROVISION_MAESTRO_CSOB),0) + COALESCE(SUM(PROVISION_VISA_CSOB),0) provision_domestic, 
                        COALESCE(SUM(C_MC_TXN_EU),0) + COALESCE(SUM(C_MAESTRO_TXN_EU),0) + COALESCE(SUM(C_VISA_TXN_EU),0)
                        + COALESCE(SUM(C_MC_TXN_NONEU),0) + COALESCE(SUM(C_MAESTRO_TXN_NONEU),0) + COALESCE(SUM(C_VISA_TXN_NONEU),0) c_txn_foreign, 
                        COALESCE(SUM(SUM_MC_TXN_EU),0) + COALESCE(SUM(SUM_MAESTRO_TXN_EU),0) + COALESCE(SUM(SUM_VISA_TXN_EU),0)
                        + COALESCE(SUM(SUM_MC_TXN_NONEU),0) + COALESCE(SUM(SUM_MAESTRO_TXN_NONEU),0) + COALESCE(SUM(SUM_VISA_TXN_NONEU),0) sum_txn_foreign, 
                        COALESCE(SUM(PROVISION_MC_EU),0) + COALESCE(SUM(PROVISION_MAESTRO_EU),0) + COALESCE(SUM(PROVISION_VISA_EU),0)
                        + COALESCE(SUM(PROVISION_MC_NONEU),0) + COALESCE(SUM(PROVISION_MAESTRO_NONEU),0) + COALESCE(SUM(PROVISION_VISA_NONEU),0) provision_foreign              
                    FROM pm_owner.TERMINAL_TXN_TYPE_LIST
                    WHERE month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))
                    GROUP BY cd_terminal) terminal_pays
                    ON terminal.cd_terminal = terminal_pays.cd_terminal
                    
                WHERE terminal.month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM')))
            GROUP BY customerid, TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))) term
            ON term.customerid = main.customerid
                 
        LEFT JOIN (
            SELECT
                customerid,
                COUNT(DISTINCT month_id) c_month_id,
                
                MAX(i_opened_posterminal_m) i_opened_posterminal_q,                           
                MAX(i_closed_posterminal_m) i_closed_posterminal_q, 
                MAX(i_active_posterminal_m) i_active_posterminal_q,               
                MAX(i_opened_smartterminal_m) i_opened_smartterminal_q,                             
                MAX(i_closed_smartterminal_m) i_closed_smartterminal_q, 
                MAX(i_active_smartterminal_m) i_active_smartterminal_q,
                
                MAX(i_opened_webpay_m) i_opened_webpay_q,                             
                MAX(i_closed_webpay_m) i_closed_webpay_q, 
                MAX(i_active_webpay_m) i_active_webpay_q,
                
                SUM(c_txn_terminal_m) c_txn_terminal_q,   
                SUM(avg_txn_terminal_m) avg_txn_terminal_q, 
                SUM(sum_provision_terminal_m) sum_provision_terminal_q, 
                SUM(avg_c_txn_terminal_m) avg_c_txn_terminal_q, 
                SUM(foreign_ratio_c_terminal_m) foreign_ratio_c_terminal_q,  
                SUM(foreign_ratio_sum_terminal_m) foreign_ratio_sum_terminal_q,
            
                SUM(c_txn_webpay_m) c_txn_webpay_q,   
                SUM(avg_txn_webpay_m) avg_txn_webpay_q, 
                SUM(sum_provision_webpay_m) sum_provision_webpay_q, 
                SUM(avg_c_txn_webpay_m) avg_c_txn_webpay_q, 
                SUM(foreign_ratio_c_webpay_m) foreign_ratio_c_webpay_q,  
                SUM(foreign_ratio_sum_webpay_m) foreign_ratio_sum_webpay_q 
            
            FROM pm_owner.attr_terminal
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -2), 'YYYYMM'))
                           AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) term_q
            ON main.customerid = term_q.customerid
            
        LEFT JOIN (
            SELECT
                customerid,
                COUNT(DISTINCT month_id) c_month_id,
                
                MAX(i_opened_posterminal_m) i_opened_posterminal_y,                           
                MAX(i_closed_posterminal_m) i_closed_posterminal_y, 
                MAX(i_active_posterminal_m) i_active_posterminal_y,               
                MAX(i_opened_smartterminal_m) i_opened_smartterminal_y,                             
                MAX(i_closed_smartterminal_m) i_closed_smartterminal_y, 
                MAX(i_active_smartterminal_m) i_active_smartterminal_y,
                
                MAX(i_opened_webpay_m) i_opened_webpay_y,                             
                MAX(i_closed_webpay_m) i_closed_webpay_y, 
                MAX(i_active_webpay_m) i_active_webpay_y,
                
                SUM(c_txn_terminal_m) c_txn_terminal_y,   
                SUM(avg_txn_terminal_m) avg_txn_terminal_y, 
                SUM(sum_provision_terminal_m) sum_provision_terminal_y, 
                SUM(avg_c_txn_terminal_m) avg_c_txn_terminal_y, 
                SUM(foreign_ratio_c_terminal_m) foreign_ratio_c_terminal_y,  
                SUM(foreign_ratio_sum_terminal_m) foreign_ratio_sum_terminal_y,
            
                SUM(c_txn_webpay_m) c_txn_webpay_y,   
                SUM(avg_txn_webpay_m) avg_txn_webpay_y, 
                SUM(sum_provision_webpay_m) sum_provision_webpay_y, 
                SUM(avg_c_txn_webpay_m) avg_c_txn_webpay_y, 
                SUM(foreign_ratio_c_webpay_m) foreign_ratio_c_webpay_y,  
                SUM(foreign_ratio_sum_webpay_m) foreign_ratio_sum_webpay_y 
            
            FROM pm_owner.attr_terminal
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -11), 'YYYYMM'))
                           AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) term_y
            ON main.customerid = term_y.customerid;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);
    
    END;

  PROCEDURE append_attr_standing_order(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_attr_standing_order';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_STANDING_ORDER';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);

  BEGIN
    
    -- ATTR_STANDING_ORDER
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);

	ETL_OWNER.etl_dbms_util.truncate_table('STG_STANDING_ORDER',l_owner_name); 
	-- stage standing order table
    INSERT INTO PM_OWNER.STG_STANDING_ORDER
        SELECT
            custid.party_id customerid,
            standing_ord.ord_id,
            standing_ord.src_syst_cd,
            standing_ord.type,
            standing_ord.standing_ord_type_cd,
            standing_ord.standing_ord_gen_fq,
            CASE WHEN LOWER(standing_ord.standing_ord_gen_fq) LIKE '%1y%'
                    OR LOWER(standing_ord.standing_ord_gen_fq) LIKE '%12m%'
                    OR LOWER(standing_ord.standing_ord_gen_fq) LIKE '%yea%' THEN 'annualy'
                 WHEN LOWER(standing_ord.standing_ord_gen_fq) LIKE '%6m%'
                    OR LOWER(standing_ord.standing_ord_gen_fq) LIKE '%hyr%' THEN 'semi_annualy'
                 WHEN LOWER(standing_ord.standing_ord_gen_fq) LIKE '%qtr%'
                    OR LOWER(standing_ord.standing_ord_gen_fq) LIKE '%3m%'
                    OR LOWER(standing_ord.standing_ord_gen_fq) LIKE '%1q%' THEN 'quaterly'
                 WHEN LOWER(standing_ord.standing_ord_gen_fq) LIKE '%1m%'
                    OR LOWER(standing_ord.standing_ord_gen_fq) LIKE '%mth%' THEN 'monthly'
                 WHEN LOWER(standing_ord.standing_ord_gen_fq) LIKE '%w%'
                    OR LOWER(standing_ord.standing_ord_gen_fq) IN ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')  THEN 'weekly'
                 WHEN LOWER(standing_ord.standing_ord_gen_fq) LIKE '%d%' THEN 'daily'
                 WHEN  standing_ord.src_syst_cd = 514 THEN 'direct_debit'
                 else 'other_frequency' end frequency,         
            standing_ord.standing_ord_gen_am_type,
            CASE WHEN standing_ord.standing_ord_gen_am_type IN (12001, 1) THEN 'fix_amount'
                 WHEN standing_ord.standing_ord_gen_am_type IN (12002, 2) THEN 'all_above'
                 WHEN standing_ord.standing_ord_gen_am_type IN (12003, 3) THEN 'all'
                 WHEN standing_ord.src_syst_cd = 514 THEN 'direct_debit'
                 ELSE 'unknown' END transfer_type,
            standing_ord.currency,
            CASE WHEN standing_ord.src_syst_cd IN (510, 520) THEN standing_ord.enter_dt
                 WHEN standing_ord.src_syst_cd IN (501) THEN standing_ord.t24_open_dt
                 WHEN standing_ord.src_syst_cd IN (514) THEN standing_ord.open_dt
                 ELSE DATE ' 1900-12-31' END create_dt,
            COALESCE(dates.status_d_standing_ord, standing_ord.exp_dt) close_dt,
            CASE WHEN standing_ord.exp_dt <= process_dt THEN 'expired'
                 WHEN COALESCE(dates.status_d_standing_ord, DATE '9999-12-31') <= process_dt THEN 'deleted'
                 WHEN dates.next_process_standing_ord = DATE '9999-12-31' AND COALESCE(dates.status_d_standing_ord, standing_ord.exp_dt) = DATE '9999-12-31' THEN 'deleted'
                 ELSE 'active' END status,
            COALESCE(payment.amount, payment.regular_payment) amount,
            payment.max_amount_direct_debit,
            payment.c_txn_direct_debit
        FROM (
            SELECT
                SO.standing_ord_id ord_id,
                SO.src_syst_cd,
                SOG.acct_id,
                POTG.paym_ord_type_gen_ds type,
                SOG.standing_ord_type_cd,
                SOG.standing_ord_gen_fq,
                SOG.standing_ord_gen_am_type,
                CG.curr_gen_tl currency,
                t24_open.t24_open_dt t24_open_dt,
                SOG.standing_ord_open_dt open_dt,
                SOG.standing_ord_exp_dt exp_dt,
                SOG.standing_ord_enter_dt enter_dt
            FROM L0_OWNER.STANDING_ORD SO
            
            JOIN L0_OWNER.STANDING_ORD_GEN SOG
                ON  SOG.standing_ord_id = SO.standing_ord_id
                AND SOG.standing_ord_gen_start_dt <= process_dt
                AND SOG.standing_ord_gen_end_dt > process_dt
                
            LEFT JOIN (
                SELECT
                    standing_ord_id,
                    MIN(standing_ord_gen_start_dt) t24_open_dt
                FROM L0_OWNER.STANDING_ORD_GEN
                GROUP BY standing_ord_id) t24_open
                ON t24_open.standing_ord_id = SO.standing_ord_id
                
            LEFT JOIN L0_OWNER.PAYM_ORD_TYPE_GEN POTG
                ON  POTG.paym_ord_type_cd = SOG.standing_ord_type_cd
                AND POTG.paym_ord_type_gen_start_dt <= process_dt
                AND POTG.paym_ord_type_gen_end_dt > process_dt
                
            LEFT JOIN L0_OWNER.CHANL_GEN CHG
                ON  CHG.chanl_id = SOG.chanl_id
                AND CHG.chanl_gen_start_dt <= process_dt
                AND CHG.chanl_gen_end_dt > process_dt
                
            LEFT JOIN L0_OWNER.CURR_GEN CG
                ON  CG.curr_cd = SOG.curr_cd
                AND CG.curr_gen_start_dt <= process_dt
                AND CG.curr_gen_end_dt > process_dt
                
            LEFT JOIN L0_OWNER.PARTY_ORG_NM PON
                ON  PON.party_id = SOG.contrbank_party_id
                AND PON.party_org_nm_start_dt <= process_dt
                AND PON.party_org_nm_end_dt > process_dt
                AND PON.nm_type_cd = 1
                
            WHERE
                SO.src_syst_cd in (501, 510, 520, 514)
            ) standing_ord
            
        LEFT JOIN (
            SELECT
                standing_ord_id,
                MAX(CASE WHEN meas_cd = 12013 THEN standing_ord_dt_actl_dt ELSE NULL END) AS first_process_standing_ord,
                MAX(CASE WHEN meas_cd = 12014 THEN standing_ord_dt_actl_dt ELSE NULL END) AS status_d_standing_ord,
                MAX(CASE WHEN meas_cd = 8014 THEN standing_ord_dt_actl_dt ELSE NULL END) AS last_process_standing_ord,
                MAX(CASE WHEN meas_cd = 8015 THEN standing_ord_dt_actl_dt ELSE NULL END) AS next_process_standing_ord,
                MAX(CASE WHEN meas_cd =  8018 THEN standing_ord_dt_actl_dt ELSE NULL END) AS last_direct_debit
            FROM l0_owner.standing_ord_dt_actl
            GROUP BY
                standing_ord_id
            ) dates
            ON  dates.standing_ord_id = standing_ord.ord_id
            
        LEFT JOIN (
            SELECT
                standing_ord_id,
                MAX(CASE WHEN meas_cd = 12006 AND row_number = 1 THEN standing_ord_val_am ELSE NULL END) first_payment,
                MAX(CASE WHEN meas_cd = 12007 AND row_number = 1 THEN standing_ord_val_am ELSE NULL END) regular_payment,
                MAX(CASE WHEN meas_cd = 12008 AND row_number = 1 THEN standing_ord_val_am ELSE NULL END) last_payment,
                MAX(CASE WHEN meas_cd = 12009 AND row_number = 1 THEN standing_ord_val_am ELSE NULL END) min_amount_payment,
                MAX(CASE WHEN meas_cd = 12011 AND row_number = 1 THEN standing_ord_val_am ELSE NULL END) direct_debit_amount,
                MAX(CASE WHEN meas_cd = 12012 AND row_number = 1 THEN standing_ord_val_am ELSE NULL END) amount,
                MAX(CASE WHEN meas_cd = 12015 AND row_number = 1 THEN standing_ord_val_am ELSE NULL END) max_amount_direct_debit,
                MAX(CASE WHEN meas_cd = 12016 AND row_number = 1 THEN standing_ord_val_am ELSE NULL END) c_txn_direct_debit
            FROM (
                SELECT
                    standing_ord_id,
                    meas_cd,
                    standing_ord_val_am,
                    standing_ord_val_start_dt,
                    standing_ord_val_end_dt,
                    ROW_NUMBER() OVER (PARTITION BY standing_ord_id, meas_cd ORDER BY standing_ord_val_start_dt DESC) row_number      
                FROM L0_OWNER.STANDING_ORD_VAL
                WHERE
                    standing_ord_val_start_dt <= process_dt
                    AND standing_ord_val_am IS NOT NULL)
            GROUP BY standing_ord_id
            ) payment
            ON  payment.standing_ord_id = standing_ord.ord_id
            
        JOIN l0_owner.party_cont party_cont
            ON  party_cont.cont_id = standing_ord.ACCT_ID
            AND party_cont.party_cont_start_dt <= process_dt
            AND party_cont.party_cont_end_dt > process_dt
            AND party_cont.party_role_cd = 1 -- client
        
        JOIN l0_owner.party_rltd custid
            ON  custid.child_party_id = party_cont.party_id
            AND custid.party_rltd_start_dt <= process_dt
            AND custid.party_rltd_end_dt > process_dt
            AND custid.party_rltd_rsn_cd = 4 -- customerid
            
        WHERE 1=1
            AND (CASE WHEN standing_ord.src_syst_cd = 510
                        AND standing_ord.standing_ord_type_cd in (191) THEN 1
                      WHEN standing_ord.src_syst_cd IN (501, 514, 520) THEN 1
                      ELSE 0 END) = 1
            AND COALESCE(dates.status_d_standing_ord, standing_ord.exp_dt) > process_dt_hist
            AND COALESCE(dates.status_d_standing_ord, standing_ord.exp_dt)
            - (CASE WHEN standing_ord.src_syst_cd IN (510, 520) THEN standing_ord.enter_dt
                 WHEN standing_ord.src_syst_cd IN (501) THEN standing_ord.t24_open_dt
                 WHEN standing_ord.src_syst_cd IN (514) THEN standing_ord.open_dt
                 ELSE DATE ' 1900-12-31' END) > 1;
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;                 

    INSERT INTO PM_OWNER.ATTR_STANDING_ORDER
        SELECT
            main.month_id,
            main.customerid,
            -- action
            COALESCE(base_so.i_created_direct_debit_m, 0) i_created_direct_debit_m,
            COALESCE(base_so.i_created_standing_ord_m, 0) i_created_standing_ord_m,
            COALESCE(base_so.c_created_direct_debit_m, 0) c_created_direct_debit_m,
            COALESCE(base_so.c_created_standing_ord_m, 0) c_created_standing_ord_m,
            COALESCE(base_so.i_closed_direct_debit_m, 0) i_closed_direct_debit_m,
            COALESCE(base_so.i_closed_standing_ord_m, 0) i_closed_standing_ord_m,
        
            GREATEST(COALESCE(base_so.i_created_direct_debit_m, 0), COALESCE(so_q.i_created_direct_debit_q, 0)) i_created_direct_debit_q,
            GREATEST(COALESCE(base_so.i_created_standing_ord_m, 0), COALESCE(so_q.i_created_standing_ord_q, 0)) i_created_standing_ord_q, 
            (COALESCE(base_so.c_created_direct_debit_m, 0) + COALESCE(so_q.c_created_direct_debit_q, 0))/main.c_month_q c_created_direct_debit_q,
            (COALESCE(base_so.c_created_standing_ord_m, 0) + COALESCE(so_q.c_created_standing_ord_q, 0))/main.c_month_q c_created_standing_ord_q,
            GREATEST(COALESCE(base_so.i_closed_direct_debit_m, 0), COALESCE(so_q.i_closed_direct_debit_q, 0)) i_closed_direct_debit_q,
            GREATEST(COALESCE(base_so.i_closed_standing_ord_m, 0), COALESCE(so_q.i_closed_standing_ord_q, 0)) i_closed_standing_ord_q,
            
            GREATEST(COALESCE(base_so.i_created_direct_debit_m, 0), COALESCE(so_y.i_created_direct_debit_y, 0)) i_created_direct_debit_y,
            GREATEST(COALESCE(base_so.i_created_standing_ord_m, 0), COALESCE(so_y.i_created_standing_ord_y, 0)) i_created_standing_ord_y, 
            (COALESCE(base_so.c_created_direct_debit_m, 0) + COALESCE(so_y.c_created_direct_debit_y, 0))/main.c_month_y c_created_direct_debit_y,
            (COALESCE(base_so.c_created_standing_ord_m, 0) + COALESCE(so_y.c_created_standing_ord_y, 0))/main.c_month_y c_created_standing_ord_y,
            GREATEST(COALESCE(base_so.i_closed_direct_debit_m, 0), COALESCE(so_y.i_closed_direct_debit_y, 0)) i_closed_direct_debit_y,
            GREATEST(COALESCE(base_so.i_closed_standing_ord_m, 0), COALESCE(so_y.i_closed_standing_ord_y, 0)) i_closed_standing_ord_y,
            
            -- status
            COALESCE(base_so.c_direct_debit, 0) c_direct_debit,
            COALESCE(base_so.c_total_txn_direct_debit, 0) c_total_txn_direct_debit,
            COALESCE(base_so.c_standing_ord, 0) c_standing_ord,
            COALESCE(base_so.c_closed_standing_ord, 0) c_closed_standing_ord,
            COALESCE(base_so.c_daily_standing_ord, 0) c_daily_standing_ord,
            COALESCE(base_so.c_monthly_standing_ord, 0) c_monthly_standing_ord,
            COALESCE(base_so.c_annualy_standing_ord, 0) c_annualy_standing_ord,
            COALESCE(base_so.max_monthly_standing_ord, 0) max_monthly_standing_ord,
            COALESCE(base_so.i_balance_standing_ord, 0) i_balance_standing_ord,
            COALESCE(base_so.c_balance_standing_ord, 0) c_balance_standing_ord,
            COALESCE(base_so.sum_total_standing_ord, 0) sum_total_standing_ord,
            COALESCE(base_so.min_time_closed_standing_ord, process_dt - process_dt_hist) min_time_closed_standing_ord,
            COALESCE(base_so.min_time_closed_direct_debit, process_dt - process_dt_hist) min_time_closed_direct_debit,
            COALESCE(base_so.min_time_opened_standing_ord, process_dt - process_dt_hist) min_time_opened_standing_ord,
            COALESCE(base_so.min_time_opened_direct_debit, process_dt - process_dt_hist) min_time_opened_direct_debit			
        
        FROM (
            SELECT
                base.month_id,
                base.customerid,
                base.bank_tenure,
                LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -3)),1)*3,
                      COALESCE(so_q.c_month, 0)+1) c_month_q,
                LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -12)),1)*12,
                      COALESCE(so_y.c_month, 0)+1) c_month_y       
            FROM pm_owner.attr_client_base base
            LEFT JOIN (
                SELECT
                    customerid,
                    COUNT(DISTINCT month_id) c_month
                FROM pm_owner.attr_standing_order
                WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-2), 'YYYYMM'))
                AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                GROUP BY customerid) so_q
                ON so_q.customerid = base.customerid
                
            LEFT JOIN (
                SELECT
                    customerid,
                    COUNT(DISTINCT month_id) c_month
                FROM pm_owner.attr_standing_order
                WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-11), 'YYYYMM'))
                AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                GROUP BY customerid) so_y
                ON so_y.customerid = base.customerid                   
            WHERE base.month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))) main
                
        LEFT JOIN (
            SELECT
                TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM')) month_id,
                customerid,
                -- action
                MAX(CASE WHEN src_syst_cd = 514 AND create_dt <= process_dt AND create_dt > ADD_MONTHS(process_dt, -1) THEN 1 ELSE 0 END) i_created_direct_debit_m,
                MAX(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND create_dt > ADD_MONTHS(process_dt, -1) THEN 1 ELSE 0 END) i_created_standing_ord_m,  
                SUM(CASE WHEN src_syst_cd = 514 AND create_dt <= process_dt AND create_dt > ADD_MONTHS(process_dt, -1) THEN 1 ELSE 0 END) c_created_direct_debit_m,
                SUM(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND create_dt > ADD_MONTHS(process_dt, -1) THEN 1 ELSE 0 END) c_created_standing_ord_m,
                MAX(CASE WHEN src_syst_cd = 514 AND close_dt <= process_dt AND close_dt > ADD_MONTHS(process_dt, -1) THEN 1 ELSE 0 END) i_closed_direct_debit_m,
                MAX(CASE WHEN src_syst_cd != 514 AND close_dt <= process_dt AND close_dt > ADD_MONTHS(process_dt, -1) THEN 1 ELSE 0 END) i_closed_standing_ord_m,     
                -- status
                SUM(CASE WHEN src_syst_cd = 514 AND create_dt <= process_dt AND close_dt > process_dt AND status = 'active' THEN 1 ELSE 0 END) c_direct_debit,
                SUM(CASE WHEN src_syst_cd = 514 AND create_dt <= process_dt THEN c_txn_direct_debit ELSE 0 END) c_total_txn_direct_debit,
                SUM(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND status = 'active' THEN 1 ELSE 0 END) c_standing_ord,
                SUM(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND status in ('expired', 'deleted') THEN 1 ELSE 0 END) c_closed_standing_ord,
                SUM(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'daily' THEN 1 ELSE 0 END) c_daily_standing_ord,
                SUM(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'monthly' THEN 1 ELSE 0 END) c_monthly_standing_ord,
                SUM(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'annualy' THEN 1 ELSE 0 END) c_annualy_standing_ord,
                MAX(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'monthly' THEN amount ELSE 0 END) max_monthly_standing_ord,
                MAX(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND transfer_type IN ('all', 'all_above') THEN 1 ELSE 0 END) i_balance_standing_ord,
                SUM(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND transfer_type IN ('all', 'all_above') THEN 1 ELSE 0 END) c_balance_standing_ord,
                SUM(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'daily' THEN 30.4375*amount
                         WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'weekly' THEN 4.3482*amount
                         WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'monthly' THEN amount
                         WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency in ('quaterly', 'other_frequency') THEN amount/3
                         WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'semi_annualy' THEN amount/6
                         WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt > process_dt AND frequency = 'annualy' THEN amount/12
                         ELSE 0 END) sum_total_standing_ord,
                process_dt - COALESCE(MAX(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND close_dt <= process_dt AND close_dt > process_dt_hist
                                                 THEN close_dt ELSE NULL END), process_dt_hist) min_time_closed_standing_ord,
                process_dt - COALESCE(MAX(CASE WHEN src_syst_cd = 514 AND create_dt <= process_dt AND close_dt <= process_dt AND close_dt > process_dt_hist
                                                 THEN close_dt ELSE NULL END), process_dt_hist) min_time_closed_direct_debit,
                process_dt - COALESCE(MAX(CASE WHEN src_syst_cd != 514 AND create_dt <= process_dt AND create_dt > process_dt_hist THEN create_dt ELSE NULL END), process_dt_hist) min_time_opened_standing_ord,
                process_dt - COALESCE(MAX(CASE WHEN src_syst_cd = 514 AND create_dt <= process_dt AND create_dt > process_dt_hist THEN create_dt ELSE NULL END), process_dt_hist) min_time_opened_direct_debit 
                         
            FROM pm_owner.stg_standing_order
            GROUP BY customerid) base_so
            ON base_so.customerid = main.customerid
            
        LEFT JOIN (
            SELECT
                customerid,
                COUNT(DISTINCT month_id) c_month_id_q,
                
                MAX(i_created_direct_debit_m) i_created_direct_debit_q,
                MAX(i_created_standing_ord_m) i_created_standing_ord_q,  
                SUM(c_created_direct_debit_m) c_created_direct_debit_q,
                SUM(c_created_standing_ord_m) c_created_standing_ord_q,
                MAX(i_closed_direct_debit_m) i_closed_direct_debit_q,
                MAX(i_closed_standing_ord_m) i_closed_standing_ord_q
                
            FROM pm_owner.attr_standing_order
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -2), 'YYYYMM'))
                           AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) so_q
            ON main.customerid = so_q.customerid
        
        LEFT JOIN (
            SELECT
                customerid,
                COUNT(DISTINCT month_id) c_month_id_y,
                
                MAX(i_created_direct_debit_m) i_created_direct_debit_y,
                MAX(i_created_standing_ord_m) i_created_standing_ord_y, 
                SUM(c_created_direct_debit_m) c_created_direct_debit_y,
                SUM(c_created_standing_ord_m) c_created_standing_ord_y,
                MAX(i_closed_direct_debit_m) i_closed_direct_debit_y,
                MAX(i_closed_standing_ord_m) i_closed_standing_ord_y
                
            FROM pm_owner.attr_standing_order
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -11), 'YYYYMM'))
                           AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) so_y
            ON main.customerid = so_y.customerid; 

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);
    
    END;

  PROCEDURE append_ATTR_ELB(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_ATTR_ELB';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_ELB';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);
    
  BEGIN

    step(p_stepname => l_proc_name || ' TRUNC', p_source => 'N/A', p_target => 'STG_ELB_BASE');
	ETL_OWNER.etl_dbms_util.truncate_table('STG_ELB_BASE',l_owner_name);      
        
    step(p_stepname => l_proc_name || ' INSERT STG_ELB_BASE', p_source => 'L0.*', p_target => 'STG_ELB_BASE');
	INSERT INTO PM_OWNER.STG_ELB_BASE 
        SELECT
            PRL_IPPID.party_id as CUSTOMERID_ippid,
            MAX(PRL6.party_id) customerid_owner,
            P570.party_src_val as IPPID,
            DAI.dispnt_appid_ippid_id,
            P570.party_id as PARTY_ID_IPPID,
            DCH.CHANL_ID,
            CASE WHEN DCH.party_role_cd = 71 THEN 1 ELSE 0 END i_account_owner,
            CASE WHEN PRS.party_stat_type_cd=1 THEN 1 ELSE 0 END i_active_ippid,
            CASE WHEN PRS.party_stat_type_cd=3 THEN 1 ELSE 0 END i_closed_ippid,
            CASE WHEN PRS.party_stat_rsn_cd in (2, 4) THEN 1 ELSE 0 END i_blocked_ippid,
            PI570.party_indiv_open_dt as IPPID_OPEN_DT,
            DCH.acct_id,
            MAX(PROD.src_syst_cd) src_syst_cd,
            MAX(prod.prod_src_val) prod_src_val,      
            CASE WHEN MAX(PAR.PARTY_SUBTP_CD) = 0 THEN 1
                 ELSE 0 END i_retail_account
        
        FROM L0_OWNER.PARTY P570
        
        JOIN L0_OWNER.PARTY_ROLE_STAT PRS
            ON  PRS.party_id = P570.party_id
            AND PRS.party_role_stat_start_dt <= process_dt
            AND PRS.party_role_stat_end_dt > process_dt
            AND PRS.party_role_cd = 74
            AND PRS.party_stat_rsn_cd IN (1,2,4)
            
        JOIN L0_OWNER.PARTY_INDIV PI570
            ON  PI570.party_id = P570.party_id
            AND PI570.party_indiv_start_dt <= process_dt
            AND PI570.party_indiv_end_dt > process_dt
            AND PI570.party_indiv_close_dt > process_dt
            
        JOIN L0_OWNER.PARTY_RLTD PRL_IPPID
            ON  PRL_IPPID.child_party_id = P570.party_id
            AND PRL_IPPID.party_rltd_start_dt <= process_dt
            AND PRL_IPPID.party_rltd_end_dt > process_dt
            AND PRL_IPPID.party_rltd_rsn_cd = 4
            
        JOIN L0_OWNER.DISPNT_APPID_IPPID DAI
            ON  P570.party_id = DAI.ippid1_party_id
            AND DAI.src_syst_cd = 570
            
        -- prod_src_val   
        LEFT JOIN L0_OWNER.DISPNT_CHANL DCH
            ON DAI.dispnt_appid_ippid_id = DCH.dispnt_appid_ippid_id
            AND DCH.DISPNT_CHANL_EFF_DT <= process_dt
            --AND DCH.DISPNT_CHANL_EXP_DT <= process_dt
            AND DCH.DISPNT_CHANL_START_DT <= process_dt
            AND DCH.DISPNT_CHANL_END_DT > process_dt
        
        LEFT JOIN L0_OWNER.CONT_PROD CP
            ON  CP.cont_id = DCH.acct_id
            AND CP.CONT_PROD_START_DT <= process_dt
            AND CP.CONT_PROD_END_DT > process_dt
        
        LEFT JOIN L0_OWNER.CONT_GEN CG
            ON  CG.cont_id = CP.cont_id
            AND CG.cont_gen_start_dt <= process_dt
            AND CG.cont_gen_open_dt <= process_dt
            AND CG.cont_gen_close_dt > process_dt
            AND CG.cont_gen_end_dt > process_dt
        
        LEFT JOIN L0_OWNER.PROD PROD
            ON  PROD.prod_id = CP.prod_id
            AND PROD.src_syst_cd in (120, 501, 510, 520)
            AND PROD.prod_type_cd = 1
        
        LEFT JOIN L0_OWNER.CONT_STAT CST
            ON  CST.cont_id = CP.cont_id
            AND CST.cont_stat_start_dt <= process_dt
            AND CST.cont_stat_end_dt > process_dt
            AND CASE WHEN CST.cont_stat_type_cd IN (0,2) AND PROD.src_syst_cd in (501, 510) THEN 1
                     WHEN  CST.cont_stat_type_cd IN (0) AND PROD.src_syst_cd in (520) THEN 1 ELSE 0 END = 1
                       
        LEFT JOIN L0_OWNER.PARTY_CONT PC
            ON  PC.cont_id = DCH.acct_id
            AND PC.PARTY_CONT_START_DT <= process_dt
            AND PC.PARTY_CONT_END_DT > process_dt
            AND pc.party_role_cd in (1)
        
        LEFT JOIN L0_OWNER.PARTY PAR
            ON  PAR.party_id = PC.party_id
        
        LEFT JOIN L0_OWNER.PARTY_RLTD PRL6
            ON  PRL6.child_party_id = PC.party_id
            AND PRL6.party_rltd_start_dt <= process_dt
            AND PRL6.party_rltd_end_dt > process_dt
            AND PRL6.party_rltd_rsn_cd = 4
                    
        WHERE 1=1
            AND (CASE WHEN prod.prod_src_val IS NOT NULL AND CG.cont_gen_close_dt IS NULL THEN 0 ELSE 1 END) = 1
        
        group by
            PRL_IPPID.party_id,
            P570.party_src_val,
            DAI.dispnt_appid_ippid_id,
            P570.party_id,
            DCH.CHANL_ID,
            DCH.party_role_cd,
            PRS.party_stat_type_cd,
            PRS.party_stat_rsn_cd,
            PI570.party_indiv_open_dt,
            DCH.acct_id;
            
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;
    
	ETL_OWNER.etl_dbms_util.truncate_table('STG_ELB_IPPID',l_owner_name);
	
	INSERT INTO PM_OWNER.STG_ELB_IPPID    
        SELECT
            ELB.customerid_ippid customerid_ippid,
            ELB.customerid_owner customerid_account,
            ELB.ippid,
            ELB.IPPID_OPEN_DT,
            MAX(ELB.i_retail_account) i_retail_account,
            CASE WHEN MIN(ELB.i_retail_account) = 0 THEN 1 ELSE 0 END i_nonretail_account,
            ELB.i_active_ippid i_active_ippid,
            ELB.i_closed_ippid i_closed_ippid,
            ELB.i_blocked_ippid i_blocked_ippid,
            CASE WHEN ELB.i_account_owner = 0 THEN 0 ELSE 1 END i_authorizeduser_elb,
            MAX(CASE WHEN ELB.chanl_id = 1116  AND  ELB.i_account_owner = 1 THEN 1 ELSE 0 END)   as i_ib,
            MAX(CASE WHEN ELB.chanl_id  in (1178, 1179)  AND  ELB.i_account_owner = 1 THEN 1 ELSE 0 END)   as i_bb,
            MAX(CASE WHEN ELB.chanl_id = 1121  AND  ELB.i_account_owner = 1 THEN 1 ELSE 0 END)   as i_ivr,
            MAX(CASE WHEN ELB.chanl_id = 1112  AND  ELB.i_account_owner = 1 THEN 1 ELSE 0 END)   as i_cc,         
            MAX(CASE WHEN ELB.chanl_id = 1116  AND  ELB.i_account_owner != 1 THEN 1 ELSE 0 END)   as i_authorized_ib,
            MAX(CASE WHEN ELB.chanl_id  in (1178, 1179)  AND  ELB.i_account_owner != 1 THEN 1 ELSE 0 END)   as i_authorized_bb,
            MAX(CASE WHEN ELB.chanl_id = 1121  AND  ELB.i_account_owner != 1 THEN 1 ELSE 0 END)   as i_authorized_ivr,
            MAX(CASE WHEN ELB.chanl_id = 1112  AND  ELB.i_account_owner != 1 THEN 1 ELSE 0 END)   as i_authorized_cc,
            MAX(CASE WHEN DCHL.lmt_type_cd =  8 AND DCHL.chanl_id = 1116 THEN DCHL.dispnt_chanl_lmt_am ELSE NULL END) as dayly_limit_ib,
            MAX(CASE WHEN DCHL.lmt_type_cd =  9 AND DCHL.chanl_id = 1116 THEN DCHL.dispnt_chanl_lmt_am ELSE NULL END) as weekly_limit_ib,
            MAX(CASE WHEN DCHL.lmt_type_cd = 10 AND DCHL.chanl_id = 1116 THEN DCHL.dispnt_chanl_lmt_am ELSE NULL END) as limit_per_txn_ib, 
            MAX(CASE WHEN DCHL.lmt_type_cd =  7 AND DCHL.chanl_id in (1178, 1179) THEN DCHL.dispnt_chanl_lmt_am ELSE NULL END) as account_limit_bb,
            MAX(CASE WHEN DCHL.lmt_type_cd =  7 AND DCHL.chanl_id in (1121) THEN DCHL.dispnt_chanl_lmt_am ELSE NULL END) as account_limit_ivr,
            MAX(CASE WHEN DCHL.lmt_type_cd =  7 AND DCHL.chanl_id in (1112) THEN DCHL.dispnt_chanl_lmt_am ELSE NULL END) as account_limit_cc,
            MAX(CASE WHEN EA.ident_type_cd =  17 THEN 1 ELSE 0 END) as i_sms_authentification_elb, --Alternativní autorizace(SMS)
            MAX(CASE WHEN EA.ident_type_cd =  21 THEN 1 ELSE 0 END) as i_pin_authentification_elb, --Autorizace IPPID + PIN
            MAX(CASE WHEN EA.ident_type_cd  in (119,204) THEN 1 ELSE 0 END) as i_token_authentification_elb, --Digipass 270 - KI_AUTORIZACE + 2019-10-14 - Digipass 770 - KI_AUTORIZACE
            MAX(CASE WHEN EA.ident_type_cd  in (123,292) THEN 1 ELSE 0 END) as i_mtoken_authentification_elb --Digipass for Mobile - KI_AUTORIZACE, SW Cronto digipass - KI_AUTORIZACE --2020-09-28
        
        FROM (SELECT DISTINCT
                customerid_ippid,
                customerid_owner,
                ippid,
                party_id_ippid,
                chanl_id,
                i_retail_account,
                i_account_owner,
                dispnt_appid_ippid_id,
                ippid_open_dt,
                i_active_ippid,
                i_closed_ippid,   
                i_blocked_ippid
             FROM PM_OWNER.STG_ELB_BASE) ELB
        
        JOIN L0_OWNER.DISPNT_CHANL_LMT DCHL
            ON  ELB.dispnt_appid_ippid_id = DCHL.dispnt_appid_ippid_id
            AND DCHL.dispnt_chanl_lmt_start_dt <= process_dt
            AND DCHL.dispnt_chanl_lmt_end_dt > process_dt
            AND NVL(DCHL.dispnt_chanl_lmt_exp_dt, date '9999-12-31') > process_dt
        
        JOIN L0_OWNER.ELB_AUTH EA
            ON  EA.ippid_party_id = ELB.party_id_ippid
            AND EA.elb_auth_start_dt <= process_dt
            AND EA.elb_auth_end_dt > process_dt
            AND EA.elb_auth_stat_cd = 1                      --AND EA.elb_auth_stat = '8'     --podla novej L0_OWNER.ELB_STAT_GEN 'Aktivní'
        
        GROUP BY
            ELB.customerid_ippid,
            ELB.customerid_owner,
            ELB.ippid,
            ELB.i_account_owner,
            ELB.IPPID_OPEN_DT,
            ELB.i_active_ippid,
            ELB.i_closed_ippid,   
            ELB.i_blocked_ippid;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    step(p_stepname => l_proc_name || ' TRUNC', p_source => 'N/A', p_target => 'STG_ELB_EVENT');
	ETL_OWNER.etl_dbms_util.truncate_table('STG_ELB_EVENT',l_owner_name);     
        
    step(p_stepname => l_proc_name || ' INSERT STG_ELB_EVENT', p_source => 'L0.*', p_target => 'STG_ELB_EVENT');
	INSERT INTO PM_OWNER.STG_ELB_EVENT         
        select * from (
        select
            PRL6.PARTY_ID customerid_elb_owner,
            PRL_IPPID.PARTY_ID customerid_rights,
            PAR.PARTY_SUBTP_CD,
            ELB.ELB_SESSN_ID,    
            ELB.IDENT_TYPE_CD,
            ELB.chanl_id, 
            ELB.TXN_ELB_ID,
            ELB.TXN_ELB_DT,
            ELB.TXN_ELB_TM,
            ELB.txn_cd_cd,
            ELB.CURR_CD,
            ELB.txn_elb_am,
            ELB.txn_elb_bc_am,
            ELB.txn_stat_cd,
            ELB.src_syst_cd
        from L0_OWNER.TXN_ELB ELB
        join L0_OWNER.IDENT_TYPE_GEN ITG
            on ITG.IDENT_TYPE_CD = ELB.IDENT_TYPE_CD
            and ITG.IDENT_TYPE_GEN_START_DT <= process_dt
            and ITG.IDENT_TYPE_GEN_END_DT > process_dt
            
        join L0_OWNER.CHANL_GEN CI
            on  CI.CHANL_ID = ELB.CHANL_ID
            and CI.CHANL_GEN_START_DT <= process_dt
            and CI.CHANL_GEN_END_DT > process_dt
        
        LEFT JOIN L0_OWNER.PARTY_RLTD PRL_IPPID
            ON  PRL_IPPID.child_party_id = ELB.ippid_party_id
            AND PRL_IPPID.party_rltd_start_dt <= process_dt
            AND PRL_IPPID.party_rltd_end_dt > process_dt
            AND PRL_IPPID.party_rltd_rsn_cd = 4
        
        LEFT JOIN L0_OWNER.DISPNT_CHANL DCH
            ON ELB.dispnt_appid_ippid_id = DCH.dispnt_appid_ippid_id
            AND ELB.CHANL_ID = DCH.CHANL_ID
            AND ELB.ACCT_ID = DCH.ACCT_ID
            AND DCH.DISPNT_CHANL_EFF_DT <= process_dt
            AND DCH.DISPNT_CHANL_EXP_DT > process_dt
            AND DCH.DISPNT_CHANL_START_DT <= process_dt
            AND DCH.DISPNT_CHANL_END_DT > process_dt
        
        LEFT JOIN L0_OWNER.PARTY_CONT PC
            ON  PC.cont_id = COALESCE(ELB.acct_id, DCH.acct_id)
            AND PC.PARTY_CONT_START_DT <= process_dt
            AND PC.PARTY_CONT_END_DT > process_dt
            AND pc.party_role_cd in (1)
        
        LEFT JOIN L0_OWNER.PARTY PAR
            ON  PAR.party_id = PC.party_id
        
        LEFT JOIN L0_OWNER.PARTY_RLTD PRL6
            ON  PRL6.child_party_id = PC.party_id
            AND PRL6.party_rltd_start_dt <= process_dt
            AND PRL6.party_rltd_end_dt > process_dt
            AND PRL6.party_rltd_rsn_cd = 4
        
        WHERE ELB.TXN_ELB_DT > add_months(process_dt, -1)
        AND COALESCE(ELB.TXN_STAT_CD, 0)= 0
        AND ELB.TXN_ELB_DT <= process_dt
        AND ELB.src_syst_cd = 570
        
        UNION ALL
        
        select
            PRL6.PARTY_ID customerid_elb_owner,
            PRL_IPPID.PARTY_ID customerid_rights,
            PAR.PARTY_SUBTP_CD,
            ELB.ELB_SESSN_ID,    
            ELB.IDENT_TYPE_CD,
            ELB.chanl_id, 
            ELB.TXN_ELB_ID,
            ELB.TXN_ELB_DT,
            ELB.TXN_ELB_TM,
            ELB.txn_cd_cd,
            ELB.CURR_CD,
            ELB.txn_elb_am,
            ELB.txn_elb_bc_am,
            ELB.txn_stat_cd,
            ELB.src_syst_cd
        from L0_OWNER.TXN_ELB ELB
        
        -- cont join should be delete
        JOIN l0_owner.cont C
            ON ELB.acct_id = C.cont_id   
            AND C.src_syst_cd = 501
            AND C.cont_single_acct_in = 'Y'

        join L0_OWNER.CHANL_GEN CI
            on  CI.CHANL_ID = ELB.CHANL_ID
            and CI.CHANL_GEN_START_DT <= process_dt
            and CI.CHANL_GEN_END_DT > process_dt
        
        -- should be change to left join    
        JOIN L0_OWNER.IDENT_TYPE_GEN ITG
            on ITG.IDENT_TYPE_CD = ELB.IDENT_TYPE_CD
            and ITG.IDENT_TYPE_GEN_START_DT <= process_dt
            and ITG.IDENT_TYPE_GEN_END_DT > process_dt
        
        LEFT JOIN L0_OWNER.PARTY_RLTD PRL_IPPID
            ON  PRL_IPPID.child_party_id = ELB.ippid_party_id
            AND PRL_IPPID.party_rltd_start_dt <= process_dt
            AND PRL_IPPID.party_rltd_end_dt > process_dt
            AND PRL_IPPID.party_rltd_rsn_cd = 4
        
        LEFT JOIN L0_OWNER.DISPNT_CHANL DCH
            ON ELB.dispnt_appid_ippid_id = DCH.dispnt_appid_ippid_id
            AND ELB.CHANL_ID = DCH.CHANL_ID
            AND ELB.ACCT_ID = DCH.ACCT_ID
            AND DCH.DISPNT_CHANL_EFF_DT <= process_dt
            AND DCH.DISPNT_CHANL_EXP_DT > process_dt
            AND DCH.DISPNT_CHANL_START_DT <= process_dt
            AND DCH.DISPNT_CHANL_END_DT > process_dt
        
        LEFT JOIN L0_OWNER.PARTY_CONT PC
            ON  PC.cont_id = COALESCE(ELB.acct_id, DCH.acct_id)
            AND PC.PARTY_CONT_START_DT <= process_dt
            AND PC.PARTY_CONT_END_DT > process_dt
            AND pc.party_role_cd in (1)
        
        LEFT JOIN L0_OWNER.PARTY PAR
            ON  PAR.party_id = PC.party_id
        
        LEFT JOIN L0_OWNER.PARTY_RLTD PRL6
            ON  PRL6.child_party_id = PC.party_id
            AND PRL6.party_rltd_start_dt <= process_dt
            AND PRL6.party_rltd_end_dt > process_dt
            AND PRL6.party_rltd_rsn_cd = 4
        
        WHERE ELB.TXN_ELB_DT > add_months(process_dt, -1)
        AND ELB.TXN_STAT_CD = 0
        AND ELB.TXN_ELB_DT <= process_dt
        AND ELB.src_syst_cd = 11008);

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    step(p_stepname => l_proc_name || ' INSERT TMP_ELB_LOG', p_source => 'L0.*', p_target => 'TMP_ELB_LOG');
    INSERT INTO PM_OWNER.TMP_ELB_LOG
        SELECT
            TELB.CUSTOMERID_RIGHTS CUSTOMERID,
            process_dt - MAX(TELB.txn_elb_dt) c_day_last_log_elb,
        
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN process_dt - 1 AND process_dt THEN TELB.elb_sessn_id ELSE NULL END) c_log_last_day_elb,
            -----   PRIHLASOVANIE - TYZDNE (za posledne dni)
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN (process_dt - 6) AND process_dt THEN TELB.txn_elb_dt ELSE NULL END) c_daylog_last_week_elb,
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN (process_dt - 6) AND process_dt THEN TELB.elb_sessn_id ELSE NULL END) c_log_last_week_elb,
            -----   PRIHLASOVANIE - HODINY
            COUNT(distinct CASE WHEN SUBSTR(TELB.txn_elb_tm,1,2) in (0, 1, 2, 3, 4, 5) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_night_elb,
            COUNT(distinct CASE WHEN SUBSTR(TELB.txn_elb_tm,1,2) in (6, 7, 8, 9, 10, 11) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_morning_elb,
            COUNT(distinct CASE WHEN SUBSTR(TELB.txn_elb_tm,1,2) in (12, 13, 14, 15, 16, 17) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_afternoon_elb,
            COUNT(distinct CASE WHEN SUBSTR(TELB.txn_elb_tm,1,2) in (18, 19, 20, 21, 22, 23) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_evening_elb,
            -----   PRIHLASOVANIE - DNI v TYZDNI
            COUNT(distinct CASE WHEN TO_CHAR(TELB.txn_elb_dt, 'D') in ('1', '2', '3', '4', '5') THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_weekday_elb,
            COUNT(distinct CASE WHEN TO_CHAR(TELB.txn_elb_dt, 'D') in ('6', '7') THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_weekend_elb,
            COUNT(distinct CASE WHEN TO_CHAR(TELB.txn_elb_dt, 'D') in ('1', '2', '3', '4', '5') THEN TELB.elb_sessn_id ELSE NULL END) as c_log_weekday_elb,
            COUNT(distinct CASE WHEN TO_CHAR(TELB.txn_elb_dt, 'D') in ('6', '7') THEN TELB.elb_sessn_id ELSE NULL END) as c_log_weekend_elb,
            -----   MESACNY
            ----- Nespravny počet logov kvôli pridaniu logov z LOCO
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_elb,
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt THEN TELB.elb_sessn_id ELSE NULL END) as c_log_elb,
            COUNT(distinct CASE WHEN TELB.CUSTOMERID_ELB_OWNER != TELB.CUSTOMERID_RIGHTS AND TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_elb_holder,
            COUNT(distinct CASE WHEN TELB.CUSTOMERID_ELB_OWNER != TELB.CUSTOMERID_RIGHTS AND TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt THEN TELB.elb_sessn_id ELSE NULL END) as c_log_elb_holder,
            COUNT(distinct CASE WHEN TELB.PARTY_SUBTP_CD != 0 AND TELB.CUSTOMERID_ELB_OWNER != TELB.CUSTOMERID_RIGHTS AND TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_noretail_elb_holder,
            COUNT(distinct CASE WHEN TELB.PARTY_SUBTP_CD != 0 AND TELB.CUSTOMERID_ELB_OWNER != TELB.CUSTOMERID_RIGHTS AND TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt THEN TELB.elb_sessn_id ELSE NULL END) as c_log_noretail_elb_holder,
            
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt
                                AND TELB.chanl_id in (1116, 2172) THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_ib,
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt
                                AND TELB.chanl_id in (1116, 2172) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_ib,
        
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt
                                AND TELB.chanl_id in (1319) THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_sb,
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt
                                AND TELB.chanl_id in (1319) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_sb,
            
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt
                                AND TELB.chanl_id in (1179, 1111) THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_bb,
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt
                                AND TELB.chanl_id in (1179, 1111) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_bb
        FROM ( SELECT * from pm_owner.stg_elb_event
                where chanl_id IN (1111, 1116, 1319, 2172, 1179)) TELB
        GROUP BY
            TELB.CUSTOMERID_RIGHTS;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    -- COALESCE na ownera -> rights
    INSERT INTO PM_OWNER.TMP_ELB_LOG_OWNER
       SELECT
            TELB.CUSTOMERID_ELB_OWNER CUSTOMERID,
            (process_dt - MAX(TELB.txn_elb_dt)) c_day_last_log_owner_elb,

            -----   PRIHLASOVANIE - HODINY
            COUNT(distinct CASE WHEN SUBSTR(TELB.txn_elb_tm,1,2) in (0, 1, 2, 3, 4, 5) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_owner_night_elb,
            COUNT(distinct CASE WHEN SUBSTR(TELB.txn_elb_tm,1,2) in (6, 7, 8, 9, 10, 11) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_owner_morning_elb,
            COUNT(distinct CASE WHEN SUBSTR(TELB.txn_elb_tm,1,2) in (12, 13, 14, 15, 16, 17) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_owner_afternoon_elb,
            COUNT(distinct CASE WHEN SUBSTR(TELB.txn_elb_tm,1,2) in (18, 19, 20, 21, 22, 23) THEN TELB.elb_sessn_id ELSE NULL END) as c_log_owner_evening_elb,
            -----   PRIHLASOVANIE - DNI v TYZDNI
            COUNT(distinct CASE WHEN TO_CHAR(TELB.txn_elb_dt, 'D') in ('1', '2', '3', '4', '5') THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_owner_weekday_elb,
            COUNT(distinct CASE WHEN TO_CHAR(TELB.txn_elb_dt, 'D') in ('6', '7') THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_owner_weekend_elb,
            COUNT(distinct CASE WHEN TO_CHAR(TELB.txn_elb_dt, 'D') in ('1', '2', '3', '4', '5') THEN TELB.elb_sessn_id ELSE NULL END) as c_log_owner_weekday_elb,
            COUNT(distinct CASE WHEN TO_CHAR(TELB.txn_elb_dt, 'D') in ('6', '7') THEN TELB.elb_sessn_id ELSE NULL END) as c_log_owner_weekend_elb,
            -----   MESACNY
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt THEN TELB.txn_elb_dt ELSE NULL END) as c_daylog_owner_elb,
            COUNT(distinct CASE WHEN TELB.txn_elb_dt BETWEEN add_months(process_dt, -1) AND process_dt THEN TELB.elb_sessn_id ELSE NULL END) as c_log_owner_elb
            
        FROM ( SELECT * from pm_owner.stg_elb_event
                where chanl_id IN (1111, 1116, 1319, 2172, 1179)) TELB
        GROUP BY
            TELB.CUSTOMERID_ELB_OWNER;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    step(p_stepname => l_proc_name || ' INSERT TMP_ELB_TXN', p_source => 'L0.*', p_target => 'TMP_ELB_TXN');
    INSERT INTO PM_OWNER.TMP_ELB_TXN
        SELECT
           ETXN.CUSTOMERID_RIGHTS customerid,
           COUNT(ETXN.txn_elb_id) as c_elb_txn_m,
           SUM(ETXN.txn_elb_bc_am) as sum_elb_txn_m,
           SUM(ETXN.txn_elb_bc_am)/GREATEST(COUNT(ETXN.txn_elb_id), 1) as avg_elb_txn_m,
           
           COUNT(CASE WHEN ETXN.txn_cd_cd in (1244) THEN ETXN.txn_elb_id ELSE NULL END) as c_elb_foreign_txn_m,
           SUM(CASE WHEN ETXN.txn_cd_cd in (1244) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_elb_foreign_txn_m,
           COUNT(CASE WHEN ETXN.txn_elb_bc_am != ETXN.txn_elb_am THEN ETXN.txn_elb_id ELSE NULL END) as c_elb_diffcurr_txn_m,
           SUM(CASE WHEN ETXN.txn_elb_bc_am != ETXN.txn_elb_am THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_elb_diffcurr_txn_m,
           MAX(CASE WHEN ETXN.txn_cd_cd in (4909) THEN 1 ELSE 0 END) as i_extra_mtg_elb_txn_m,
           SUM(CASE WHEN ETXN.txn_cd_cd in (4909) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_extra_mtg_elb_txn_m,
           
           MAX(CASE WHEN ETXN.txn_cd_cd in (23543) THEN 1 ELSE 0 END) as i_created_stand_order_elb_m,
           MAX(CASE WHEN ETXN.txn_cd_cd in (23544) THEN 1 ELSE 0 END) as i_changed_stand_order_elb_m,
           MAX(CASE WHEN ETXN.txn_cd_cd in (23545) THEN 1 ELSE 0 END) as i_closed_stand_order_elb_m,
           MAX(CASE WHEN ETXN.txn_cd_cd in (22411) THEN 1 ELSE 0 END) as i_cancelled_elb_txn_m,
           
           MAX(CASE WHEN ETXN.txn_cd_cd in (23548) THEN 1 ELSE 0 END) as I_PAY_OFF_CC_ELB_M,
           SUM(CASE WHEN ETXN.txn_cd_cd in (23548) THEN ETXN.txn_elb_bc_am ELSE 0 END) as SUM_PAY_OFF_CC_ELB_M,
           
           MAX(CASE WHEN ETXN.txn_cd_cd in (23603) THEN 1 ELSE 0 END) as i_bulk_elb_txn_m,
           SUM(CASE WHEN ETXN.txn_cd_cd in (23603) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_bulk_elb_txn_m,
           
           COUNT(CASE WHEN ETXN.chanl_id in (1116, 2172) THEN ETXN.txn_elb_id ELSE NULL END) as c_ib_txn_m,
           COUNT(CASE WHEN ETXN.chanl_id in (1319) THEN ETXN.txn_elb_id ELSE NULL END) as c_sb_txn_m,
           COUNT(CASE WHEN ETXN.chanl_id in (1178, 1179) THEN ETXN.txn_elb_id ELSE NULL END) as c_bb_txn_m,
        
           SUM(CASE WHEN ETXN.chanl_id in (1116, 2172) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_ib_txn_m,
           SUM(CASE WHEN ETXN.chanl_id in (1319) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_sb_txn_m,
           SUM(CASE WHEN ETXN.chanl_id in (1178, 1179) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_bb_txn_m,
           
           COUNT(CASE WHEN SUBSTR(ETXN.txn_elb_tm,1,2) in (0, 1, 2, 3, 4, 5) THEN ETXN.txn_elb_id ELSE NULL END) as c_night_elb_txn_m,
           COUNT(CASE WHEN SUBSTR(ETXN.txn_elb_tm,1,2) in (6, 7, 8, 9, 10, 11) THEN ETXN.txn_elb_id ELSE NULL END) as c_morning_elb_txn_m,
           COUNT(CASE WHEN SUBSTR(ETXN.txn_elb_tm,1,2) in (12, 13, 14, 15, 16, 17) THEN ETXN.txn_elb_id ELSE NULL END) as c_afternoon_elb_txn_m,
           COUNT(CASE WHEN SUBSTR(ETXN.txn_elb_tm,1,2) in (18, 19, 20, 21, 22, 23) THEN ETXN.txn_elb_id ELSE NULL END) as c_evening_elb_txn_m,
           -----   PRIHLASOVANIE - DNI v TYZDNI
           COUNT(CASE WHEN TO_CHAR(ETXN.txn_elb_dt, 'D') in ('1', '2', '3', '4', '5') THEN ETXN.txn_elb_id ELSE NULL END)/
                         (select count(trunc(process_dt,'MM') +(level-1)) from dual
                          where to_char(trunc(process_dt,'MM') +(level-1),'D') not in ('6', '7')
                          connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) as c_weekday_elb_txn_m,
           COUNT(CASE WHEN TO_CHAR(ETXN.txn_elb_dt, 'D') in ('6', '7') THEN ETXN.txn_elb_id ELSE NULL END)/
                         (select count(trunc(process_dt,'MM') +(level-1)) from dual
                          where to_char(trunc(process_dt,'MM') +(level-1),'D') in ('6', '7')
                          connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) as c_weekend_elb_txn_m,
                          
           COUNT(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS THEN ETXN.txn_elb_id ELSE NULL END) as c_elb_txn_holder_m,
           SUM(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_elb_txn_holder_m,
           SUM(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS THEN ETXN.txn_elb_bc_am ELSE 0 END)
            /GREATEST(COUNT(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS THEN ETXN.txn_elb_id ELSE NULL END), 1) as avg_elb_txn_holder_m,
    
           COUNT(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS AND PARTY_SUBTP_CD != 0 THEN ETXN.txn_elb_id ELSE NULL END) as c_noretail_elb_txn_holder_m,
           SUM(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS AND PARTY_SUBTP_CD != 0 THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_noretail_elb_txn_holder_m,
           
           COUNT(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS AND ETXN.chanl_id in (1116, 2172) THEN ETXN.txn_elb_id ELSE NULL END) as c_ib_txn_holder_m,
           COUNT(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS AND ETXN.chanl_id in (1319) THEN ETXN.txn_elb_id ELSE NULL END) as c_sb_txn_holder_m,
           COUNT(CASE WHEN ETXN.CUSTOMERID_ELB_OWNER != ETXN.CUSTOMERID_RIGHTS AND ETXN.chanl_id in (1178, 1179) THEN ETXN.txn_elb_id ELSE NULL END) as c_bb_txn_holder_m                
                          
        FROM (select * from pm_owner.stg_elb_event
			  where 1=1
			  and txn_elb_bc_am is not null
			  and txn_elb_bc_am < 900000000 -- exclude too big unrealistic txn
			  and txn_cd_cd not in (-63045, -63048, -63076, 5035, 5036, 5037) -- exclude direct debit
			  ) ETXN     
        GROUP BY ETXN.CUSTOMERID_RIGHTS;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    INSERT INTO PM_OWNER.TMP_ELB_TXN_OWNER
        SELECT
           ETXN.CUSTOMERID_ELB_OWNER customerid,
           COUNT(ETXN.txn_elb_id) as c_elb_txn_owner_m,
           SUM(ETXN.txn_elb_bc_am) as sum_elb_txn_owner_m,
           SUM(ETXN.txn_elb_bc_am)/GREATEST(COUNT(ETXN.txn_elb_id), 1) as avg_elb_txn_owner_m,
           
           COUNT(CASE WHEN ETXN.txn_cd_cd in (1244) THEN ETXN.txn_elb_id ELSE NULL END) as c_elb_foreign_txn_owner_m,
           SUM(CASE WHEN ETXN.txn_cd_cd in (1244) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_elb_foreign_txn_owner_m,
           COUNT(CASE WHEN ETXN.txn_elb_bc_am != ETXN.txn_elb_am THEN ETXN.txn_elb_id ELSE NULL END) as c_elb_foreign_curr_txn_owner_m,
           SUM(CASE WHEN ETXN.txn_elb_bc_am != ETXN.txn_elb_am THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_elb_foreign_curr_txn_owner_m,
           MAX(CASE WHEN ETXN.txn_cd_cd in (4909) THEN 1 ELSE 0 END) as i_extra_mtg_elb_txn_owner_m,
           SUM(CASE WHEN ETXN.txn_cd_cd in (4909) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_extra_mtg_elb_txn_owner_m,
           
           MAX(CASE WHEN ETXN.txn_cd_cd in (23603) THEN 1 ELSE 0 END) as i_bulk_elb_txn_owner_m,
           SUM(CASE WHEN ETXN.txn_cd_cd in (23603) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_bulk_elb_txn_owner_m,
           
           COUNT(CASE WHEN ETXN.chanl_id in (1116, 2172) THEN ETXN.txn_elb_id ELSE NULL END) as c_ib_txn_owner_m,
           COUNT(CASE WHEN ETXN.chanl_id in (1319) THEN ETXN.txn_elb_id ELSE NULL END) as c_sb_txn_owner_m,
           COUNT(CASE WHEN ETXN.chanl_id in (1178, 1179) THEN ETXN.txn_elb_id ELSE NULL END) as c_bb_txn_owner_m,
        
           SUM(CASE WHEN ETXN.chanl_id in (1116, 2172) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_ib_txn_owner_m,
           SUM(CASE WHEN ETXN.chanl_id in (1319) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_sb_txn_owner_m,
           SUM(CASE WHEN ETXN.chanl_id in (1178, 1179) THEN ETXN.txn_elb_bc_am ELSE 0 END) as sum_bb_txn_owner_m,
           
           COUNT(CASE WHEN SUBSTR(ETXN.txn_elb_tm,1,2) in (0, 1, 2, 3, 4, 5) THEN ETXN.txn_elb_id ELSE NULL END) as c_night_elb_txn_owner_m,
           COUNT(CASE WHEN SUBSTR(ETXN.txn_elb_tm,1,2) in (6, 7, 8, 9, 10, 11) THEN ETXN.txn_elb_id ELSE NULL END) as c_morning_elb_txn_owner_m,
           COUNT(CASE WHEN SUBSTR(ETXN.txn_elb_tm,1,2) in (12, 13, 14, 15, 16, 17) THEN ETXN.txn_elb_id ELSE NULL END) as c_afternoon_elb_txn_owner_m,
           COUNT(CASE WHEN SUBSTR(ETXN.txn_elb_tm,1,2) in (18, 19, 20, 21, 22, 23) THEN ETXN.txn_elb_id ELSE NULL END) as c_evening_elb_txn_owner_m,
           -----   PRIHLASOVANIE - DNI v TYZDNI
           COUNT(CASE WHEN TO_CHAR(ETXN.txn_elb_dt, 'D') in ('1', '2', '3', '4', '5') THEN ETXN.txn_elb_id ELSE NULL END)/
                         (select count(trunc(process_dt,'MM') +(level-1)) from dual
                          where to_char(trunc(process_dt,'MM') +(level-1),'D') not in ('6', '7')
                          connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) as c_weekday_elb_txn_owner_m,
           COUNT(CASE WHEN TO_CHAR(ETXN.txn_elb_dt, 'D') in ('6', '7') THEN ETXN.txn_elb_id ELSE NULL END)/
                         (select count(trunc(process_dt,'MM') +(level-1)) from dual
                          where to_char(trunc(process_dt,'MM') +(level-1),'D') in ('6', '7')
                          connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) as c_weekend_elb_txn_owner_m            
                          
        FROM (select * from pm_owner.stg_elb_event
			  where 1=1
			  and txn_elb_bc_am is not null
			  and txn_elb_bc_am < 900000000 -- exclude too big unrealistic txn
			  and txn_cd_cd not in (-63045, -63048, -63076, 5035, 5036, 5037) -- exclude direct debit
			  ) ETXN    
        GROUP BY ETXN.CUSTOMERID_ELB_OWNER;

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;

    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);
    
	INSERT INTO PM_OWNER.ATTR_ELB
        SELECT
            main.month_id month_id,
            main.customerid customerid,
            COALESCE(ippid.recency_open_ippid, process_dt - process_dt_hist) recency_open_ippid,
            COALESCE(ippid.c_active_ippid, 0) c_active_ippid,
            COALESCE(ippid.c_closed_ippid, 0) c_closed_ippid,
            COALESCE(ippid.c_blocked_ippid, 0) c_blocked_ippid,  
            COALESCE(ippid.c_total_ippid, 0) c_total_ippid,
            COALESCE(ippid.c_client_elb_authorized, 0) c_client_elb_authorized,
            COALESCE(ippid.i_active_owner_ippid, 0) i_active_owner_ippid,
            COALESCE(ippid.i_active_authorized_ippid, 0) i_active_authorized_ippid,
            COALESCE(ippid.max_limit_txn_ib, 0) max_limit_txn_ib,
            COALESCE(ippid.min_limit_txn_ib, 0) min_limit_txn_ib,
            COALESCE(ippid.c_authent_type_elb, 0) c_authent_type_elb,   
            COALESCE(ippid.i_sms_authent_elb, 0) i_sms_authent_elb,
            COALESCE(ippid.i_pin_authent_elb, 0) i_pin_authent_elb,
            COALESCE(ippid.i_token_authent_elb, 0) i_token_authent_elb,
            COALESCE(ippid.i_mtoken_authent_elb, 0) i_mtoken_authent_elb,
                   
            COALESCE(ippid.i_opened_ippid_m, 0) i_opened_ippid_m,
            GREATEST(COALESCE(elb_q.i_opened_ippid_q, 0), COALESCE(ippid.i_opened_ippid_m, 0)) i_opened_ippid_q,
            GREATEST(COALESCE(elb_y.i_opened_ippid_y, 0), COALESCE(ippid.i_opened_ippid_m, 0)) i_opened_ippid_y,
            
            COALESCE(ippid_holder.c_total_ippid_holder, 0) c_total_ippid_holder,
            COALESCE(ippid_holder.c_access_elb_holder, 0) c_access_elb_holder,
            COALESCE(ippid_holder.i_active_ippid_holder, 0) i_active_ippid_holder,
            COALESCE(ippid_holder.c_active_ret_ippid_holder, 0) c_active_ret_ippid_holder,
            COALESCE(ippid_holder.c_active_noret_ippid_holder, 0) c_active_noret_ippid_holder,
            
            COALESCE(ippid_holder.max_limit_txn_ib_holder, 0) max_limit_txn_ib_holder,
            COALESCE(ippid_holder.min_limit_txn_ib_holder, 0) min_limit_txn_ib_holder,
            COALESCE(ippid_holder.c_authent_type_elb_holder, 0) c_authent_type_elb_holder,
            COALESCE(ippid_holder.i_sms_authent_elb_holder, 0) i_sms_authent_elb_holder,
            COALESCE(ippid_holder.i_pin_authent_elb_holder, 0) i_pin_authent_elb_holder,
            COALESCE(ippid_holder.i_token_authent_elb_holder, 0) i_token_authent_elb_holder,
            COALESCE(ippid_holder.i_mtoken_authent_elb_holder, 0) i_mtoken_authent_elb_holder,
            
            COALESCE(ippid_holder.i_opened_ippid_holder_m, 0) i_opened_ippid_holder_m,
            GREATEST(COALESCE(elb_q.i_opened_ippid_holder_q, 0), COALESCE(ippid_holder.i_opened_ippid_holder_m, 0)) i_opened_ippid_holder_q,
            GREATEST(COALESCE(elb_y.i_opened_ippid_holder_y, 0), COALESCE(ippid_holder.i_opened_ippid_holder_m, 0)) i_opened_ippid_holder_y,
               
            COALESCE(ippid_prod.c_product_access_ippid, 0) c_product_access_ippid,
            COALESCE(ippid_prod.c_dda_access_ippid, 0) c_dda_access_ippid,
            COALESCE(ippid_prod.c_savings_access_ippid, 0) c_savings_access_ippid,
            COALESCE(ippid_prod.c_loans_access_ippid, 0) c_loans_access_ippid,
            COALESCE(ippid_prod.c_product_access_ippid_holder, 0) c_product_access_ippid_holder,
            COALESCE(ippid_prod.c_dda_access_ippid_holder, 0) c_dda_access_ippid_holder,
            COALESCE(ippid_prod.c_savings_access_ippid_holder, 0) c_savings_access_ippid_holder,
            COALESCE(ippid_prod.c_loans_access_ippid_holder, 0) c_loans_access_ippid_holder,
            
            COALESCE(logs.recency_log_elb_m, 0) recency_log_elb_m,
            COALESCE(logs.c_log_last_day_elb_m, 0) c_log_last_day_elb_m,
            COALESCE(logs.c_daylog_last_week_elb_m, 0) c_daylog_last_week_elb_m,
            COALESCE(logs.c_log_last_week_elb_m, 0) c_log_last_week_elb_m,
            
            COALESCE(logs.c_log_night_elb_m, 0) c_log_night_elb_m,
            COALESCE(logs.c_log_morning_elb_m, 0) c_log_morning_elb_m,
            COALESCE(logs.c_log_afternoon_elb_m, 0) c_log_afternoon_elb_m,
            COALESCE(logs.c_log_evening_elb_m, 0) c_log_evening_elb_m,
            
            COALESCE(logs.ratio_log_night_elb_m, 0) ratio_log_night_elb_m,
            COALESCE(logs.ratio_log_morning_elb_m, 0) ratio_log_morning_elb_m,
            COALESCE(logs.ratio_log_afternoon_elb_m, 0) ratio_log_afternoon_elb_m,
            COALESCE(logs.ratio_log_evening_elb_m, 0) ratio_log_evening_elb_m,
            
            COALESCE(logs.RATIO_DAYLOG_WEEKDAY_ELB_M, 0) RATIO_DAYLOG_WEEKDAY_ELB_M,
            COALESCE(logs.RATIO_DAYLOG_WEEKEND_ELB_M, 0) RATIO_DAYLOG_WEEKEND_ELB_M,
            COALESCE(logs.AVG_LOG_WEEKDAY_ELB_M, 0) AVG_LOG_WEEKDAY_ELB_M,
            COALESCE(logs.AVG_LOG_WEEKEND_ELB_M, 0) AVG_LOG_WEEKEND_ELB_M,
            COALESCE(logs.C_DAYLOG_ELB_M, 0) C_DAYLOG_ELB_M,
            COALESCE(logs.C_LOG_ELB_M, 0) C_LOG_ELB_M,
            
            (COALESCE(elb_q.c_daylog_elb_q, 0) + COALESCE(logs.c_daylog_elb_m, 0))/main.c_month_q c_daylog_elb_q,
            (COALESCE(elb_q.c_log_elb_q, 0) + COALESCE(logs.c_log_elb_m, 0))/main.c_month_q c_log_elb_q,
            (COALESCE(elb_y.c_daylog_elb_y, 0) + COALESCE(logs.c_daylog_elb_m, 0))/main.c_month_y c_daylog_elb_y,
            (COALESCE(elb_y.c_log_elb_y, 0) + COALESCE(logs.c_log_elb_m, 0))/main.c_month_y c_log_elb_y,
               
            
            CASE WHEN (COALESCE(elb_q.c_daylog_elb_q, 0) + COALESCE(logs.c_daylog_elb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_daylog_elb_m, 0)/GREATEST((COALESCE(elb_q.c_daylog_elb_q, 0) + COALESCE(logs.c_daylog_elb_m, 0))/main.c_month_q, 0.0001) - 1 END rel_diff_c_daylog_elb_q,
            CASE WHEN (COALESCE(elb_q.c_log_elb_q, 0) + COALESCE(logs.c_log_elb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_log_elb_m, 0)/GREATEST((COALESCE(elb_q.c_log_elb_q, 0) + COALESCE(logs.c_log_elb_m, 0))/main.c_month_q, 0.0001) - 1 END rel_diff_c_log_elb_q,
            CASE WHEN (COALESCE(elb_y.c_daylog_elb_y, 0) + COALESCE(logs.c_daylog_elb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_daylog_elb_m, 0)/GREATEST((COALESCE(elb_y.c_daylog_elb_y, 0) + COALESCE(logs.c_daylog_elb_m, 0))/main.c_month_y, 0.0001) - 1 END rel_diff_c_daylog_elb_y,
            CASE WHEN (COALESCE(elb_y.c_log_elb_y, 0) + COALESCE(logs.c_log_elb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_log_elb_m, 0)/GREATEST((COALESCE(elb_y.c_log_elb_y, 0) + COALESCE(logs.c_log_elb_m, 0))/main.c_month_y, 0.0001) - 1 END rel_diff_c_log_elb_y,
        
            COALESCE(logs.C_DAYLOG_ELB_HOLDER_M, 0) C_DAYLOG_ELB_HOLDER_M,
            COALESCE(logs.C_LOG_ELB_HOLDER_M, 0) C_LOG_ELB_HOLDER_M,
            (COALESCE(elb_q.c_daylog_elb_holder_q, 0) + COALESCE(logs.c_daylog_elb_holder_m, 0))/main.c_month_q c_daylog_elb_holder_q,
            (COALESCE(elb_q.c_log_elb_holder_q, 0) + COALESCE(logs.c_log_elb_holder_m, 0))/main.c_month_q c_log_elb_holder_q,
            (COALESCE(elb_y.c_daylog_elb_holder_y, 0) + COALESCE(logs.c_daylog_elb_holder_m, 0))/main.c_month_y c_daylog_elb_holder_y,
            (COALESCE(elb_y.c_log_elb_holder_y, 0) + COALESCE(logs.c_log_elb_holder_m, 0))/main.c_month_y c_log_elb_holder_y,
        
            COALESCE(logs.C_DAYLOG_NORETAIL_ELB_HOLDER_M, 0) C_DAYLOG_NORETAIL_ELB_M,
            COALESCE(logs.C_LOG_NORETAIL_ELB_HOLDER_M, 0) C_LOG_NORETAIL_ELB_M,
            (COALESCE(elb_q.c_daylog_noretail_elb_holder_q, 0) + COALESCE(logs.c_daylog_noretail_elb_holder_m, 0))/main.c_month_q c_daylog_noretail_elb_holder_q,
            (COALESCE(elb_q.c_log_noretail_elb_holder_q, 0) + COALESCE(logs.c_log_noretail_elb_holder_m, 0))/main.c_month_q c_log_noretail_elb_holder_q,
            (COALESCE(elb_y.c_daylog_noretail_elb_holder_y, 0) + COALESCE(logs.c_daylog_noretail_elb_holder_m, 0))/main.c_month_y c_daylog_noretail_elb_holder_y,
            (COALESCE(elb_y.c_log_noretail_elb_holder_y, 0) + COALESCE(logs.c_log_noretail_elb_holder_m, 0))/main.c_month_y c_log_noretail_elb_holder_y,
            
            COALESCE(logs.C_DAYLOG_IB_M, 0) C_DAYLOG_IB_M,
            COALESCE(logs.C_LOG_IB_M, 0) C_LOG_IB_M,
            COALESCE(logs.C_DAYLOG_SB_M, 0) C_DAYLOG_SB_M,
            COALESCE(logs.C_LOG_SB_M, 0) C_LOG_SB_M,
            COALESCE(logs.C_DAYLOG_BB_M, 0) C_DAYLOG_BB_M,
            COALESCE(logs.C_LOG_BB_M, 0) C_LOG_BB_M,
            
            (COALESCE(elb_q.c_daylog_ib_q, 0) + COALESCE(logs.c_daylog_ib_m, 0))/main.c_month_q c_daylog_ib_q,
            (COALESCE(elb_q.c_log_ib_q, 0) + COALESCE(logs.c_log_ib_m, 0))/main.c_month_q c_log_ib_q,
            (COALESCE(elb_y.c_daylog_ib_y, 0) + COALESCE(logs.c_daylog_ib_m, 0))/main.c_month_y c_daylog_ib_y,
            (COALESCE(elb_y.c_log_ib_y, 0) + COALESCE(logs.c_log_ib_m, 0))/main.c_month_y c_log_ib_y,
            
            (COALESCE(elb_q.c_daylog_sb_q, 0) + COALESCE(logs.c_daylog_sb_m, 0))/main.c_month_q c_daylog_sb_q,
            (COALESCE(elb_q.c_log_sb_q, 0) + COALESCE(logs.c_log_sb_m, 0))/main.c_month_q c_log_sb_q,
            (COALESCE(elb_y.c_daylog_sb_y, 0) + COALESCE(logs.c_daylog_sb_m, 0))/main.c_month_y c_daylog_sb_y,
            (COALESCE(elb_y.c_log_sb_y, 0) + COALESCE(logs.c_log_sb_m, 0))/main.c_month_y c_log_sb_y,
            
            (COALESCE(elb_q.c_daylog_bb_q, 0) + COALESCE(logs.c_daylog_bb_m, 0))/main.c_month_q c_daylog_bb_q,
            (COALESCE(elb_q.c_log_bb_q, 0) + COALESCE(logs.c_log_bb_m, 0))/main.c_month_q c_log_bb_q,
            (COALESCE(elb_y.c_daylog_bb_y, 0) + COALESCE(logs.c_daylog_bb_m, 0))/main.c_month_y c_daylog_bb_y,
            (COALESCE(elb_y.c_log_bb_y, 0) + COALESCE(logs.c_log_bb_m, 0))/main.c_month_y c_log_bb_y,
            
            CASE WHEN (COALESCE(elb_q.c_daylog_ib_q, 0) + COALESCE(logs.c_daylog_ib_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_daylog_ib_m, 0)/GREATEST((COALESCE(elb_q.c_daylog_ib_q, 0) + COALESCE(logs.c_daylog_ib_m, 0))/main.c_month_q, 0.0001) - 1 END rel_diff_c_daylog_ib_q,
            CASE WHEN (COALESCE(elb_q.c_log_ib_q, 0) + COALESCE(logs.c_log_ib_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_log_ib_m, 0)/GREATEST((COALESCE(elb_q.c_log_ib_q, 0) + COALESCE(logs.c_log_ib_m, 0))/main.c_month_q, 0.0001) - 1 END rel_diff_c_log_ib_q,                 
            CASE WHEN (COALESCE(elb_y.c_daylog_ib_y, 0) + COALESCE(logs.c_daylog_ib_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_daylog_ib_m, 0)/GREATEST((COALESCE(elb_y.c_daylog_ib_y, 0) + COALESCE(logs.c_daylog_ib_m, 0))/main.c_month_y, 0.0001) - 1 END rel_diff_c_daylog_ib_y,
            CASE WHEN (COALESCE(elb_y.c_log_ib_y, 0) + COALESCE(logs.c_log_ib_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_log_ib_m, 0)/GREATEST((COALESCE(elb_y.c_log_ib_y, 0) + COALESCE(logs.c_log_ib_m, 0))/main.c_month_y, 0.0001) - 1 END rel_diff_c_log_ib_y,
            
            CASE WHEN (COALESCE(elb_q.c_daylog_sb_q, 0) + COALESCE(logs.c_daylog_sb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_daylog_sb_m, 0)/GREATEST((COALESCE(elb_q.c_daylog_sb_q, 0) + COALESCE(logs.c_daylog_sb_m, 0))/main.c_month_q, 0.0001) - 1 END rel_diff_c_daylog_sb_q,
            CASE WHEN (COALESCE(elb_q.c_log_sb_q, 0) + COALESCE(logs.c_log_sb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_log_sb_m, 0)/GREATEST((COALESCE(elb_q.c_log_sb_q, 0) + COALESCE(logs.c_log_sb_m, 0))/main.c_month_q, 0.0001) - 1 END rel_diff_c_log_sb_q,
            CASE WHEN (COALESCE(elb_y.c_daylog_sb_y, 0) + COALESCE(logs.c_daylog_sb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_daylog_sb_m, 0)/GREATEST((COALESCE(elb_y.c_daylog_sb_y, 0) + COALESCE(logs.c_daylog_sb_m, 0))/main.c_month_y, 0.0001) - 1 END rel_diff_c_daylog_sb_y,
            CASE WHEN (COALESCE(elb_y.c_log_sb_y, 0) + COALESCE(logs.c_log_sb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_log_sb_m, 0)/GREATEST((COALESCE(elb_y.c_log_sb_y, 0) + COALESCE(logs.c_log_sb_m, 0))/main.c_month_y, 0.0001) - 1 END rel_diff_c_log_sb_y,
            
            CASE WHEN (COALESCE(elb_q.c_daylog_bb_q, 0) + COALESCE(logs.c_daylog_bb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_daylog_bb_m, 0)/GREATEST((COALESCE(elb_q.c_daylog_bb_q, 0) + COALESCE(logs.c_daylog_bb_m, 0))/main.c_month_q, 0.0001) - 1 END rel_diff_c_daylog_bb_q,
            CASE WHEN (COALESCE(elb_q.c_log_bb_q, 0) + COALESCE(logs.c_log_bb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_log_bb_m, 0)/GREATEST((COALESCE(elb_q.c_log_bb_q, 0) + COALESCE(logs.c_log_bb_m, 0))/main.c_month_q, 0.0001) - 1 END rel_diff_c_log_bb_q,
            CASE WHEN (COALESCE(elb_y.c_daylog_bb_y, 0) + COALESCE(logs.c_daylog_bb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_daylog_bb_m, 0)/GREATEST((COALESCE(elb_y.c_daylog_bb_y, 0) + COALESCE(logs.c_daylog_bb_m, 0))/main.c_month_y, 0.0001) - 1 END rel_diff_c_daylog_bb_y,
            CASE WHEN (COALESCE(elb_y.c_log_bb_y, 0) + COALESCE(logs.c_log_bb_m, 0)) = 0 THEN 0
                ELSE COALESCE(logs.c_log_bb_m, 0)/GREATEST((COALESCE(elb_y.c_log_bb_y, 0) + COALESCE(logs.c_log_bb_m, 0))/main.c_month_y, 0.0001) - 1 END rel_diff_c_log_bb_y,
            
            COALESCE(txn.c_elb_txn_m, 0) c_elb_txn_m,
            COALESCE(txn.sum_elb_txn_m, 0) sum_elb_txn_m,
            COALESCE(txn.avg_elb_txn_m, 0) avg_elb_txn_m,
            
            (COALESCE(elb_q.c_elb_txn_q, 0) + COALESCE(txn.c_elb_txn_m, 0))/main.c_month_q c_elb_txn_q,
            (COALESCE(elb_q.sum_elb_txn_q, 0) + COALESCE(txn.sum_elb_txn_m, 0))/main.c_month_q sum_elb_txn_q,
            (COALESCE(elb_q.avg_elb_txn_q, 0) + COALESCE(txn.avg_elb_txn_m, 0))/main.c_month_q avg_elb_txn_q,
            
            (COALESCE(elb_y.c_elb_txn_y, 0) + COALESCE(txn.c_elb_txn_m, 0))/main.c_month_y c_elb_txn_y,
            (COALESCE(elb_y.sum_elb_txn_y, 0) + COALESCE(txn.sum_elb_txn_m, 0))/main.c_month_y sum_elb_txn_y,
            (COALESCE(elb_y.avg_elb_txn_y, 0) + COALESCE(txn.avg_elb_txn_m, 0))/main.c_month_y avg_elb_txn_y,
            
            COALESCE(txn.c_elb_foreign_txn_m, 0) c_elb_foreign_txn_m,
            COALESCE(txn.sum_elb_foreign_txn_m, 0) sum_elb_foreign_txn_m,
            COALESCE(txn.c_elb_diffcurr_txn_m, 0) c_elb_diffcurr_txn_m,
            COALESCE(txn.sum_elb_diffcurr_txn_m, 0) sum_elb_diffcurr_txn_m,
            COALESCE(txn.i_extra_mtg_elb_txn_m, 0) i_extra_mtg_elb_txn_m,
            GREATEST(COALESCE(elb_q.i_extra_mtg_elb_txn_q, 0), COALESCE(txn.i_extra_mtg_elb_txn_m, 0)) i_extra_mtg_elb_txn_q,
            GREATEST(COALESCE(elb_y.i_extra_mtg_elb_txn_y, 0), COALESCE(txn.i_extra_mtg_elb_txn_m, 0)) i_extra_mtg_elb_txn_y, 
            COALESCE(txn.sum_extra_mtg_elb_txn_m, 0) sum_extra_mtg_elb_txn,
            GREATEST(COALESCE(elb_q.sum_extra_mtg_elb_txn_q, 0), COALESCE(txn.sum_extra_mtg_elb_txn_m, 0)) sum_extra_mtg_elb_txn_q,
            GREATEST(COALESCE(elb_y.sum_extra_mtg_elb_txn_y, 0), COALESCE(txn.sum_extra_mtg_elb_txn_m, 0)) sum_extra_mtg_elb_txn_y, 
            
            COALESCE(txn.i_created_stand_order_elb_m, 0) i_created_stand_order_elb_m,
            COALESCE(txn.i_changed_stand_order_elb_m, 0) i_changed_stand_order_elb_m,
            COALESCE(txn.i_closed_stand_order_elb_m, 0) i_closed_stand_order_elb_m,
            COALESCE(txn.i_cancelled_elb_txn_m, 0) i_cancelled_elb_txn_m,
            
            GREATEST(COALESCE(elb_q.i_created_stand_order_elb_q, 0), COALESCE(txn.i_created_stand_order_elb_m, 0)) i_created_stand_order_elb_q,
            GREATEST(COALESCE(elb_q.i_changed_stand_order_elb_q, 0), COALESCE(txn.i_changed_stand_order_elb_m, 0)) i_changed_stand_order_elb_q,
            
            GREATEST(COALESCE(elb_y.i_created_stand_order_elb_y, 0), COALESCE(txn.i_created_stand_order_elb_m, 0)) i_created_stand_order_elb_y,
            GREATEST(COALESCE(elb_y.i_changed_stand_order_elb_y, 0), COALESCE(txn.i_changed_stand_order_elb_m, 0)) i_changed_stand_order_elb_y,
            
            COALESCE(txn.I_PAY_OFF_CC_ELB_M, 0) I_PAY_OFF_CC_ELB_M,
            COALESCE(txn.SUM_PAY_OFF_CC_ELB_M, 0) SUM_PAY_OFF_CC_ELB_M,
            
            COALESCE(txn.i_bulk_elb_txn_m, 0) i_bulk_elb_txn_m,
            COALESCE(txn.sum_bulk_elb_txn_m, 0) sum_bulk_elb_txn_m,
            
            GREATEST(COALESCE(elb_q.I_PAY_OFF_CC_ELB_Q, 0), COALESCE(txn.I_PAY_OFF_CC_ELB_M, 0)) I_PAY_OFF_CC_ELB_Q,
            (COALESCE(elb_q.SUM_PAY_OFF_CC_ELB_Q, 0) + COALESCE(txn.SUM_PAY_OFF_CC_ELB_M, 0))/main.c_month_q SUM_PAY_OFF_CC_ELB_Q,
            
            GREATEST(COALESCE(elb_q.i_bulk_elb_txn_q, 0), COALESCE(txn.i_bulk_elb_txn_m, 0)) i_bulk_elb_txn_q,
            (COALESCE(elb_q.sum_bulk_elb_txn_q, 0) + COALESCE(txn.sum_bulk_elb_txn_m, 0))/main.c_month_q sum_bulk_elb_txn_q,
            
            GREATEST(COALESCE(elb_y.I_PAY_OFF_CC_ELB_Y, 0), COALESCE(txn.I_PAY_OFF_CC_ELB_M, 0)) I_PAY_OFF_CC_ELB_Y,
            (COALESCE(elb_y.SUM_PAY_OFF_CC_ELB_Y, 0) + COALESCE(txn.SUM_PAY_OFF_CC_ELB_M, 0))/main.c_month_y SUM_PAY_OFF_CC_ELB_Y,
            
            GREATEST(COALESCE(elb_y.i_bulk_elb_txn_y, 0), COALESCE(txn.i_bulk_elb_txn_m, 0)) i_bulk_elb_txn_y,
            (COALESCE(elb_y.sum_bulk_elb_txn_y, 0) + COALESCE(txn.sum_bulk_elb_txn_m, 0))/main.c_month_y sum_bulk_elb_txn_y,
            
            COALESCE(txn.c_ib_txn_m, 0) c_ib_txn_m,
            COALESCE(txn.c_sb_txn_m, 0) c_sb_txn_m,
            COALESCE(txn.c_bb_txn_m, 0) c_bb_txn_m,
            
            COALESCE(txn.sum_ib_txn_m, 0) sum_ib_txn_m,
            COALESCE(txn.sum_sb_txn_m, 0) sum_sb_txn_m,
            COALESCE(txn.sum_bb_txn_m, 0) sum_bb_txn_m,
            
            (COALESCE(elb_q.sum_ib_txn_q, 0) + COALESCE(txn.c_ib_txn_m, 0))/main.c_month_q sum_ib_txn_q,
            (COALESCE(elb_q.sum_sb_txn_q, 0) + COALESCE(txn.c_sb_txn_m, 0))/main.c_month_q sum_sb_txn_q,
            (COALESCE(elb_q.sum_bb_txn_q, 0) + COALESCE(txn.c_bb_txn_m, 0))/main.c_month_q sum_bb_txn_q,
            
            (COALESCE(elb_y.sum_ib_txn_y, 0) + COALESCE(txn.c_ib_txn_m, 0))/main.c_month_y sum_ib_txn_y,
            (COALESCE(elb_y.sum_sb_txn_y, 0) + COALESCE(txn.c_sb_txn_m, 0))/main.c_month_y sum_sb_txn_y,
            (COALESCE(elb_y.sum_bb_txn_y, 0) + COALESCE(txn.c_bb_txn_m, 0))/main.c_month_y sum_bb_txn_y,
            
            COALESCE(txn.c_night_elb_txn_m, 0) c_night_elb_txn_m,
            COALESCE(txn.c_morning_elb_txn_m, 0) c_morning_elb_txn_m,
            COALESCE(txn.c_afternoon_elb_txn_m, 0) c_afternoon_elb_txn_m,
            COALESCE(txn.c_evening_elb_txn_m, 0) c_evening_elb_txn_m,
            
            COALESCE(txn.c_weekday_elb_txn_m, 0) c_weekday_elb_txn_m,
            COALESCE(txn.c_weekend_elb_txn_m, 0) c_weekend_elb_txn_m,
            
            COALESCE(txn.c_elb_txn_holder_m, 0) c_elb_txn_holder_m,
            COALESCE(txn.sum_elb_txn_holder_m, 0) sum_elb_txn_holder_m,
            COALESCE(txn.avg_elb_txn_holder_m, 0) avg_elb_txn_holder_m,
            
            (COALESCE(elb_q.c_elb_txn_holder_q, 0) + COALESCE(txn.c_elb_txn_holder_m, 0))/main.c_month_q c_elb_txn_holder_q,
            (COALESCE(elb_q.sum_elb_txn_holder_q, 0) + COALESCE(txn.sum_elb_txn_holder_m, 0))/main.c_month_q sum_elb_txn_holder_q,
            (COALESCE(elb_q.avg_elb_txn_holder_q, 0) + COALESCE(txn.avg_elb_txn_holder_m, 0))/main.c_month_q avg_elb_txn_holder_q,
            
            (COALESCE(elb_y.c_elb_txn_holder_y, 0) + COALESCE(txn.c_elb_txn_holder_m, 0))/main.c_month_y c_elb_txn_holder_y,
            (COALESCE(elb_y.sum_elb_txn_holder_y, 0) + COALESCE(txn.sum_elb_txn_holder_m, 0))/main.c_month_y sum_elb_txn_holder_y,
            (COALESCE(elb_y.avg_elb_txn_holder_y, 0) + COALESCE(txn.avg_elb_txn_holder_m, 0))/main.c_month_y avg_elb_txn_holder_y,
            
            COALESCE(txn.c_noretail_elb_txn_holder_m, 0) c_noretail_elb_txn_holder_m,
            COALESCE(txn.sum_noretail_elb_txn_holder_m, 0) sum_noretail_elb_txn_holder_m,       
            (COALESCE(elb_q.c_noretail_elb_txn_holder_q, 0) + COALESCE(txn.c_noretail_elb_txn_holder_m, 0))/main.c_month_q c_noretail_elb_txn_holder_q,
            (COALESCE(elb_q.sum_noretail_elb_txn_holder_q, 0) + COALESCE(txn.sum_noretail_elb_txn_holder_m, 0))/main.c_month_q sum_noretail_elb_txn_holder_q,       
            (COALESCE(elb_y.c_noretail_elb_txn_holder_y, 0) + COALESCE(txn.c_noretail_elb_txn_holder_m, 0))/main.c_month_y c_noretail_elb_txn_holder_y,
            (COALESCE(elb_y.sum_noretail_elb_txn_holder_y, 0) + COALESCE(txn.sum_noretail_elb_txn_holder_m, 0))/main.c_month_y sum_noretail_elb_txn_holder_y,     
                      
            COALESCE(txn.c_ib_txn_holder_m, 0) c_ib_txn_holder_m,
            COALESCE(txn.c_sb_txn_holder_m, 0) c_sb_txn_holder_m,
            COALESCE(txn.c_bb_txn_holder_m, 0) c_bb_txn_holder_m,
            
            (COALESCE(elb_q.c_ib_txn_holder_q, 0) + COALESCE(txn.c_ib_txn_holder_m, 0))/main.c_month_q c_ib_txn_holder_q,
            (COALESCE(elb_q.c_sb_txn_holder_q, 0) + COALESCE(txn.c_sb_txn_holder_m, 0))/main.c_month_q c_sb_txn_holder_q,
            (COALESCE(elb_q.c_bb_txn_holder_q, 0) + COALESCE(txn.c_bb_txn_holder_m, 0))/main.c_month_q c_bb_txn_holder_q,
            
            (COALESCE(elb_y.c_ib_txn_holder_y, 0) + COALESCE(txn.c_ib_txn_holder_m, 0))/main.c_month_y c_ib_txn_holder_y,
            (COALESCE(elb_y.c_sb_txn_holder_y, 0) + COALESCE(txn.c_sb_txn_holder_m, 0))/main.c_month_y c_sb_txn_holder_y,
            (COALESCE(elb_y.c_bb_txn_holder_y, 0) + COALESCE(txn.c_bb_txn_holder_m, 0))/main.c_month_y c_bb_txn_holder_y,
            ----------------------------------------------------------------------------
            
            COALESCE(logs_owner.recency_log_owner_elb, process_dt - process_dt_hist) recency_log_owner_elb,
            COALESCE(logs_owner.c_log_owner_night_elb_m, 0) c_log_owner_night_elb_m,
            COALESCE(logs_owner.c_log_owner_morning_elb_m, 0) c_log_owner_morning_elb_m,
            COALESCE(logs_owner.c_log_owner_afternoon_elb_m, 0) c_log_owner_afternoon_elb_m,
            COALESCE(logs_owner.c_log_owner_evening_elb_m, 0) c_log_owner_evening_elb_m,
        
            COALESCE(logs_owner.ratio_log_owner_night_elb_m, 0) ratio_log_owner_night_elb_m,
            COALESCE(logs_owner.ratio_log_owner_morning_elb_m, 0) ratio_log_owner_morning_elb_m,
            COALESCE(logs_owner.ratio_log_owner_afternoon_elb_m, 0) ratio_log_owner_afternoon_elb_m,
            COALESCE(logs_owner.ratio_log_owner_evening_elb_m, 0) ratio_log_owner_evening_elb_m,
            
            COALESCE(logs_owner.RATIO_DAYLOG_OWNER_WEEKDAY_ELB_M, 0) RATIO_DAYLOG_OWNER_WEEKDAY_ELB_M,
            COALESCE(logs_owner.RATIO_DAYLOG_OWNER_WEEKEND_ELB_M, 0) RATIO_DAYLOG_OWNER_WEEKEND_ELB_M,
            COALESCE(logs_owner.AVG_OWNER_LOG_OWNER_WEEKDAY_ELB_M, 0) AVG_OWNER_LOG_OWNER_WEEKDAY_ELB_M,
            COALESCE(logs_owner.AVG_OWNER_LOG_OWNER_WEEKEND_ELB_M, 0) AVG_OWNER_LOG_OWNER_WEEKEND_ELB_M,
            
            COALESCE(logs_owner.C_DAYLOG_OWNER_ELB_M, 0) C_DAYLOG_OWNER_ELB_M,
            COALESCE(logs_owner.C_LOG_OWNER_ELB_M, 0) C_LOG_OWNER_ELB_M,   
            (COALESCE(elb_q.C_DAYLOG_OWNER_ELB_Q, 0) + COALESCE(logs_owner.C_DAYLOG_OWNER_ELB_M, 0))/main.c_month_q C_DAYLOG_OWNER_ELB_Q,    
            (COALESCE(elb_q.C_LOG_OWNER_ELB_Q, 0) + COALESCE(logs_owner.C_LOG_OWNER_ELB_M, 0))/main.c_month_q C_LOG_OWNER_ELB_Q,     
            (COALESCE(elb_y.C_DAYLOG_OWNER_ELB_Y, 0) + COALESCE(logs_owner.C_DAYLOG_OWNER_ELB_M, 0))/main.c_month_y C_DAYLOG_OWNER_ELB_Y,    
            (COALESCE(elb_y.C_LOG_OWNER_ELB_Y, 0) + COALESCE(logs_owner.C_LOG_OWNER_ELB_M, 0))/main.c_month_y C_LOG_OWNER_ELB_Y,   
        
            COALESCE(txn_owner.c_elb_txn_owner_m, 0) c_elb_txn_owner_m,
            COALESCE(txn_owner.sum_elb_txn_owner_m, 0) sum_elb_txn_owner_m,
            COALESCE(txn_owner.avg_elb_txn_owner_m, 0) avg_elb_txn_owner_m,
            
            (COALESCE(elb_q.c_elb_txn_owner_q, 0) + COALESCE(txn_owner.c_elb_txn_owner_m, 0))/main.c_month_q c_elb_txn_owner_q,
            (COALESCE(elb_q.sum_elb_txn_owner_q, 0) + COALESCE(txn_owner.sum_elb_txn_owner_m, 0))/main.c_month_q sum_elb_txn_owner_q,
            (COALESCE(elb_q.avg_elb_txn_owner_q, 0) + COALESCE(txn_owner.avg_elb_txn_owner_m, 0))/main.c_month_q avg_elb_txn_owner_q,
            
            (COALESCE(elb_y.c_elb_txn_owner_y, 0) + COALESCE(txn_owner.c_elb_txn_owner_m, 0))/main.c_month_y c_elb_txn_owner_y,
            (COALESCE(elb_y.sum_elb_txn_owner_y, 0) + COALESCE(txn_owner.sum_elb_txn_owner_m, 0))/main.c_month_y sum_elb_txn_owner_y,
            (COALESCE(elb_y.avg_elb_txn_owner_y, 0) + COALESCE(txn_owner.avg_elb_txn_owner_m, 0))/main.c_month_y avg_elb_txn_owner_y,
            
            COALESCE(txn_owner.c_elb_foreign_txn_owner_m, 0) c_elb_foreign_txn_owner_m,
            COALESCE(txn_owner.sum_elb_foreign_txn_owner_m, 0) sum_elb_foreign_txn_owner_m,
            COALESCE(txn_owner.c_elb_foreign_curr_txn_owner_m, 0) c_elb_dforeign_curr_txn_owner_m,
            COALESCE(txn_owner.sum_elb_foreign_curr_txn_owner_m, 0) sum_elb_foreign_curr_txn_owner_m,
            COALESCE(txn_owner.i_extra_mtg_elb_txn_owner_m, 0) i_extra_mtg_elb_txn_owner_m,
            GREATEST(COALESCE(elb_q.i_extra_mtg_elb_txn_owner_q, 0), COALESCE(txn_owner.i_extra_mtg_elb_txn_owner_m, 0)) i_extra_mtg_elb_txn_owner_q,
            GREATEST(COALESCE(elb_y.i_extra_mtg_elb_txn_owner_y, 0), COALESCE(txn_owner.i_extra_mtg_elb_txn_owner_m, 0)) i_extra_mtg_elb_txn_owner_y, 
            COALESCE(txn_owner.sum_extra_mtg_elb_txn_owner_m, 0) sum_extra_mtg_elb_txn_owner_m,
            GREATEST(COALESCE(elb_q.sum_extra_mtg_elb_txn_owner_q, 0), COALESCE(txn_owner.sum_extra_mtg_elb_txn_owner_m, 0)) sum_extra_mtg_elb_txn_owner_q,
            GREATEST(COALESCE(elb_y.sum_extra_mtg_elb_txn_owner_y, 0), COALESCE(txn_owner.sum_extra_mtg_elb_txn_owner_m, 0)) sum_extra_mtg_elb_txn_owner_y, 
        
            COALESCE(txn_owner.i_bulk_elb_txn_owner_m, 0) i_bulk_elb_txn_owner_m,
            COALESCE(txn_owner.sum_bulk_elb_txn_owner_m, 0) sum_bulk_elb_txn_owner_m,
            (COALESCE(elb_q.i_bulk_elb_txn_owner_q, 0) + COALESCE(txn_owner.i_bulk_elb_txn_owner_m, 0))/main.c_month_q i_bulk_elb_txn_owner_q,
            (COALESCE(elb_q.sum_bulk_elb_txn_owner_q, 0) + COALESCE(txn_owner.sum_bulk_elb_txn_owner_m, 0))/main.c_month_q sum_bulk_elb_txn_owner_q,
            (COALESCE(elb_y.i_bulk_elb_txn_owner_y, 0) + COALESCE(txn_owner.i_bulk_elb_txn_owner_m, 0))/main.c_month_y i_bulk_elb_txn_owner_y,
            (COALESCE(elb_y.sum_bulk_elb_txn_owner_y, 0) + COALESCE(txn_owner.sum_bulk_elb_txn_owner_m, 0))/main.c_month_y sum_bulk_elb_txn_owner_y,
        --
            COALESCE(txn_owner.c_ib_txn_owner_m, 0) c_ib_txn_owner_m,
            COALESCE(txn_owner.c_sb_txn_owner_m, 0) c_sb_txn_owner_m,
            COALESCE(txn_owner.c_bb_txn_owner_m, 0) c_bb_txn_owner_m,
            
            COALESCE(txn_owner.sum_ib_txn_owner_m, 0) sum_ib_txn_owner_m,
            COALESCE(txn_owner.sum_sb_txn_owner_m, 0) sum_sb_txn_owner_m,
            COALESCE(txn_owner.sum_bb_txn_owner_m, 0) sum_bb_txn_owner_m,
            
            (COALESCE(elb_q.sum_ib_txn_owner_q, 0) + COALESCE(txn_owner.c_ib_txn_owner_m, 0))/main.c_month_q sum_ib_txn_owner_q,
            (COALESCE(elb_q.sum_sb_txn_owner_q, 0) + COALESCE(txn_owner.c_sb_txn_owner_m, 0))/main.c_month_q sum_sb_txn_owner_q,
            (COALESCE(elb_q.sum_bb_txn_owner_q, 0) + COALESCE(txn_owner.c_bb_txn_owner_m, 0))/main.c_month_q sum_bb_txn_owner_q,
            
            (COALESCE(elb_y.sum_ib_txn_owner_y, 0) + COALESCE(txn_owner.c_ib_txn_owner_m, 0))/main.c_month_y sum_ib_txn_owner_y,
            (COALESCE(elb_y.sum_sb_txn_owner_y, 0) + COALESCE(txn_owner.c_sb_txn_owner_m, 0))/main.c_month_y sum_sb_txn_owner_y,
            (COALESCE(elb_y.sum_bb_txn_owner_y, 0) + COALESCE(txn_owner.c_bb_txn_owner_m, 0))/main.c_month_y sum_bb_txn_owner_y,
            
            COALESCE(txn_owner.c_night_elb_txn_owner_m, 0) c_night_elb_txn_owner_m,
            COALESCE(txn_owner.c_morning_elb_txn_owner_m, 0) c_morning_elb_txn_owner_m,
            COALESCE(txn_owner.c_afternoon_elb_txn_owner_m, 0) c_afternoon_elb_txn_owner_m,
            COALESCE(txn_owner.c_evening_elb_txn_owner_m, 0) c_evening_elb_txn_owner_m,
            
            COALESCE(txn_owner.c_weekday_elb_txn_owner_m, 0) c_weekday_elb_txn_owner_m,
            COALESCE(txn_owner.c_weekend_elb_txn_owner_m, 0) c_weekend_elb_txn_owner_m
                    
        FROM (
            SELECT
               base.month_id,
               base.customerid,
               base.bank_tenure,
               LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -3)),1)*3,
                     COALESCE(elb_q.c_month, 0)+1) c_month_q,
               LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -12)),1)*12,
                     COALESCE(elb_y.c_month, 0)+1) c_month_y       
            FROM pm_owner.attr_client_base base
            LEFT JOIN (
               SELECT
                   customerid,
                   COUNT(DISTINCT month_id) c_month
               FROM pm_owner.attr_elb
               WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-2), 'YYYYMM'))
               AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
               GROUP BY customerid) elb_q
               ON elb_q.customerid = base.customerid
           
            LEFT JOIN (
               SELECT
                   customerid,
                   COUNT(DISTINCT month_id) c_month
               FROM pm_owner.attr_elb
               WHERE month_id >= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-11), 'YYYYMM'))
               AND month_id <= TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
               GROUP BY customerid) elb_y
               ON elb_y.customerid = base.customerid                   
            WHERE base.month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))) main
        
        LEFT JOIN (
            SELECT
               ippid.customerid_account customerid,
               (process_dt - GREATEST(MAX(ippid.ippid_open_dt), process_dt_hist)) recency_open_ippid,
               (process_dt - GREATEST(MAX(CASE WHEN ippid.i_authorizeduser_elb = 1 THEN ippid.ippid_open_dt ELSE process_dt_hist END),
                                            process_dt_hist)) recency_open_authorized_ippid,
               COUNT(DISTINCT CASE WHEN ippid.i_active_ippid = 1 THEN ippid.ippid ELSE NULL END) c_active_ippid,
               COUNT(DISTINCT CASE WHEN ippid.i_closed_ippid = 1 THEN ippid.ippid ELSE NULL END) c_closed_ippid,
               COUNT(DISTINCT CASE WHEN ippid.i_blocked_ippid = 1 THEN ippid.ippid ELSE NULL END) c_blocked_ippid,  
            
               COUNT(DISTINCT ippid.ippid) c_total_ippid,
               COUNT(DISTINCT ippid.customerid_ippid) c_client_elb_authorized,
               
               MAX(CASE WHEN ippid.i_active_ippid = 1 AND ippid.i_authorizeduser_elb = 0 THEN 1 ELSE 0 END) i_active_owner_ippid,
               MAX(CASE WHEN ippid.i_active_ippid = 1 AND ippid.i_authorizeduser_elb = 1 THEN 1 ELSE 0 END) i_active_authorized_ippid,
               
               MAX(CASE WHEN ippid.limit_per_txn_ib > 0 AND ippid.limit_per_txn_ib < 99999999 THEN ippid.limit_per_txn_ib
                        ELSE LEAST(CASE WHEN ippid.daily_limit_ib > 0 THEN ippid.daily_limit_ib ELSE 34000 END, 34000) END) max_limit_txn_ib,
               MIN(CASE WHEN ippid.limit_per_txn_ib > 0 AND ippid.limit_per_txn_ib < 99999999 THEN ippid.limit_per_txn_ib
                        ELSE LEAST(CASE WHEN ippid.daily_limit_ib > 0 THEN ippid.daily_limit_ib ELSE 34000 END, 34000) END) min_limit_txn_ib,
            
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_sms_authentification_elb ELSE 0 END)
               + MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_pin_authentification_elb ELSE 0 END)
               + MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_token_authentification_elb ELSE 0 END)
               + MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_mtoken_authentification_elb ELSE 0 END) c_authent_type_elb,   
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_sms_authentification_elb ELSE 0 END) i_sms_authent_elb,
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_pin_authentification_elb ELSE 0 END) i_pin_authent_elb,
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_token_authentification_elb ELSE 0 END) i_token_authent_elb,
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_mtoken_authentification_elb ELSE 0 END) i_mtoken_authent_elb,
               
               MAX(CASE WHEN (ippid.ippid_open_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) i_opened_ippid_m 
                   
            FROM pm_owner.stg_elb_ippid ippid
            WHERE ippid.customerid_account IS NOT NULL
            
            GROUP BY ippid.customerid_account) ippid
            ON ippid.customerid = main.customerid
        
        LEFT JOIN (
            SELECT
               ippid.customerid_ippid customerid,
               (process_dt - GREATEST(MAX(ippid.ippid_open_dt), process_dt_hist)) recency_open_ippid_holder,
            
               COUNT(DISTINCT ippid.ippid) c_total_ippid_holder,
               COUNT(DISTINCT ippid.customerid_account) c_access_elb_holder,
               
               MAX(DISTINCT CASE WHEN ippid.i_active_ippid = 1 THEN 1 ELSE 0 END) i_active_ippid_holder,
               COUNT(DISTINCT CASE WHEN ippid.I_RETAIL_ACCOUNT = 1 THEN ippid.ippid ELSE NULL END) c_active_ret_ippid_holder,
               COUNT(DISTINCT CASE WHEN ippid.I_NONRETAIL_ACCOUNT = 1 THEN ippid.ippid ELSE NULL END) c_active_noret_ippid_holder,
               
               MAX(CASE WHEN ippid.limit_per_txn_ib > 0 AND ippid.limit_per_txn_ib < 99999999 THEN ippid.limit_per_txn_ib
                        ELSE LEAST(CASE WHEN ippid.daily_limit_ib > 0 THEN ippid.daily_limit_ib ELSE 34000 END, 34000) END) max_limit_txn_ib_holder,
               MIN(CASE WHEN ippid.limit_per_txn_ib > 0 AND ippid.limit_per_txn_ib < 99999999 THEN ippid.limit_per_txn_ib
                        ELSE LEAST(CASE WHEN ippid.daily_limit_ib > 0 THEN ippid.daily_limit_ib ELSE 34000 END, 34000) END) min_limit_txn_ib_holder,
            
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_sms_authentification_elb ELSE 0 END)
               + MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_pin_authentification_elb ELSE 0 END)
               + MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_token_authentification_elb ELSE 0 END)
               + MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_mtoken_authentification_elb ELSE 0 END) c_authent_type_elb_holder,
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_sms_authentification_elb ELSE 0 END) i_sms_authent_elb_holder,
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_pin_authentification_elb ELSE 0 END) i_pin_authent_elb_holder,
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_token_authentification_elb ELSE 0 END) i_token_authent_elb_holder,
               MAX(CASE WHEN ippid.i_active_ippid = 1 THEN ippid.i_mtoken_authentification_elb ELSE 0 END) i_mtoken_authent_elb_holder,
               
               MAX(CASE WHEN (ippid.ippid_open_dt BETWEEN ADD_MONTHS(process_dt, -1)+1 AND process_dt) THEN 1 ELSE 0 END) i_opened_ippid_holder_m
                   
            FROM pm_owner.stg_elb_ippid ippid
            WHERE 1=1
            AND ippid.customerid_ippid != ippid.customerid_account
            
            GROUP BY ippid.customerid_ippid) ippid_holder
            ON ippid_holder.customerid = main.customerid
        
        LEFT JOIN (
            SELECT
               ippid.customerid_ippid customerid,
               COUNT(DISTINCT ippid.cont_id) c_product_access_ippid,
               COUNT(DISTINCT CASE WHEN lvl2_category = 1100 THEN ippid.cont_id ELSE NULL END) c_dda_access_ippid,
               COUNT(DISTINCT CASE WHEN lvl2_category = 1400 THEN ippid.cont_id ELSE NULL END) c_savings_access_ippid,
               COUNT(DISTINCT CASE WHEN lvl2_category = 1500 THEN ippid.cont_id ELSE NULL END) c_loans_access_ippid,
               COUNT(DISTINCT CASE WHEN ippid.customerid_ippid != ippid.customerid_owner
                                   THEN ippid.cont_id ELSE NULL END) c_product_access_ippid_holder,
               COUNT(DISTINCT CASE WHEN lvl2_category = 1100 AND ippid.customerid_ippid != ippid.customerid_owner
                                   THEN ippid.cont_id ELSE NULL END) c_dda_access_ippid_holder,
               COUNT(DISTINCT CASE WHEN lvl2_category = 1400 AND ippid.customerid_ippid != ippid.customerid_owner
                                    THEN ippid.cont_id ELSE NULL END) c_savings_access_ippid_holder,
               COUNT(DISTINCT CASE WHEN lvl2_category = 1500 AND ippid.customerid_ippid != ippid.customerid_owner
                                    THEN ippid.cont_id ELSE NULL END) c_loans_access_ippid_holder
            FROM (
               SELECT elb_base.*, plc.lvl2_category FROM pm_owner.stg_elb_base elb_base
               LEFT JOIN pm_owner.product_level_category plc
               ON elb_base.prod_src_val = plc.prod_src_val
               AND elb_base.src_syst_cd = plc.src_syst_cd
               WHERE elb_base.prod_src_val is not null
               AND elb_base.i_active_ippid = 1) ippid
            GROUP BY ippid.customerid_ippid) ippid_prod
            ON ippid_prod.customerid = main.customerid
        
        LEFT JOIN (
            SELECT
               customerid customerid,
               c_day_last_log_elb recency_log_elb_m,
               c_log_last_day_elb c_log_last_day_elb_m,
               c_daylog_last_week_elb c_daylog_last_week_elb_m,
               c_log_last_week_elb c_log_last_week_elb_m,
            
               c_log_night_elb c_log_night_elb_m,
               c_log_morning_elb c_log_morning_elb_m,
               c_log_afternoon_elb c_log_afternoon_elb_m,
               c_log_evening_elb c_log_evening_elb_m,
               
               c_log_night_elb/GREATEST(c_log_night_elb + c_log_morning_elb + c_log_afternoon_elb + c_log_evening_elb, 1) ratio_log_night_elb_m,
               c_log_morning_elb/GREATEST(c_log_night_elb + c_log_morning_elb + c_log_afternoon_elb + c_log_evening_elb, 1) ratio_log_morning_elb_m,
               c_log_afternoon_elb/GREATEST(c_log_night_elb + c_log_morning_elb + c_log_afternoon_elb + c_log_evening_elb, 1) ratio_log_afternoon_elb_m,
               c_log_evening_elb/GREATEST(c_log_night_elb + c_log_morning_elb + c_log_afternoon_elb + c_log_evening_elb, 1) ratio_log_evening_elb_m,
            
               C_DAYLOG_WEEKDAY_ELB/
                             (select count(trunc(process_dt,'MM') +(level-1)) from dual
                              where to_char(trunc(process_dt,'MM') +(level-1),'D') not in ('6', '7')
                              connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) RATIO_DAYLOG_WEEKDAY_ELB_M,
               C_DAYLOG_WEEKEND_ELB/
                             (select count(trunc(process_dt,'MM') +(level-1)) from dual
                              where to_char(trunc(process_dt,'MM') +(level-1),'D') in ('6', '7')
                              connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) RATIO_DAYLOG_WEEKEND_ELB_M,
               C_LOG_WEEKDAY_ELB/
                             (select count(trunc(process_dt,'MM') +(level-1)) from dual
                              where to_char(trunc(process_dt,'MM') +(level-1),'D') not in ('6', '7')
                              connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) AVG_LOG_WEEKDAY_ELB_M,
               C_LOG_WEEKEND_ELB/
                             (select count(trunc(process_dt,'MM') +(level-1)) from dual
                              where to_char(trunc(process_dt,'MM') +(level-1),'D') in ('6', '7')
                              connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) AVG_LOG_WEEKEND_ELB_M,
               C_DAYLOG_ELB C_DAYLOG_ELB_M,
               C_LOG_ELB C_LOG_ELB_M,
               c_daylog_elb_holder c_daylog_elb_holder_m,
               c_log_elb_holder c_log_elb_holder_m,
               c_daylog_noretail_elb_holder c_daylog_noretail_elb_holder_m,
               c_log_noretail_elb_holder c_log_noretail_elb_holder_m,
          
               -- zmena v logoch, narast/pokles
               C_DAYLOG_IB C_DAYLOG_IB_M,
               C_LOG_IB C_LOG_IB_M,
               C_DAYLOG_SB C_DAYLOG_SB_M,
               C_LOG_SB C_LOG_SB_M,
               C_DAYLOG_BB C_DAYLOG_BB_M,
               C_LOG_BB C_LOG_BB_M
                   
            FROM PM_OWNER.TMP_ELB_LOG) logs
            ON logs.customerid = main.customerid
        
        LEFT JOIN PM_OWNER.TMP_ELB_TXN txn
            ON txn.customerid = main.customerid
        
        -- cez elb_ownera
        LEFT JOIN (
            SELECT
               elog.customerid customerid,
               elog.c_day_last_log_owner_elb recency_log_owner_elb,
            
               elog.c_log_owner_night_elb c_log_owner_night_elb_m,
               elog.c_log_owner_morning_elb c_log_owner_morning_elb_m,
               elog.c_log_owner_afternoon_elb c_log_owner_afternoon_elb_m,
               elog.c_log_owner_evening_elb c_log_owner_evening_elb_m,
               
               elog.c_log_owner_night_elb/GREATEST(elog.c_log_owner_night_elb + elog.c_log_owner_morning_elb + elog.c_log_owner_afternoon_elb + elog.c_log_owner_evening_elb, 1) ratio_log_owner_night_elb_m,
               elog.c_log_owner_morning_elb/GREATEST(elog.c_log_owner_night_elb + elog.c_log_owner_morning_elb + elog.c_log_owner_afternoon_elb + elog.c_log_owner_evening_elb, 1) ratio_log_owner_morning_elb_m,
               elog.c_log_owner_afternoon_elb/GREATEST(elog.c_log_owner_night_elb + elog.c_log_owner_morning_elb + elog.c_log_owner_afternoon_elb + elog.c_log_owner_evening_elb, 1) ratio_log_owner_afternoon_elb_m,
               elog.c_log_owner_evening_elb/GREATEST(elog.c_log_owner_night_elb + elog.c_log_owner_morning_elb + elog.c_log_owner_afternoon_elb + elog.c_log_owner_evening_elb, 1) ratio_log_owner_evening_elb_m,
            
               elog.C_DAYLOg_owner_WEEKDAY_ELB/
                             (select count(trunc(process_dt,'MM') +(level-1)) from dual
                              where to_char(trunc(process_dt,'MM') +(level-1),'D') not in ('6', '7')
                              connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) RATIO_DAYLOg_owner_WEEKDAY_ELB_M,
               elog.C_DAYLOg_owner_WEEKEND_ELB/
                             (select count(trunc(process_dt,'MM') +(level-1)) from dual
                              where to_char(trunc(process_dt,'MM') +(level-1),'D') in ('6', '7')
                              connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) RATIO_DAYLOg_owner_WEEKEND_ELB_M,
               elog.C_LOg_owner_WEEKDAY_ELB/
                             (select count(trunc(process_dt,'MM') +(level-1)) from dual
                              where to_char(trunc(process_dt,'MM') +(level-1),'D') not in ('6', '7')
                              connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) AVg_owner_LOg_owner_WEEKDAY_ELB_M,
               elog.C_LOg_owner_WEEKEND_ELB/
                             (select count(trunc(process_dt,'MM') +(level-1)) from dual
                              where to_char(trunc(process_dt,'MM') +(level-1),'D') in ('6', '7')
                              connect by level <= last_day(process_dt) - trunc(process_dt,'MM') + 1) AVg_owner_LOg_owner_WEEKEND_ELB_M,
               elog.C_DAYLOG_OWNER_ELB C_DAYLOG_OWNER_ELB_M,
               elog.C_LOG_OWNER_ELB C_LOG_OWNER_ELB_M
                   
            FROM PM_OWNER.TMP_ELB_LOG_OWNER elog) logs_owner
            ON logs_owner.customerid = main.customerid
        
        LEFT JOIN PM_OWNER.TMP_ELB_TXN_OWNER txn_owner
            ON txn_owner.customerid = main.customerid
              
        LEFT JOIN (
            SELECT
               customerid,
               COUNT(DISTINCT month_id) + 1 c_month_id,  
               MAX(i_opened_ippid_m) i_opened_ippid_q,    
               MAX(i_opened_ippid_holder_m) i_opened_ippid_holder_q,
               
               SUM(c_daylog_elb_m) c_daylog_elb_q,
               SUM(c_log_elb_m) c_log_elb_q,
               SUM(c_daylog_elb_holder_m) c_daylog_elb_holder_q,
               SUM(c_log_elb_holder_m) c_log_elb_holder_q,
               SUM(c_daylog_noretail_elb_holder_m) c_daylog_noretail_elb_holder_q,
               SUM(c_log_noretail_elb_holder_m) c_log_noretail_elb_holder_q,
               
               SUM(c_daylog_ib_m) c_daylog_ib_q,
               SUM(c_log_ib_m) c_log_ib_q,
               SUM(c_daylog_sb_m) c_daylog_sb_q,
               SUM(c_log_sb_m) c_log_sb_q,
               SUM(c_daylog_bb_m) c_daylog_bb_q,
               SUM(c_log_bb_m) c_log_bb_q,
               
               SUM(c_elb_txn_m) c_elb_txn_q,
               SUM(sum_elb_txn_m) sum_elb_txn_q,
               SUM(avg_elb_txn_m) avg_elb_txn_q,    
            
               MAX(i_extra_mtg_elb_txn_m) i_extra_mtg_elb_txn_q,
               SUM(sum_extra_mtg_elb_txn_m) sum_extra_mtg_elb_txn_q,
               
               MAX(i_created_stand_order_elb_m) i_created_stand_order_elb_q,
               MAX(i_changed_stand_order_elb_m) i_changed_stand_order_elb_q,
              
               MAX(I_PAY_OFF_CC_ELB_M) I_PAY_OFF_CC_ELB_Q,
               SUM(SUM_PAY_OFF_CC_ELB_M) SUM_PAY_OFF_CC_ELB_Q,
               
               MAX(i_bulk_elb_txn_m) i_bulk_elb_txn_q,
               SUM(sum_bulk_elb_txn_m) sum_bulk_elb_txn_q,
            
               SUM(sum_ib_txn_m) sum_ib_txn_q,
               SUM(sum_sb_txn_m) sum_sb_txn_q,
               SUM(sum_bb_txn_m) sum_bb_txn_q,
               
               SUM(c_elb_txn_holder_m) c_elb_txn_holder_q,
               SUM(sum_elb_txn_holder_m) sum_elb_txn_holder_q,
               SUM(avg_elb_txn_holder_m) avg_elb_txn_holder_q,
        
               SUM(c_noretail_elb_txn_holder_m) c_noretail_elb_txn_holder_q,
               SUM(sum_noretail_elb_txn_holder_m) sum_noretail_elb_txn_holder_q,
               
               SUM(c_ib_txn_holder_m) c_ib_txn_holder_q,
               SUM(c_sb_txn_holder_m) c_sb_txn_holder_q,
               SUM(c_bb_txn_holder_m) c_bb_txn_holder_q,
               
               SUM(C_DAYLOG_OWNER_ELB_M) C_DAYLOG_OWNER_ELB_Q,
               SUM(C_LOG_OWNER_ELB_M) C_LOG_OWNER_ELB_Q,
               
               SUM(c_elb_txn_owner_m) c_elb_txn_owner_q,
               SUM(sum_elb_txn_owner_m) sum_elb_txn_owner_q,
               SUM(avg_elb_txn_owner_m) avg_elb_txn_owner_q,
               
               SUM(c_elb_foreign_txn_owner_m) c_elb_foreign_txn_owner_q,
               SUM(sum_elb_foreign_txn_owner_m) sum_elb_foreign_txn_owner_q,
               SUM(c_elb_foreign_curr_txn_owner_m) c_elb_foreign_curr_txn_owner_q,
               SUM(sum_elb_foreign_curr_txn_owner_m) sum_elb_foreign_curr_txn_owner_q,
               MAX(i_extra_mtg_elb_txn_owner_m) i_extra_mtg_elb_txn_owner_q,
               SUM(sum_extra_mtg_elb_txn_owner_m) sum_extra_mtg_elb_txn_owner_q,
               
               MAX(i_bulk_elb_txn_owner_m) i_bulk_elb_txn_owner_q,
               SUM(sum_bulk_elb_txn_owner_m) sum_bulk_elb_txn_owner_q,
               
               SUM(c_ib_txn_owner_m) c_ib_txn_owner_q,
               SUM(c_sb_txn_owner_m) c_sb_txn_owner_q,
               SUM(c_bb_txn_owner_m) c_bb_txn_owner_q,
            
               SUM(sum_ib_txn_owner_m) sum_ib_txn_owner_q,
               SUM(sum_sb_txn_owner_m) sum_sb_txn_owner_q,
               SUM(sum_bb_txn_owner_m) sum_bb_txn_owner_q,
               
               SUM(c_weekday_elb_txn_owner_m) c_weekday_elb_txn_owner_q,
               SUM(c_weekend_elb_txn_owner_m) c_weekend_elb_txn_owner_q        
            
            FROM pm_owner.attr_elb
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -2), 'YYYYMM'))
                          AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) elb_q
            ON elb_q.customerid = main.customerid
        
        LEFT JOIN (
            SELECT
               customerid,
               COUNT(DISTINCT month_id) + 1 c_month_id,  
               MAX(i_opened_ippid_m) i_opened_ippid_y,    
               MAX(i_opened_ippid_holder_m) i_opened_ippid_holder_y,
               
               SUM(c_daylog_elb_m) c_daylog_elb_y,
               SUM(c_log_elb_m) c_log_elb_y,
               SUM(c_daylog_elb_holder_m) c_daylog_elb_holder_y,
               SUM(c_log_elb_holder_m) c_log_elb_holder_y,
               SUM(c_daylog_noretail_elb_holder_m) c_daylog_noretail_elb_holder_y,
               SUM(c_log_noretail_elb_holder_m) c_log_noretail_elb_holder_y,
               
               SUM(c_daylog_ib_m) c_daylog_ib_y,
               SUM(c_log_ib_m) c_log_ib_y,
               SUM(c_daylog_sb_m) c_daylog_sb_y,
               SUM(c_log_sb_m) c_log_sb_y,
               SUM(c_daylog_bb_m) c_daylog_bb_y,
               SUM(c_log_bb_m) c_log_bb_y,
               
               SUM(c_elb_txn_m) c_elb_txn_y,
               SUM(sum_elb_txn_m) sum_elb_txn_y,
               SUM(avg_elb_txn_m) avg_elb_txn_y,    
            
               MAX(i_extra_mtg_elb_txn_m) i_extra_mtg_elb_txn_y,
               SUM(sum_extra_mtg_elb_txn_m) sum_extra_mtg_elb_txn_y,
               
               MAX(i_created_stand_order_elb_m) i_created_stand_order_elb_y,
               MAX(i_changed_stand_order_elb_m) i_changed_stand_order_elb_y,
              
               MAX(I_PAY_OFF_CC_ELB_M) I_PAY_OFF_CC_ELB_Y,
               SUM(SUM_PAY_OFF_CC_ELB_M) SUM_PAY_OFF_CC_ELB_Y,
               
               MAX(i_bulk_elb_txn_m) i_bulk_elb_txn_y,
               SUM(sum_bulk_elb_txn_m) sum_bulk_elb_txn_y,
            
               SUM(sum_ib_txn_m) sum_ib_txn_y,
               SUM(sum_sb_txn_m) sum_sb_txn_y,
               SUM(sum_bb_txn_m) sum_bb_txn_y,
               
               SUM(c_elb_txn_holder_m) c_elb_txn_holder_y,
               SUM(sum_elb_txn_holder_m) sum_elb_txn_holder_y,
               SUM(avg_elb_txn_holder_m) avg_elb_txn_holder_y,
               
               SUM(c_noretail_elb_txn_holder_m) c_noretail_elb_txn_holder_y,
               SUM(sum_noretail_elb_txn_holder_m) sum_noretail_elb_txn_holder_y,
               
               SUM(c_ib_txn_holder_m) c_ib_txn_holder_y,
               SUM(c_sb_txn_holder_m) c_sb_txn_holder_y,
               SUM(c_bb_txn_holder_m) c_bb_txn_holder_y,
               
               SUM(C_DAYLOG_OWNER_ELB_M) C_DAYLOG_OWNER_ELB_y,
               SUM(C_LOG_OWNER_ELB_M) C_LOG_OWNER_ELB_y,
               
               SUM(c_elb_txn_owner_m) c_elb_txn_owner_y,
               SUM(sum_elb_txn_owner_m) sum_elb_txn_owner_y,
               SUM(avg_elb_txn_owner_m) avg_elb_txn_owner_y,
               
               SUM(c_elb_foreign_txn_owner_m) c_elb_foreign_txn_owner_y,
               SUM(sum_elb_foreign_txn_owner_m) sum_elb_foreign_txn_owner_y,
               SUM(c_elb_foreign_curr_txn_owner_m) c_elb_foreign_curr_txn_owner_y,
               SUM(sum_elb_foreign_curr_txn_owner_m) sum_elb_foreign_curr_txn_owner_y,
               MAX(i_extra_mtg_elb_txn_owner_m) i_extra_mtg_elb_txn_owner_y,
               SUM(sum_extra_mtg_elb_txn_owner_m) sum_extra_mtg_elb_txn_owner_y,
               
               MAX(i_bulk_elb_txn_owner_m) i_bulk_elb_txn_owner_y,
               SUM(sum_bulk_elb_txn_owner_m) sum_bulk_elb_txn_owner_y,
               
               SUM(c_ib_txn_owner_m) c_ib_txn_owner_y,
               SUM(c_sb_txn_owner_m) c_sb_txn_owner_y,
               SUM(c_bb_txn_owner_m) c_bb_txn_owner_y,
            
               SUM(sum_ib_txn_owner_m) sum_ib_txn_owner_y,
               SUM(sum_sb_txn_owner_m) sum_sb_txn_owner_y,
               SUM(sum_bb_txn_owner_m) sum_bb_txn_owner_y,
               
               SUM(c_weekday_elb_txn_owner_m) c_weekday_elb_txn_owner_y,
               SUM(c_weekend_elb_txn_owner_m) c_weekend_elb_txn_owner_y
            
            FROM pm_owner.attr_elb
            WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -11), 'YYYYMM'))
                          AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            GROUP BY customerid) elb_y
            ON elb_y.customerid = main.customerid;
	           
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);

	ETL_OWNER.etl_dbms_util.truncate_table('TMP_ELB_LOG',l_owner_name);
	ETL_OWNER.etl_dbms_util.truncate_table('TMP_ELB_LOG_OWNER',l_owner_name);
	ETL_OWNER.etl_dbms_util.truncate_table('TMP_ELB_TXN',l_owner_name);
	ETL_OWNER.etl_dbms_util.truncate_table('TMP_ELB_TXN_OWNER',l_owner_name);
  END;
  
  
    PROCEDURE append_ATTR_FUND(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_ATTR_FUND';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_FUND';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);
    
  BEGIN

    -- ATTR_FUND
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);
    
      INSERT INTO pm_owner.ATTR_FUND
      (
        select 
        
        a.month_id,
        a.customerid,
        
        a.I_ACTIVECAPITOL_SK as I_ACTIVECAPITOL_SK,
        
        a.I_NEW_FUND as I_NEW_FUND,
        a.I_REGULAR_FUND as I_REGULAR_FUND,
        case 
            when a.BALANCE_FUND = 0 and a.C_ISIN_TOTAL_FUND = 0 
            and coalesce(prev_val.BALANCE_FUND, 0) > 0 then 1 else 0 end as I_CLOSED_FUND,
        case when a.C_REGULAR_FUND > coalesce(prev_val.C_REGULAR_FUND, 0) then 1 else 0 end as I_NEW_REGULAR_FUND_M,
        
        GREATEST(coalesce(term_q.I_NEW_FUND_Q, 0), a.I_NEW_FUND) as I_NEW_FUND_Q,
        GREATEST(coalesce(term_y.I_NEW_FUND_Y, 0), a.I_NEW_FUND) as I_NEW_FUND_Y,
        
        GREATEST(coalesce(term_q.I_REGULAR_FUND_Q, 0), a.I_REGULAR_FUND) as I_REGULAR_FUND_Q,
        GREATEST(coalesce(term_y.I_REGULAR_FUND_Y, 0), a.I_REGULAR_FUND) as I_REGULAR_FUND_Y,
        
        GREATEST(coalesce(term_q.I_NEW_REGULAR_FUND_Q, 0),
                 case when a.C_REGULAR_FUND > coalesce(prev_val.C_REGULAR_FUND, 0) then 1 else 0 end) as I_NEW_REGULAR_FUND_Q,
        --max(term_y.I_NEW_REGULAR_FUND_Y) as I_NEW_REGULAR_FUND_Y,
        GREATEST(coalesce(term_y.I_NEW_REGULAR_FUND_Y, 0),
                 case when a.C_REGULAR_FUND > coalesce(prev_val.C_REGULAR_FUND, 0) then 1 else 0 end) as I_NEW_REGULAR_FUND_Y,
        
        GREATEST(coalesce(term_q.I_CLOSED_FUND_Q, 0),
                (case when a.BALANCE_FUND = 0 and a.C_ISIN_TOTAL_FUND = 0 
                  and coalesce(prev_val.BALANCE_FUND, 0) > 0 then 1 else 0 end)) as I_CLOSED_FUND_Q,
        GREATEST(coalesce(term_y.I_CLOSED_FUND_Y, 0),
                (case when a.BALANCE_FUND = 0 and a.C_ISIN_TOTAL_FUND = 0 and
                 coalesce(prev_val.BALANCE_FUND, 0) > 0 then 1 else 0 end)) as I_CLOSED_FUND_Y,
        
        
        a.RECENCY_NEW_REGULAR_FUND as RECENCY_NEW_REGULAR_FUND,
        a.RECENCY_INVESTED_FUND as RECENCY_INVESTED_FUND,
        a.RECENCY_SOLD_FUND as RECENCY_SOLD_FUND,
        
        a.SUM_INVESTED_FUND as SUM_INVESTED_FUND_M,
        (coalesce(term_q.SUM_INVESTED_FUND_Q, 0) + a.SUM_INVESTED_FUND) / (coalesce(term_q.C_MONTH_ID, 0) + 1) as AVG_INVESTED_FUND_Q,
        (coalesce(term_y.SUM_INVESTED_FUND_Y, 0) + a.SUM_INVESTED_FUND) / (coalesce(term_y.C_MONTH_ID, 0) + 1) as AVG_INVESTED_FUND_Y,
        
        a.SUM_SOLD_FUND as SUM_SOLD_FUND_M,
        (coalesce(term_q.SUM_SOLD_FUND_Q, 0) + a.SUM_SOLD_FUND) / (coalesce(term_q.C_MONTH_ID, 0) + 1) as AVG_SOLD_FUND_Q,
        (coalesce(term_y.SUM_SOLD_FUND_Y, 0) + a.SUM_SOLD_FUND) / (coalesce(term_y.C_MONTH_ID, 0) + 1) as AVG_SOLD_FUND_Y,
        
        a.SUM_NEW_FUND as SUM_NEW_FUND,    
        a.SUM_REGULAR_FUND as SUM_REGULAR_FUND_M,
        (coalesce(term_q.SUM_REGULAR_FUND_Q, 0) + a.SUM_REGULAR_FUND) / (coalesce(term_q.C_MONTH_ID, 0) + 1) as SUM_REGULAR_FUND_Q,
        (coalesce(term_y.SUM_REGULAR_FUND_Y, 0) + a.SUM_REGULAR_FUND) / (coalesce(term_y.C_MONTH_ID, 0) + 1) as SUM_REGULAR_FUND_Y,
        
        a.SUM_FEES_FUND as SUM_FEES_FUND,
        
        a.BALANCE_FUND as BALANCE_FUND,
        (coalesce(term_q.SUM_BALANCE_FUND_Q, 0) + a.BALANCE_FUND) / (coalesce(term_q.C_MONTH_ID, 0) + 1) as AVG_BALANCE_FUND_Q,
        (coalesce(term_y.SUM_BALANCE_FUND_Y, 0) + a.BALANCE_FUND) / (coalesce(term_y.C_MONTH_ID, 0) + 1) as AVG_BALANCE_FUND_Y,
        
        (a.BALANCE_FUND - coalesce(prev_val_q.BALANCE_FUND_Q, 0)) as CHANGE_BALANCE_FUND_Q,
        (a.BALANCE_FUND - coalesce(prev_val_y.BALANCE_FUND_Y, 0)) as CHANGE_BALANCE_FUND_Y,
        
        greatest(least(case when prev_val_q.BALANCE_FUND_Q > 0 and a.BALANCE_FUND > 0 then (a.BALANCE_FUND 
                                                                             + coalesce(pt_q.SOLD_PROCESSED, 0) - coalesce(pt_q.BOUGHT_PROCESSED, 0)
                                                                             - coalesce(prev_val_q.BALANCE_FUND_Q, 0)) / prev_val_q.BALANCE_FUND_Q else 0 end, 1), -1) as PORTFOLIO_RETURN_FUND_Q,
                                                      
        greatest(least(case when prev_val_y.BALANCE_FUND_Y > 0 and a.BALANCE_FUND > 0 then (a.BALANCE_FUND 
                                                                             + coalesce(pt_y.SOLD_PROCESSED, 0) - coalesce(pt_y.BOUGHT_PROCESSED, 0)
                                                                             - coalesce(prev_val_y.BALANCE_FUND_Y, 0)) / prev_val_y.BALANCE_FUND_Y else 0 end, 1), -1) as PORTFOLIO_RETURN_FUND_Y,
        
        a.ptf_risk_fund as PORTFOLIO_RISK_FUND,
         
        a.C_REGULAR_FUND as C_REGULAR_FUND,
        a.C_SOLD_FUND as C_SOLD_FUND_M,
        a.C_NEW_FUND as C_NEW_FUND_M,
        a.C_CP_FUND as C_SECURITY_FUND,
        case when a.C_CP_FUND <> 0 then a.BALANCE_FUND / a.C_CP_FUND else 0 end as AVG_PRICE_PER_SECURITY_FUND,
        a.C_ISIN_FUND as C_ISIN_FUND,
        a.C_ISIN_TOTAL_FUND as C_ISIN_TOTAL_FUND,
        
        a.C_SHARES_FUND as C_INVESTED_SHARES_FUND_M,
        a.C_MIX_FUND as C_INVESTED_MIX_FUND_M,
        a.C_BOND_FUND as C_INVESTED_BOND_FUND_M,
        a.C_GUARANTEED_FUND as C_INVESTED_GUARANTEED_FUND_M,
        a.C_MM_FUND as C_INVESTED_MM_FUND_M,
        a.C_OTHER_FUND as C_INVESTED_OTHER_FUND_M,
        a.C_STRUCTURE_FUND as C_INVESTED_STRUCTURE_FUND_M
            
        from
        (
            select b.month_id, 
            b.customerid,
            case when f.c_cp > 0 then 1 else 0 end as I_ACTIVECAPITOL_SK,
            
            coalesce(d.recency_new_regular_fund, process_dt - process_dt_hist) as recency_new_regular_fund,
            coalesce(d.recency_invested_fund, process_dt - process_dt_hist) as recency_invested_fund,
            coalesce(d.recency_sold_fund, process_dt - process_dt_hist) as recency_sold_fund,
            
            coalesce(c.eur_new, 0) + coalesce(c.eur_regular, 0) as sum_invested_fund,
            coalesce(c.eur_sold, 0) as sum_sold_fund,
            coalesce(c.eur_new, 0) as sum_new_fund,
            coalesce(c.eur_regular, 0) as sum_regular_fund,
            coalesce(c.fund_fees, 0) as sum_fees_fund,
            
            case
                when c.c_new > 0 then 1 else 0
            end as i_new_fund,
            case
                when c.c_regular > 0 then 1 else 0
            end as i_regular_fund,
            
            coalesce(c.c_new, 0) as c_new_fund,
            coalesce(c.c_sold, 0) as c_sold_fund,
            coalesce(c.c_regular, 0) as c_regular_fund,
            coalesce(f.BALCAPITOL_PTFI_ULT, 0) as balance_fund,
            coalesce(f.c_cp, 0) as c_cp_fund,
            coalesce(f.c_isin, 0) as c_isin_fund,
            coalesce(f.c_isin_active, 0) as c_isin_total_fund,
            coalesce(round(f.ptf_risk_fund, 1), 0) as ptf_risk_fund,
            coalesce(g.c_fund_shares, 0) as c_shares_fund,
            coalesce(g.c_fund_mix, 0) as c_mix_fund,
            coalesce(g.c_fund_bond, 0) as c_bond_fund,
            coalesce(g.c_fund_garanted, 0) as c_guaranteed_fund,
            coalesce(g.c_fund_mm, 0) as c_mm_fund,
            coalesce(g.c_fund_other, 0) as c_other_fund,
            coalesce(g.c_fund_structure, 0) as c_structure_fund
            from
            (
                select distinct customerid, month_id
                from PM_OWNER.attr_client_base
                where month_id = to_number(to_char(process_dt, 'YYYYMM'))
            ) b
            left join
            (   
                select customerid,
                sum(case when operacia in ('1 - nové sporenie do fondov', '1 - nový obchod') then objem_v_eur else 0 end) as eur_new,
                sum(case when operacia in ('1 - nové sporenie do fondov', '1 - nový obchod') then 1 else 0 end) as c_new,
                
                sum(case when operacia = '6 - Predaj' then objem_v_eur else 0 end) as eur_sold,
                sum(case when operacia = '6 - Predaj' then 1 else 0 end) as c_sold,
                
                sum(case when operacia in ('9 - sporenie') then objem_v_eur else 0 end) as eur_regular,
                sum(case when operacia in ('9 - sporenie') then 1 else 0 end) as c_regular,
                
                sum(objem_popl_v_cm) as fund_fees
                
                from (
    
                      select obchody.id_transakcie
                      , obchody.DATUM_OBCHODU AS d_objednavky
                      , obchody.objem_v_eur
                      , poplatky.OBJEM_POPL_V_CM
                      , poplatky.POKYNDRUH
                      , obchody.customerid
                      , obchody.operacia
                      from 
                      (    select a.*, row_number() over(partition by id_transakcie order by paid_ins) as rn
                           from uni_owner.report_daily_move_capitol_det a
                           where DATUM_OBCHODU > ADD_MONTHS(process_dt, -1) and DATUM_OBCHODU <= process_dt
                           and operacia in ('1 - nový obchod', '1 - nové sporenie do fondov', '4- zatvorený obchod', '6 - Predaj', '9 - sporenie')
                      ) obchody    
                      left join (select id_transakcie, OBJEM_POPL_V_CM, POKYNDRUH, row_number() over(partition by id_transakcie order by paid_ins) as rn
                                 from uni_owner.report_daily_move_capitol
                                 where d_objednavky > ADD_MONTHS(process_dt, -1) and d_objednavky <= process_dt
                                 ) poplatky
                     ON obchody.id_transakcie = poplatky.id_transakcie
                     AND obchody.rn = poplatky.rn
                     WHERE obchody.rn = 1
                     order by obchody.id_transakcie
                    
                      )
                group by customerid
            ) c
            on b.customerid = c.customerid
            left join(
                select 
                coalesce(y.customerid, x.customerid) as customerid,
                
                least(process_dt - coalesce(x.dt_last_invested_fund, process_dt_hist),
                    coalesce(y.recency_invested_fund, process_dt - process_dt_hist)
                    + (process_dt - process_dt_hist)) as recency_invested_fund,
                     
                least(process_dt - coalesce(x.dt_last_new_regular, process_dt_hist),
                    coalesce(y.recency_new_regular_fund, process_dt - process_dt_hist)
                    + (process_dt - ADD_MONTHS(process_dt, -1))) as recency_new_regular_fund,
                    
                least(process_dt - coalesce(x.dt_last_sold_fund,process_dt_hist),
                    coalesce(y.recency_sold_fund, process_dt - process_dt_hist)
                    + (process_dt - ADD_MONTHS(process_dt, -1))) as recency_sold_fund
                    
                from(
                     select customerid, recency_invested_fund, recency_sold_fund, recency_new_regular_fund
                     from pm_owner.attr_fund 
                     where month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
                     
                ) y
               full outer join(
                    select
                    customerid,
                    max(case when smer = 'Nákup' and NOVE_SPORENIE_DO_FONDOV = 'Y' then D_OBJEDNAVKY else null end) as dt_last_new_regular,
                    max(case when smer = 'Nákup' then D_OBJEDNAVKY else null end) as dt_last_invested_fund,
                    max(case when smer = 'Prodej' then D_OBJEDNAVKY else null end) as dt_last_sold_fund
                    from (select id_transakcie, min(d_objednavky) as d_objednavky, min(customerid) as customerid, 
                          nove_sporenie_do_fondov, smer
                          from (select * from uni_owner.report_daily_move_capitol
                                where d_objednavky > ADD_MONTHS(process_dt, -1) and d_objednavky <= process_dt)
                          group by id_transakcie, pokyndruh, smer, nove_sporenie_do_fondov
                          )
                    group by customerid
                ) x
                ON y.customerid = x.customerid
            ) d
            on b.customerid = d.customerid
            left join(
                SELECT PR.PARTY_ID                                              AS customerid
                    ,count(distinct C.cont_id)                                       AS c_cont_id
                    ,count(distinct P.prod_src_val)                                  AS c_isin_active
                    ,sum(case when CBAL.CB10000 > 0 then 1 else 0 end)         AS c_isin
                    ,sum(CBAL.CB10000)                                      AS BALCAPITOL_PTFI_ULT 
                    ,sum(CBAL.CB10050)                                      AS c_cp
                    ,sum(case when CBAL.CB10000 > 0 
                         then CBAL.CB10000 * Z.rizikovost_fondu else 0 end) / greatest(sum(case when CBAL.CB10000 > 0 
                                                                                           then CBAL.CB10000 else 0 end), 1) AS ptf_risk_fund
                FROM L0_OWNER.CONT C
                JOIN L0_OWNER.CONT_PROD CP
                    ON  CP.cont_id = C.cont_id
                    AND CP.cont_prod_start_dt <= process_dt
                    AND CP.cont_prod_end_dt > process_dt
                JOIN L0_OWNER.PROD P
                    ON  P.prod_id = CP.prod_id
                    AND P.prod_type_cd = 13
                JOIN(
                    SELECT   SP.prod_id, max(SG.seg_gen_ds) AS rizikovost_fondu
                    from L0_owner.SEG_GEN SG
                            JOIN
                               L0_owner.SEG S
                            ON     S.seg_cd = SG.seg_cd
                               AND S.src_syst_cd = 90
                               AND S.seg_type_cd = 2
                               AND SG.seg_gen_tl LIKE '%CSOB_SK%'
                         JOIN
                            L0_owner.seg_prod SP
                         ON     SP.seg1_cd = SG.seg_cd
                            AND SP.seg_prod_start_dt <= process_dt
                            AND SP.seg_prod_end_dt > process_dt
                            AND SG.seg_gen_start_dt <= process_dt
                            AND SG.seg_gen_end_dt > process_dt
                    GROUP BY SP.prod_id) Z
                    ON P.prod_id = Z.prod_id
                JOIN L0_OWNER.PARTY_CONT PC
                    ON  PC.cont_id = C.cont_id
                    AND PC.party_role_cd = 1
                    AND PC.party_cont_start_dt <= process_dt
                    AND PC.party_cont_end_dt > process_dt
                LEFT JOIN
                (
                    SELECT
                        CB.cont_id,
                        MAX(CASE WHEN CB.meas_cd = 10000 THEN CB.cont_bal_bc_am ELSE NULL END) AS CB10000,
                        MAX(CASE WHEN CB.meas_cd = 10050 THEN CB.cont_bal_bc_am ELSE NULL END) AS CB10050
                    FROM L0_OWNER.CONT_BAL CB
                        where
                        CB.meas_cd IN (10000, 10050)
                        AND CB.cont_bal_dt = process_dt
                    GROUP BY    
                        CB.cont_id        
                ) CBAL
                    ON  CBAL.cont_id = C.cont_id
                JOIN L0_OWNER.PARTY_RLTD PR
                on PR.child_party_id = PC.party_id
                AND PR.party_rltd_start_dt <= process_dt
                AND PR.party_rltd_end_dt > process_dt
                AND PR.party_rltd_rsn_cd = 4 -- customerid
                WHERE
                    C.src_syst_cd IN (90,660)
                group by PR.PARTY_ID
            ) f
            on b.customerid = f.customerid
            left join
                (
                    SELECT customerid
                    , sum(case when typ_fondu = 'Akciový' then 1 else 0 end) as c_fund_shares
                    , sum(case when typ_fondu = 'Smíšený' then 1 else 0 end) as c_fund_mix
                    , sum(case when typ_fondu = 'Dluhopisový' then 1 else 0 end) as c_fund_bond
                    , sum(case when typ_fondu = 'Zajištěný' then 1 else 0 end) as c_fund_garanted
                    , sum(case when typ_fondu = 'Peněžního trhu' then 1 else 0 end) as c_fund_mm
                    , sum(case when typ_fondu = 'Struktura' then 1 else 0 end) as c_fund_structure
                    , sum(case when typ_fondu is null then 1 else 0 end) as c_fund_other
                    from (
                            select customerid, id_transakcie, isin, typ_fondu
                            from uni_owner.report_daily_move_capitol
                            where D_OBJEDNAVKY > ADD_MONTHS(process_dt, -1) 
                            and D_OBJEDNAVKY <= process_dt and smer = 'Nákup'
                            group by customerid, id_transakcie, isin, typ_fondu
                        )
                    group by customerid
                ) g
            on b.customerid = g.customerid
        ) a
        left join(
                SELECT
                customerid,
                COUNT(DISTINCT month_id) c_month_id
                , MAX(I_NEW_FUND) AS I_NEW_FUND_Q
                , MAX(I_REGULAR_FUND) AS I_REGULAR_FUND_Q
                , MAX(I_CLOSED_FUND) AS I_CLOSED_FUND_Q
                , MAX(I_NEW_REGULAR_FUND_M) AS I_NEW_REGULAR_FUND_Q
                
                , SUM(SUM_INVESTED_FUND_M) AS SUM_INVESTED_FUND_Q
                , SUM(SUM_REGULAR_FUND_M) AS SUM_REGULAR_FUND_Q
                , SUM(BALANCE_FUND) AS SUM_BALANCE_FUND_Q
                , SUM(SUM_SOLD_FUND_M) AS SUM_SOLD_FUND_Q
                FROM pm_owner.attr_fund
                WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -2), 'YYYYMM'))
                               AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
                GROUP BY customerid
        ) term_q
        ON a.customerid = term_q.customerid
        left join(
                    SELECT
                    customerid,
                    COUNT(DISTINCT month_id) C_MONTH_ID
                    , MAX(I_NEW_FUND) AS I_NEW_FUND_Y
                    , MAX(I_REGULAR_FUND) AS I_REGULAR_FUND_Y
                    , MAX(I_CLOSED_FUND) AS I_CLOSED_FUND_Y
                    , MAX(I_NEW_REGULAR_FUND_M) AS I_NEW_REGULAR_FUND_Y
                    
                    , SUM(SUM_INVESTED_FUND_M) AS SUM_INVESTED_FUND_Y
                    , SUM(SUM_REGULAR_FUND_M) AS SUM_REGULAR_FUND_Y
                    , SUM(BALANCE_FUND) AS SUM_BALANCE_FUND_Y
                    , SUM(SUM_SOLD_FUND_M) AS SUM_SOLD_FUND_Y
                    FROM pm_owner.attr_fund
                    WHERE month_id BETWEEN to_number(to_char(ADD_MONTHS(process_dt,  -11), 'YYYYMM'))
                                   AND to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
                    GROUP BY customerid
        ) term_y
        ON a.customerid = term_y.customerid
        left join(
                    SELECT customerid
                    --, SUM_INVESTED_FUND AS SUM_INVESTED_FUND_PREV_Q
                    ----, SUM_SOLD_FUND AS SUM_SOLD_FUND_PREV_Q
                    , BALANCE_FUND AS BALANCE_FUND_Q
                    , month_id
                    FROM pm_owner.attr_fund
                    WHERE month_id = to_number(to_char(ADD_MONTHS(process_dt,  -3), 'YYYYMM'))
        ) prev_val_q
        ON a.customerid = prev_val_q.customerid
        left join(
                    SELECT
                    customerid
                    --, SUM_INVESTED_FUND AS SUM_INVESTED_FUND_PREV_Y
                    --, SUM_SOLD_FUND AS SUM_SOLD_FUND_PREV_Y
                    , BALANCE_FUND AS BALANCE_FUND_Y
                    , month_id
                    FROM pm_owner.attr_fund
                    WHERE month_id = to_number(to_char(ADD_MONTHS(process_dt,  -12), 'YYYYMM'))
        ) prev_val_y
        ON a.customerid = prev_val_y.customerid
        left join(
                    SELECT
                    customerid
                    , balance_fund
                    , month_id
                    , c_regular_fund
                    FROM pm_owner.attr_fund
                    WHERE month_id = to_number(to_char(ADD_MONTHS(process_dt,  -1), 'YYYYMM'))
        ) prev_val
        ON a.customerid = prev_val.customerid
        left join(
                 select customerid
                        , sum(case when smer = 'Prodej' then objem_v_eur else 0 end) as sold_processed
                        , sum(case when smer = 'Nákup' then objem_v_eur else 0 end) as bought_processed
                 from
                 (
                  select id_transakcie, min(DATUM_VYSPOR) as DATUM_VYSPOR,
                         avg(objem_v_eur) as objem_v_eur,
                         min(customerid) as customerid, smer
                         from (select * from uni_owner.report_daily_move_capitol
                               where DATUM_VYSPOR >= ADD_MONTHS(process_dt, -3) and DATUM_VYSPOR < process_dt)
                  group by id_transakcie, smer
                )
                group by customerid
        ) pt_q
        ON a.customerid = pt_q.customerid
        left join(
                 select customerid
                        , sum(case when smer = 'Prodej' then objem_v_eur else 0 end) as sold_processed
                        , sum(case when smer = 'Nákup' then objem_v_eur else 0 end) as bought_processed
                 from
                 (
                  select id_transakcie, min(DATUM_VYSPOR) as DATUM_VYSPOR,
                         avg(objem_v_eur) as objem_v_eur,
                         min(customerid) as customerid, smer
                         from (select * from uni_owner.report_daily_move_capitol
                               where DATUM_VYSPOR >= ADD_MONTHS(process_dt, -12) 
                               and DATUM_VYSPOR < process_dt
                               and DC_DATE <= process_dt)
                  group by id_transakcie, smer
                 )
                 group by customerid
        ) pt_y
        ON a.customerid = pt_y.customerid
        );
        
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;              
        mng_stats(l_owner_name, l_table_name, l_proc_name);   
    END;

    PROCEDURE append_ATTR_INSURANCE(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
        l_proc_name varchar2(255) := 'append_ATTR_INSURANCE';
        l_owner_name varchar2(30) := 'PM_OWNER';
        l_table_name varchar2(30) := 'ATTR_INSURANCE';
        l_month_start_dt    date  := trunc(process_dt,'MM');
        l_month_end_dt      date  := last_day(process_dt);
        
        BEGIN
    
        step(p_stepname => l_proc_name || ' TRUNC', p_source => 'N/A', p_target => 'STG_INSURANCE_CONT');
        ETL_OWNER.etl_dbms_util.truncate_table('STG_INSURANCE_CONT',l_owner_name);       
            
        step(p_stepname => l_proc_name || ' INSERT STG_INSURANCE_CONT', p_source => 'L0.*', p_target => 'STG_INSURANCE_CONT');
        INSERT INTO pm_owner.STG_INSURANCE_CONT
        (  
            SELECT /*+ ORDERED */
            process_dt                              AS insert_dt,
            DTSK.CUSTOMERID,
            C.cont_id,
            COALESCE(CPZ.cont_acct, C.cont_acct)    AS CONT_ACCT,
            PID.prod_src_val,
            CONT_STAV.OTVORENA                      AS ACTIVE,
            CONT_STAV.cont_stat_type_ds             AS STATUS,
            case when CDA.cont_dt_actl_dt < least(process_dt, CG.cont_gen_matur_dt)
                 then CDA.cont_dt_actl_dt else null end AS DT_CANCELED_INS,        
            CG.cont_gen_sign_dt                     AS DT_NEGOTIATED_INS,
            CG.cont_gen_open_dt                     AS DT_START_INS,
            CG.cont_gen_matur_dt                    AS DT_END_INS,
            UKAZ1.SPLATKA_VYSKA                     AS PAYMENT_VALUE,
            UKAZ1.POISTNE                           AS INSURANCE_PREMIUM,
            coalesce(UKAZ1.KAPITALOVA_HODNOTA, 0)   AS CAPITAL_VALUE,
            UKAZ1.DT_PRVEJ_PLATBY                   AS DT_FIRST_PAYMENT,
            DTSK.TYP
        FROM L0_OWNER.PARTY P
        -------- Informácie o klientovi
        JOIN
        (
            SELECT /*+ full(po) full(pon) full(pin) full(p) full(prl) full(pi) */
                PRL.party_id as CUSTOMERID,
                P.party_id,
                P.party_src_val,
                P.party_subtp_cd as typ
            FROM L0_OWNER.PARTY P
            JOIN L0_OWNER.PARTY_RLTD PRL
                ON  PRL.child_party_id = P.party_id
                AND PRL.party_rltd_start_dt <= process_dt
                AND PRL.party_rltd_end_dt > process_dt
                AND PRL.party_rltd_rsn_cd = 4
            WHERE
                P.src_syst_cd = 790
        ) DTSK
            ON  DTSK.party_id = P.party_id
        JOIN L0_OWNER.PARTY_CONT PC
            ON  PC.party_id = P.party_id
            AND PC.party_role_cd = 168 -- Klient Poisťovni SR
            AND PC.party_cont_start_dt <= process_dt
            AND PC.party_cont_end_dt > process_dt
        JOIN L0_OWNER.CONT C
            ON  C.cont_id = PC.cont_id
            AND (C.cont_type_cd = 3 OR (C.cont_type_cd = 2 AND C.cont_single_acct_in = 'Y'))
            AND C.src_syst_cd = 790
        LEFT JOIN
        (
            SELECT
                CRL.cont_id,
                C.cont_acct
            FROM L0_OWNER.CONT_RLTD CRL
            JOIN L0_OWNER.CONT C
                ON  C.cont_id = CRL.child_cont_id
                AND C.src_syst_cd = 790
            WHERE
                CRL.etl_src_syst_cd = 790
                AND CRL.cont_rltd_rsn_cd = 71
            GROUP BY
                CRL.cont_id,
                C.cont_acct
        ) CPZ
            ON  CPZ.cont_id = C.cont_id
        JOIN L0_OWNER.CONT_GEN CG
            ON  CG.cont_id = C.cont_id
            AND CG.cont_gen_start_dt <= process_dt
            AND CG.cont_gen_end_dt > process_dt
        JOIN L0_OWNER.CONT_PROD PID_C
            ON  PID_C.cont_id = C.cont_id
            AND PID_C.cont_prod_start_dt <= process_dt
            AND PID_C.cont_prod_end_dt > process_dt
        JOIN L0_OWNER.PROD PID
            ON  PID.prod_id = PID_C.prod_id
            AND PID.src_syst_cd = 790
        JOIN L0_OWNER.CONT_STAT
            ON  CONT_STAT.cont_id = C.cont_id
            AND CONT_STAT.cont_stat_start_dt <= process_dt
            AND CONT_STAT.cont_stat_end_dt > process_dt
        -------- CISELNIK: Určenie stavu otvorenia
        JOIN
        (
            SELECT
                cont_stat_type_cd,
                cont_stat_type_ds,
                cont_stat_type_gen_start_dt,
                CASE
                    WHEN CSTG.cont_stat_type_ds NOT IN (
                                                        'Storno pojistky',
                                                        'Pojistka nebyla ještě verifikována',
                                                        'Pojistka je chybná a nebyla ještě verifikován',
                                                        'Pojistka je vrácena z verifikace k opravě'
                                                        /*,'Pojistka je verifikována, ale nemá dosud vytisknutu výbavu'*/
                                                       ) THEN 1
                    ELSE 0
                END as OTVORENA
            FROM L0_OWNER.CONT_STAT_TYPE_GEN CSTG
            WHERE
                CSTG.cont_stat_type_gen_start_dt <= process_dt
                AND CSTG.cont_stat_type_gen_end_dt > process_dt
        ) CONT_STAV
            ON  CONT_STAV.cont_stat_type_cd = CONT_STAT.cont_stat_type_cd
        LEFT JOIN L0_OWNER.CONT_DT_ACTL CDA
            ON  CDA.cont_id = C.cont_id
            AND CDA.meas_cd = 8016
        -------- UKAZOVATELE 1: Datumy a Poistné
        LEFT JOIN
        (
            SELECT
                C.cont_id,
                MAX(CASE WHEN CV.meas_cd = 30654 THEN CV.cont_val_am ELSE NULL END) as SPLATKA_VYSKA,
                MAX(CASE WHEN CV.meas_cd = 30655 THEN CV.cont_val_am ELSE NULL END) as POISTNE,
                MAX(CASE WHEN CV.meas_cd = 30656 THEN CV.cont_val_am ELSE NULL END) as KAPITALOVA_HODNOTA,
                MAX(CASE WHEN CH.meas_cd = 6023 THEN CH.cont_dt_hist_dt ELSE NULL END) as DT_VLOZENIA_KONTR_POIST,
                MAX(CASE WHEN CH.meas_cd = 6022 THEN CH.cont_dt_hist_dt ELSE NULL END) as DT_PRVEJ_PLATBY
            FROM L0_OWNER.CONT C
            LEFT JOIN L0_OWNER.CONT_VAL CV
                ON  CV.cont_id = C.cont_id
                AND CV.cont_val_start_dt <= process_dt
                AND CV.cont_val_end_dt > process_dt
                AND CV.meas_cd IN(30654, 30655, 30656)
            LEFT JOIN L0_OWNER.CONT_DT_HIST CH
                ON  CH.cont_id = C.cont_id
                AND CH.cont_dt_start_dt <= process_dt
                AND CH.cont_dt_end_dt > process_dt
                AND CH.meas_cd IN(6022, 6023)
            WHERE
                (C.cont_type_cd = 3 OR (C.cont_type_cd = 2 AND C.cont_single_acct_in = 'Y'))
                AND C.src_syst_cd = 790
            GROUP BY
                C.cont_id
        ) UKAZ1
            ON  UKAZ1.cont_id = C.cont_id
        where (case when CONT_STAV.cont_stat_type_ds = 'Storno pojistky' 
               and (CONT_STAT.cont_stat_start_dt < process_dt_hist 
                    or CG.cont_gen_matur_dt < process_dt_hist) then 0 else 1 end) = 1)
        -- predosla podmienka vyradi kontrakty ukoncene/stornovane viac ako 5 rokov dozadu
        ;
    
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;
      
        step(p_stepname => l_proc_name || ' INSERT tmp_insurance_cont', p_source => 'L0.*', p_target => 'tmp_insurance_cont');
        INSERT INTO PM_OWNER.tmp_insurance_cont
        SELECT
                   to_number(to_char(process_dt, 'YYYYMM')) as MONTHID
                   , K.ACCOUNT_OWNER as CUSTOMERID         
                   , '-1' as CONT_ACCT
                   , 1 as ACTIVE
                   , '301' as prod_src_val
                   , 1 as I_NONLIFE_INS
                   , process_dt-start_dt as DAYS_FROM_AGREEMENT
                   , process_dt-start_dt as DAYS_FROM_START
                   , end_dt - process_dt as DAYS_to_END
                   , process_dt - process_dt_hist as DAYS_FROM_CANCELED
                   , process_dt - process_dt_hist as DAYS_FROM_PAYOUT
                   
                   , 0 as i_canceled
                   
                   -----THIS PART IS DUE TO BE CHECKED ACCORDING ACTUAL PRICING OF PRODUCTS !!!!!!!
                   , CASE WHEN ((k.prod_id_card in (select prod_id from l0_owner.prod 
                                                    where prod_src_val in (select prod_src_val from pm_owner.PRODUCT_LEVEL_CATEGORY 
                                                                           where lvl4_category = '1122')) and CSV.PROD_NM_CZ='CSOB Standard' )
                                --OR (CSV.PROD_NM_CZ='CSOB Exclusive' and k.card_description='Debit Gold MC debetná CL')
                                )
                              THEN 0 
                          WHEN (CSV.PROD_NM_CZ='CSOB Standard' ) 
                              THEN (SELECT PAYMENT_VALUE 
                                    FROM (SELECT PAYMENT_VALUE, row_number() over (order by PAYMENT_VALUE) rn 
                                          FROM pm_owner.stg_insurance_cont 
                                          WHERE prod_src_val = '301' 
                                              AND DT_START_INS between ADD_MONTHS(process_dt, -1) AND process_dt 
                                          GROUP BY PAYMENT_VALUE)
                                    WHERE rn = 1)
                         WHEN (CSV.PROD_NM_CZ='CSOB Exclusive') 
                             THEN (SELECT PAYMENT_VALUE 
                                   FROM (SELECT PAYMENT_VALUE, row_number() over (order by PAYMENT_VALUE) rn 
                                         FROM pm_owner.stg_insurance_cont 
                                         WHERE prod_src_val = '301' 
                                              AND DT_START_INS between ADD_MONTHS(process_dt, -1) AND process_dt 
                                         GROUP BY PAYMENT_VALUE) 
                                   WHERE rn = 3)
                        WHEN (CSV.PROD_NM_CZ='CSOB Standard family' ) 
                            THEN (SELECT PAYMENT_VALUE
                                  FROM (SELECT PAYMENT_VALUE, row_number() over (order by PAYMENT_VALUE) rn 
                                        FROM pm_owner.stg_insurance_cont 
                                        WHERE prod_src_val = '301' 
                                              AND DT_START_INS between ADD_MONTHS(process_dt, -1) AND process_dt ) 
                                  WHERE rn = 2)
                       WHEN (CSV.PROD_NM_CZ='CSOB Exclusive family') 
                           THEN (SELECT PAYMENT_VALUE 
                                 FROM (SELECT PAYMENT_VALUE, row_number() over (order by PAYMENT_VALUE) rn 
                                       FROM pm_owner.stg_insurance_cont 
                                       WHERE prod_src_val = '301' 
                                              AND DT_START_INS between ADD_MONTHS(process_dt, -1) AND process_dt 
                                       GROUP BY PAYMENT_VALUE) 
                                 WHERE rn = 4)
                       END as payment_year
                       
                   , 0 as num_payment_year
                   , 0 as CAPITAL_VALUE
                
                    -- LIFE PRODUCTS  
                   , 0 as risk_life_ins
                   , 0 as invest_life_ins
                   , 0 as loan_ins
                   , 0 as reg_expenses_ins
                    -- NONLIFE PRODUCTS 
                   , 1 as card_travel_ins
                   , 0 as travel_ins
                   , 0 as mtpl_ins
                   , 0 as casco_ins
                   , 0 as drivers_accident_ins
                   , 0 as household_property_ins
                   , 0 as business_risk_ins
                
                   , 0 as travel_ins_europe
                   , 0 as travel_ins_tourist
                   , 0 as travel_ins_risksport
                   , 0 as travel_ins_manual_labor
                   , 0 as travel_ins_nonmanual_labor
                   , 0 as travel_ins_luggage_amt
                   , 0 as duration_travel_ins
                   
                   , case when CSV.PROD_NM_CZ in ('CSOB Standard family', 'CSOB Exclusive family')
                     then 1 else 0 end as travel_ins_family
                   , case when CSV.PROD_NM_CZ in ('CSOB Exclusive', 'CSOB Exclusive family')
                     then 1 else 0 end as  travel_ins_exclusive
                    
                   , 0 as NUM_INSURED 
                   , 0 as NUM_INSURED_CHILDREN
                   , 0 as NUM_INSURED_SMALL_CHILDREN
                   , 0 as NUM_INSURED_TEEN_CHILDREN
                   , 0 as NUM_INSURED_SENIOR
                   
                   , 0 as household_property_ins_type0
                   , 0 as household_property_ins_type1
                   , 0 as household_property_ins_type2
                   , 0 as household_property_ins_type3   
                   , 0 as FLG_LIABILITY
                   , 0 as FLG_POOL
                
                   , 0 as household_property_ins_west
                   , 0 as household_property_ins_middle
                   , 0 as household_property_ins_east
                   , 0 as household_property_ins_capital
                 
                   , 0 as pivnica_plocha
                   , 0 as garaz_plocha
                   , 0 as domacnost_podkrovie_m2
                   , 0 as domacnost_byt_m2
                   , 0 as povala_plocha
                   , 0 as vedl_budova_plocha
                   , 0 as domacnost_garaz_m2
                   , 0 as prizemie_plocha
                   , 0 as domacnost_vedl_budova_m2 
                   , 0 as domacnost_prizemie_m2
                   , 0 as domacnost_pivnica_m2
                   , 0 as podkrovie_plocha 
                   , 0 as podlazie_plocha 
                   , 0 as domacnost_povala_m2 
                   , 0 as byt_plocha
                   , 0 as domacnost_podlazie_m2
                   , 0 as dom_obyvana_m2
                   , 0 as dom_neobyvana_m2
                   
                   , 0 as HLASENIE_PU
                   , 0 as VZNIK_PU
                   , 0 as REVIDACIA_PU
                   , 0 as PLNENIE_PU
                   , 0 as NELIKVIDNA_PU
                   
                   , 0 as I_VEHICLE_EVENT_INS
                   , 0 as I_HOUSEHOLD_PROPERTY_EVENT_INS
                   , 0 as I_LIFE_EVENT_INS
                
                    , 0 as leasing
                    , 0 as REMINDER
                    , 0 as sum_owed    
                    
                   FROM   pm_owner.card   /*KARTY*/ K
                   JOIN (
                       SELECT card_id, prod_id, PROD_NM_CZ, min(card_svc_start_dt) as start_dt, max(card_svc_end_dt ) as end_dt 
                       FROM ( SELECT card_id, CSVCNM.PROD_NM_CZ, card_svc_start_dt, card_svc_end_dt, CSVCNM.prod_id
                              FROM L0_OWNER.CARD_SVC CSVC
                               JOIN L0_OWNER.PROD CPRD
                                   ON CPRD.prod_id = CSVC.prod_id
                                       AND CPRD.prod_type_cd IN (16, 136)
                               JOIN L0_OWNER.PROD_NM CSVCNM
                                   ON CSVCNM.prod_id = CSVC.prod_id
                                       AND CSVCNM.prod_nm_start_dt <= process_dt
                                       AND CSVCNM.prod_nm_end_dt > process_dt
                               JOIN L0_OWNER.PROD_HIER CPH
                                   ON CPH.child_prod_id = CPRD.prod_id
                                       AND CPH.prod_hier_start_dt <= process_dt
                                       AND CPH.prod_hier_end_dt > process_dt
                               JOIN L0_OWNER.PROD_NM CSVCNM2
                                   ON CSVCNM2.prod_id = CPH.prod_id
                                       AND CSVCNM2.prod_nm_start_dt <= process_dt
                                       AND CSVCNM2.prod_nm_end_dt > process_dt)
                       GROUP BY card_id, PROD_NM_CZ, prod_id) CSV 
                   ON k.card_id = csv.card_id
                   AND k.month_id = to_number(to_char(process_dt, 'YYYYMM'))
                   WHERE CSV.start_dt <= process_dt 
                   AND CSV.end_dt > process_dt;
    
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;
     
        INSERT INTO PM_OWNER.tmp_insurance_cont
        (
            select 
                    distinct
                    to_number(to_char(process_dt, 'YYYYMM')) as MONTHID
                    , CONTRACT.CUSTOMERID
                    , CONTRACT.CONT_ACCT
                    , CONTRACT.ACTIVE
                    , CONTRACT.PROD_SRC_VAL
                    , case when CONTRACT.PROD_SRC_VAL in (select prod_src_val
                                                          from pm_owner.product_level_category
                                                          where src_syst_dsc = 'INSURANCE'
                                                          and lvl2_category = 3200)
                      then 1 else 0 end as I_NONLIFE_INS
                    , process_dt-CONTRACT.DT_NEGOTIATED_INS as DAYS_FROM_AGREEMENT
                    , process_dt-CONTRACT.DT_START_INS as DAYS_FROM_START
                    , least(CONTRACT.DT_END_INS - process_dt, process_dt - process_dt_hist) as DAYS_TO_END
                    , process_dt - coalesce(CONTRACT.DT_CANCELED_INS, process_dt_hist) as DAYS_FROM_CANCELED
                    , case when pu.PLNENIE > 0 and pu.D_REVIDACIE <= process_dt 
                           then process_dt-coalesce(pu.D_REVIDACIE, process_dt_hist) else process_dt - process_dt_hist end as DAYS_FROM_PAYOUT
                    
                    , CASE WHEN ( CONTRACT.DT_CANCELED_INS < CONTRACT.DT_END_INS AND (ADD_MONTHS(process_dt, -1) < CONTRACT.DT_CANCELED_INS 
                           and CONTRACT.DT_CANCELED_INS <= process_dt )) THEN 1 ELSE 0 END as I_CANCELED
                
                    , finukaz.INSURANCE_PREMIUM as PAYMENT_YEAR
                    , ROUND(finukaz.INSURANCE_PREMIUM / finukaz.PAYMENT_VALUE) as NUM_PAYMENT_YEAR
                    , NVL(finukaz.CAPITAL_VALUE,0) as  CAPITAL_VALUE
        
                    -- LIFE PRODUCTS  
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3110 then 1 else 0 end as risk_life_ins
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3130 then 1 else 0 end as invest_life_ins
                    -- note: loan_ins only 1 customerid - bank
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3140 then 1 else 0 end as loan_ins
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3120 then 1 else 0 end as reg_expenses_ins
                    -- NONLIFE PRODUCTS 
                    , CASE WHEN prod_cat.LVL4_CATEGORY=3213 then 1 else 0 end as card_travel_ins --TODO tu mozno dat 0 as card_travel_ins, pretoze je tam iba jedno customerid a to banka
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3210 and prod_cat.LVL4_CATEGORY is null then 1 else 0 end as travel_ins
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3220 then 1 else 0 end as mtpl_ins
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3230 then 1 else 0 end as casco_ins
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3240 then 1 else 0 end as drivers_accident_ins
                    -- tu sa ako household insurance rataju vsetky domosy, cize poistenie nehnutelnost aj domacnosti
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3250 then 1 else 0 end as household_property_ins
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3270 then 1 else 0 end as business_risk_ins
                    
                    , CASE WHEN CP_INFO.INS_TERRITORY='Európa' then 1 else 0 end as travel_ins_europe
                    
                    , CASE WHEN CP_INFO.RISK_TYPE='turisticke' then 1 else 0 end as travel_ins_tourist
                    , CASE WHEN CP_INFO.RISK_TYPE='rizikove sporty' then 1 else 0 end as travel_ins_risksport
                    , CASE WHEN CP_INFO.RISK_TYPE='maualna praca' then 1 else 0 end as travel_ins_manual_labor
                    , CASE WHEN CP_INFO.RISK_TYPE='nemanualna praca' then 1 else 0 end as travel_ins_nonmanual_labor
                    
                    , NVL(CP_INFO.LUGGAGE_INS_AMT, 0) as travel_ins_luggage_amt
                    , CASE WHEN prod_cat.LVL3_CATEGORY=3210 and prod_cat.LVL4_CATEGORY is null and coalesce(CONTRACT.DT_END_INS - CONTRACT.DT_START_INS, 0) < 180
                           then coalesce(least(CONTRACT.DT_END_INS, process_dt) - greatest(CONTRACT.DT_START_INS, ADD_MONTHS(process_dt, -1)), 0) 
                           else 0 end as duration_travel_ins
                    
                    , CASE WHEN CP_INFO.VARIANT in ('Stan. Family','EXC. Family') then 1 else 0 end as travel_ins_family
                    , CASE WHEN CP_INFO.VARIANT in ('EXCLUSIVE','EXC. Family','Prémiový') then 1 else 0 end as travel_ins_exclusive
                    
                    , NVL(CP_OSOBY2.NUM_INSURED,0) as NUM_INSURED
                    , NVL(CP_OSOBY3.NUM_INSURED_CHILDREN,0) as NUM_INSURED_CHILDREN
                    , NVL(CP_OSOBY3.NUM_INSURED_SMALL_CHILDREN,0) as NUM_INSURED_SMALL_CHILDREN
                    , NVL(CP_OSOBY3.NUM_INSURED_TEEN_CHILDREN,0) as NUM_INSURED_TEEN_CHILDREN
                    , NVL(CP_OSOBY4.NUM_INSURED_SENIOR,0) as NUM_INSURED_SENIOR
                    
                    , CASE WHEN DOMOS.HOUSE_INS_TYPE=0 then 1 else 0 end as household_property_ins_type0
                    , CASE WHEN DOMOS.HOUSE_INS_TYPE=1 then 1 else 0 end as household_property_ins_type1
                    , CASE WHEN DOMOS.HOUSE_INS_TYPE=2 then 1 else 0 end as household_property_ins_type2
                    , CASE WHEN DOMOS.HOUSE_INS_TYPE=3 then 1 else 0 end as household_property_ins_type3
                    , NVL(DOMOS.FLG_LIABILITY,0) as FLG_LIABILITY
                    , NVL(DOMOS.FLG_POOL,0) as FLG_POOL
                
                    , CASE WHEN admin.NAZOV_OBLASTI='Západné Slovensko' then 1 else 0 end as household_property_ins_west
                    , CASE WHEN admin.NAZOV_OBLASTI='Stredné Slovensko' then 1 else 0 end as household_property_ins_middle
                    , CASE WHEN admin.NAZOV_OBLASTI='Východné Slovensko' then 1 else 0 end as household_property_ins_east
                    , CASE WHEN admin.NAZOV_OBLASTI='Bratislavský kraj' then 1 else 0 end as household_property_ins_capital
                
                    , NVL(DOMOS_PLOCHA.pivnica_plocha,0) as pivnica_plocha
                    , NVL(DOMOS_PLOCHA.garaz_plocha,0) as garaz_plocha
                    , NVL(DOMOS_PLOCHA.domacnost_podkrovie_m2,0) as domacnost_podkrovie_m2
                    , NVL(DOMOS_PLOCHA.domacnost_byt_m2,0) as domacnost_byt_m2
                    , NVL(DOMOS_PLOCHA.povala_plocha,0) as povala_plocha
                    , NVL(DOMOS_PLOCHA.vedl_budova_plocha,0) as vedl_budova_plocha
                    , NVL(DOMOS_PLOCHA.domacnost_garaz_m2,0) as domacnost_garaz_m2
                    , NVL(DOMOS_PLOCHA.prizemie_plocha,0) as prizemie_plocha
                    , NVL(DOMOS_PLOCHA.domacnost_vedl_budova_m2,0) as domacnost_vedl_budova_m2
                    , NVL(DOMOS_PLOCHA.domacnost_prizemie_m2,0) as domacnost_prizemie_m2
                    , NVL(DOMOS_PLOCHA.domacnost_pivnica_m2,0) as domacnost_pivnica_m2
                    , NVL(DOMOS_PLOCHA.podkrovie_plocha,0) as podkrovie_plocha
                    , NVL(DOMOS_PLOCHA.podlazie_plocha,0) as podlazie_plocha
                    , NVL(DOMOS_PLOCHA.domacnost_povala_m2,0) as domacnost_povala_m2
                    , NVL(DOMOS_PLOCHA.byt_plocha,0) as byt_plocha
                    , NVL(DOMOS_PLOCHA.domacnost_podlazie_m2,0) as domacnost_podlazie_m2
                    , NVL(DOMOS_PLOCHA.dom_obyvana_m2, 0) as dom_obyvana_m2
                    , NVL(DOMOS_PLOCHA.dom_neobyvana_m2, 0) as dom_neobyvana_m2
                    
                    , case when pu.DAT_HLASENIA_PU between ADD_MONTHS(process_dt, -1) AND process_dt then 1 else 0 end as HLASENIE_PU
                    , case when pu.D_VZNIKU_PU between ADD_MONTHS(process_dt, -1) AND process_dt then 1 else 0 end as VZNIK_PU
                    , case when pu.D_REVIDACIE between ADD_MONTHS(process_dt, -1) AND process_dt then 1 else 0 end as REVIDACIA_PU
                    , case when (pu.D_REVIDACIE between ADD_MONTHS(process_dt, -1) AND process_dt ) 
                            then nvl(pu.PLNENIE,0) else 0 end as PLNENIE_PU
                    , case when (pu.D_REVIDACIE between ADD_MONTHS(process_dt, -1) AND process_dt and (pu.likvidny = 'Nelikvidny          ' or pu.plnenie = 0)) 
                            then 1 else 0 end as NELIKVIDNA_PU
        
                    --  count of most frequent insurance claim events related to vehicle and household/real estate or life insurance
                    , case when pu.typ_pu_popis in ('Škoda na vozidle spôsobená stretom vozidiel', 'Havária motorového vozidla',
                                                        'Stret so zverou', 'Škoda na vozidle spôsobená padnutým-odleteným predmetom',
                                                        'Poškodenie skla', 'Poistenie skiel') then 1 else 0 end as I_VEHICLE_EVENT_INS
                    , case when pu.typ_pu_popis in ('Živel - stavba', 'Živel - domácnosť', 'Sklo - domácnosť', 'Sklo - stavba', 
                                                        'Stroj - domácnosť', 'Škoda na nehnuteľnosti', 'Stroj - stavba', 'Odcudzenie - domácnosť') then 1 else 0 end as I_HOUSEHOLD_PROPERTY_EVENT_INS
                    , case when pu.typ_pu_popis in ('Denné odškodné EUR', 'Denné odškodné', 'Hosp. choroby/úrazu EUR', 'Zrýchlené plnenie úrazu EUR', 'Trvalé následky') then 1 else 0 end as I_LIFE_EVENT_INS
                    
                    , case when CONTRACT.PROD_SRC_VAL IN (select prod_src_val 
                                                          from pm_owner.PRODUCT_LEVEL_CATEGORY
                                                          where lower(PROD_DSC) like '%leasing%') then 1 else 0 end as LEASING
                    , case when upom.typ_upom is not null then 1 else 0 end as REMINDER
                    , coalesce(upom.DLZNE_POISTNE, 0) as sum_owed
        
                    
                from
                (  select c.insert_dt, c.CUSTOMERID, c.CONT_ID, c.CONT_ACCT,
                        c.PROD_SRC_VAL, c.DT_NEGOTIATED_INS, c.DT_START_INS, c.DT_END_INS,
                        c.PAYMENT_VALUE, c.INSURANCE_PREMIUM, c.CAPITAL_VALUE, 
                        case when c.DT_END_INS >= process_dt and (c.DT_CANCELED_INS is null or c.DT_CANCELED_INS > process_dt ) then c.ACTIVE else 0 end as ACTIVE,
                        coalesce(coalesce(coalesce(c.DT_CANCELED_INS,pzp_storna.D_STORNA),cakaren_storien.DATUM_STORNA),ins_data.DAT_STORNA) as DT_CANCELED_INS
                        from (
                                select 
                                    insert_dt, CUSTOMERID, CONT_ID,
                                    CASE WHEN INSTR(CONT_ACCT,'-')=0 then CONT_ACCT
                                        WHEN INSTR(CONT_ACCT,'-')=4 then REPLACE(CONT_ACCT ,'-','')
                                        WHEN INSTR(CONT_ACCT,'-')=10 then substr(CONT_ACCT,1,INSTR(CONT_ACCT,'-')-1)
                                        WHEN INSTR(CONT_ACCT,'-')=11 then substr(CONT_ACCT,1,INSTR(CONT_ACCT,'-')-1)
                                        end as CONT_ACCT,
                                    PROD_SRC_VAL,
                                    DT_NEGOTIATED_INS, DT_START_INS, DT_END_INS,
                                    DT_CANCELED_INS, PAYMENT_VALUE, INSURANCE_PREMIUM,
                                    CAPITAL_VALUE, ACTIVE
                                from pm_owner.stg_insurance_cont
                            )c
                    left join L0_OWNER.INSURANCE_PZP_STORNA_DAT pzp_storna
                    on pzp_storna.KOD_CISLO_ZMLUVY=c.CONT_ACCT
                    left join L0_OWNER.INSURANCE_CAKAJUCE_STOR_DAT cakaren_storien
                    on cakaren_storien.KOD_CISLO_ZMLUVY=c.CONT_ACCT
                    left join l0_owner.insurance_zmluvy_dat ins_data
                    on ins_data.C_POJ_SML=c.CONT_ACCT
                ) CONTRACT
                
                LEFT JOIN pm_owner.PRODUCT_LEVEL_CATEGORY prod_cat
                    ON prod_cat.PROD_SRC_VAL=contract.PROD_SRC_VAL
                    and SRC_SYST_CD=790 --INSURANCE
                    
                LEFT JOIN (SELECT distinct CPZ, INS_TERRITORY, RISK_TYPE, LUGGAGE_INS_AMT, VARIANT
                           FROM L0_OWNER.PZ_DETAIL_CP_DETAIL_DAT ) CP_INFO
                        ON CONTRACT.CONT_ACCT = CP_INFO.CPZ
                        
                LEFT JOIN (SELECT CPZ, count(*) as NUM_INSURED 
                               FROM (SELECT CPZ
                                     FROM L0_OWNER.PZ_DETAIL_CP_OSOBY_DAT 
                                     WHERE osobavztah = 'Poistený') 
                              GROUP BY CPZ) CP_OSOBY2
                        ON CONTRACT.CONT_ACCT= CP_OSOBY2.CPZ
                    
                LEFT JOIN 
                        (SELECT CPZ, count(*) as NUM_INSURED_CHILDREN,
                         sum(case when age_child < 13 then 1 else 0 end) as NUM_INSURED_SMALL_CHILDREN,
                         sum(case when age_child >= 13 then 1 else 0 end) as NUM_INSURED_TEEN_CHILDREN 
                         FROM 
                            (SELECT * FROM 
                                (SELECT CPZ, 
                                    EXTRACT(year FROM (process_dt))-(substr(kod_RC,1,2)+1900
                                        +100*(
                                        CASE WHEN substr(EXTRACT(year FROM (process_dt )),3,4) < substr (kod_RC,1,2 ) 
                                            THEN 0 
                                            ELSE 1 
                                            END)) AS age_child
                                        FROM L0_OWNER.PZ_DETAIL_CP_OSOBY_DAT a
                                        WHERE osobavztah = 'Poistený'
                                        and length(kod_rc)=10) deti
                                WHERE age_child < 18)
                            GROUP BY CPZ) CP_OSOBY3
                        ON CONTRACT.CONT_ACCT = CP_OSOBY3.CPZ
                
                LEFT JOIN 
                    (SELECT CPZ, count(*) as NUM_INSURED_SENIOR 
                       FROM (SELECT * FROM 
                            (SELECT cpz, EXTRACT(year FROM (process_dt))
                                       -(substr(kod_RC,1,2 )+1900+100*(
                                            CASE WHEN substr(EXTRACT(year FROM (process_dt )),3,4) < substr(kod_RC,1,2) 
                                                THEN 0 ELSE 1 END)) as age_senior
                                  FROM L0_OWNER.PZ_DETAIL_CP_OSOBY_DAT 
                                  WHERE osobavztah='Poistený')
                            WHERE age_senior>65) 
                    GROUP BY CPZ) CP_OSOBY4
                    ON CONTRACT.CONT_ACCT= CP_OSOBY4.CPZ
                    
                LEFT JOIN (select CPZ, HOUSE_INS_TYPE, REGION_INS_REALITY, FLG_LIABILITY, PACKAGE_TYPE, FLG_POOL 
                           from L0_OWNER.PZ_DETAIL_DOMOS_DETAIL_DAT) DOMOS
                    ON CONTRACT.CONT_ACCT =  DOMOS.CPZ 
                LEFT JOIN JE69050.AREA_CODE_GEN gen
                    ON gen.zip=DOMOS.REGION_INS_REALITY
                LEFT JOIN je69050.administrative_structure_svk admin 
                    ON gen.code_municipality = admin.kod_obce
                
                LEFT JOIN (SELECT * 
                           FROM 
                               (SELECT CPZ, popis, m2
                                FROM L0_OWNER.PZ_DETAIL_DOMOS_PLOCHY_DAT) 
                               PIVOT (sum(m2) FOR popis IN ('Pivnica (plocha)' as pivnica_plocha,
                                     'Garaz (plocha)' as garaz_plocha,
                                     'Domacnost podkrovie m2' as domacnost_podkrovie_m2,
                                     'Domacnost byt m2' as domacnost_byt_m2,
                                     'Povala (plocha)' as povala_plocha,
                                     'Vedlajsia budova (plocha)' as vedl_budova_plocha,
                                     'Domacnost garaz m2' as domacnost_garaz_m2,
                                     'Prizemie (plocha)' as prizemie_plocha,
                                     'Domacnost vedl. budova m2' as domacnost_vedl_budova_m2 ,
                                     'Domacnost prizemie m2' as domacnost_prizemie_m2,
                                     'Domacnost pivnica m2' as domacnost_pivnica_m2,
                                     'Podkrovie (plocha)' as podkrovie_plocha,
                                     'Podlazie (plocha)'as podlazie_plocha,
                                     'Domacnost povala m2' as domacnost_povala_m2,
                                     'Byt (plocha)' as byt_plocha,
                                     'Domacnost podlazie m2' as domacnost_podlazie_m2,
                                     'Stavba - obývaná plocha' as dom_obyvana_m2,
                                     'Stavba - neobývaná plocha' as dom_neobyvana_m2))) DOMOS_PLOCHA      
                    ON CONTRACT.CONT_ACCT = DOMOS_PLOCHA.CPZ 
                LEFT JOIN (SELECT * 
                           FROM 
                               (SELECT CPZ, popis, m2
                                FROM L0_OWNER.PZ_DETAIL_DOMOS_PLOCHY_DAT) 
                               PIVOT (sum(m2) FOR popis IN ('Pivnica (plocha)' as pivnica_plocha,
                                     'Garaz (plocha)' as garaz_plocha,
                                     'Domacnost podkrovie m2' as domacnost_podkrovie_m2,
                                     'Domacnost byt m2' as domacnost_byt_m2,
                                     'Povala (plocha)' as povala_plocha,
                                     'Vedlajsia budova (plocha)' as vedl_budova_plocha,
                                     'Domacnost garaz m2' as domacnost_garaz_m2,
                                     'Prizemie (plocha)' as prizemie_plocha,
                                     'Domacnost vedl. budova m2' as domacnost_vedl_budova_m2 ,
                                     'Domacnost prizemie m2' as domacnost_prizemie_m2,
                                     'Domacnost pivnica m2' as domacnost_pivnica_m2,
                                     'Podkrovie (plocha)' as podkrovie_plocha,
                                     'Podlazie (plocha)'as podlazie_plocha,
                                     'Domacnost povala m2' as domacnost_povala_m2,
                                     'Byt (plocha)' as byt_plocha,
                                     'Domacnost podlazie m2' as domacnost_podlazie_m2,
                                     'Stavba - obývaná plocha' as dom_obyvana_m2,
                                     'Stavba - neobývaná plocha' as dom_neobyvana_m2))) DOMOS_PS      
                    ON CONTRACT.CONT_ACCT = DOMOS_PS.CPZ 
                LEFT JOIN (
                    SELECT
                           C.cont_id,
                           MAX(CASE WHEN CV.meas_cd = 30654 THEN CV.cont_val_am ELSE NULL END) as PAYMENT_VALUE,
                           MAX(CASE WHEN CV.meas_cd = 30655 THEN CV.cont_val_am ELSE NULL END) as INSURANCE_PREMIUM,
                           MAX(CASE WHEN CV.meas_cd = 30656 THEN CV.cont_val_am ELSE NULL END) as CAPITAL_VALUE
                        FROM L0_OWNER.CONT C 
                        LEFT JOIN L0_OWNER.CONT_VAL CV
                           ON  CV.cont_id = C.cont_id
                           AND CV.cont_val_start_dt <=   process_dt 
                           AND CV.cont_val_end_dt >  process_dt
                           AND CV.meas_cd IN(30654, 30655, 30656)
                        LEFT JOIN L0_OWNER.CONT_DT_HIST CH
                           ON  CH.cont_id = C.cont_id
                           AND CH.cont_dt_start_dt <=  process_dt
                           AND CH.cont_dt_end_dt >  process_dt
                           AND CH.meas_cd IN(6022, 6023)
                        WHERE C.src_syst_cd = 790 --CSOB_POJIST_SR	Systémy ČSOB Poisťovňi, a.s.  SR
                           AND (C.cont_type_cd = 3 OR (C.cont_type_cd = 2 AND C.cont_single_acct_in = 'Y'))
                        GROUP BY C.cont_id
                        ) finukaz 
                    ON CONTRACT.cont_id = finukaz.cont_id 
                
                left join (select * from l0_owner.INSURANCE_UPOMIENKY_DAT 
                           where d_tlac_upom > ADD_MONTHS(process_dt, -1) 
                           and d_tlac_upom <= process_dt) upom
                ON contract.cont_acct = upom.kod_cislo_zmluvy            
                
                left join (select * from L0_OWNER.INSURANCE_RESEARCH_LPU_DAT 
                           where D_VZNIKU_PU > ADD_MONTHS(process_dt, -1)
                           and D_VZNIKU_PU  <= process_dt
                           and D_REVIDACIE> ADD_MONTHS(process_dt, -1) 
                           and D_REVIDACIE <= process_dt) pu
                on pu.C_POJ_SML=contract.CONT_ACCT
                
                where 1=1
                    AND contract.DT_NEGOTIATED_INS < process_dt
                    AND contract.DT_START_INS < process_dt
            );
    
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;
      
        INSERT INTO PM_OWNER.tmp_life_assured (
            SELECT CPZ, CUSTOMERID
            FROM L0_OWNER.PZ_DETAIL_LIFE_PERSON_DAT
            WHERE PERSON_TYPE = 'Life Assured'
            AND CUSTOMERID IS NOT NULL
        );
    
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;
        
        INSERT INTO PM_OWNER.tmp_life_assured_c
        (
            SELECT COMPANY.CPZ, ASSURED.CUSTOMERID
            FROM L0_OWNER.PZ_DETAIL_LIFE_PERSON_DAT COMPANY
            JOIN L0_OWNER.PZ_DETAIL_LIFE_PERSON_DAT ASSURED
            ON COMPANY.CPZ = ASSURED.CPZ
            WHERE COMPANY.PERSON_TYPE = 'Payer' 
            AND COMPANY.SEX = 'C'
            AND ASSURED.PERSON_TYPE = 'Life Assured'
            AND ASSURED.CUSTOMERID IS NOT NULL
        );
        
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;  
        
        -- ATTR_INSURANCE
        step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
        mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
        step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);
        
        INSERT INTO pm_owner.ATTR_INSURANCE
        (
           select by_customer.*
           
          , case when coalesce(by_customer.C_ACTIVE_INS, 0) > coalesce(prev_vals.C_ACTIVE_INS, 0) 
            then 1 else 0 end as I_OPENED_INS
          , case when coalesce(by_customer.C_ACTIVE_INS, 0) > coalesce(prev_vals.C_ACTIVE_INS, 0) 
            then coalesce(by_customer.C_ACTIVE_INS, 0) - coalesce(prev_vals.C_ACTIVE_INS, 0) else 0 end as C_OPENED_INS
          , by_customer.SUM_PAYOUT_INS_M + coalesce(agg_prev_two_m.SUM_PAYOUT_INS, 0) as SUM_PAYOUT_INS_Q
          , by_customer.SUM_PAYOUT_INS_M + coalesce(agg_prev_y.SUM_PAYOUT_INS, 0) as SUM_PAYOUT_INS_Y
          , by_customer.C_CLAIM_INS_M + coalesce(agg_prev_two_m.C_CLAIM_INS, 0) as C_CLAIM_INS_Q
          , by_customer.C_CLAIM_INS_M + coalesce(agg_prev_y.C_CLAIM_INS, 0) as C_CLAIM_INS_Y
          , greatest(least(by_customer.C_CLAIM_DENIED_INS_M, 1), coalesce(agg_prev_two_m.C_CLAIM_DENIED_INS_Q, 0)) as I_CLAIM_DENIED_INS_Q
          , greatest(least(by_customer.C_CLAIM_DENIED_INS_M, 1), coalesce(agg_prev_y.C_CLAIM_DENIED_INS_Y, 0)) as I_CLAIM_DENIED_INS_Y
          , greatest(by_customer.I_WORK_TRAVEL_INS, coalesce(agg_prev_two_m.I_WORK_TRAVEL_INS_Q, 0)) as I_WORK_TRAVEL_INS_Q
          , greatest(by_customer.I_WORK_TRAVEL_INS, coalesce(agg_prev_y.I_WORK_TRAVEL_INS_Y, 0)) as I_WORK_TRAVEL_INS_Y
          , case when prev_val_q.SUM_PAYMENT_YEAR_INS <> 0
                 then (by_customer.SUM_PAYMENT_YEAR_INS -  prev_val_q.SUM_PAYMENT_YEAR_INS) / prev_val_q.SUM_PAYMENT_YEAR_INS
                 else 0 end as REL_CHANGE_PAYMENT_YEAR_INS_Q
                 
          , greatest(case when by_customer.c_opened_RISK_LIFE_INS + by_customer.c_opened_invest_life_ins
                              + by_customer.c_opened_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_two_m.I_OPENED_LIFE_INS_Q, 0)) as I_OPENED_LIFE_INS_Q
          , greatest(case when by_customer.c_opened_TRAVEL_INS + by_customer.c_opened_MTPL_ins
                              + by_customer.c_opened_CASCO_ins + by_customer.c_opened_household_property_ins
                              + by_customer.c_opened_card_travel_ins + by_customer.c_opened_accident_vehicle_ins
                              + by_customer.c_opened_business_risk_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_two_m.I_OPENED_NONLIFE_INS_Q, 0)) as I_OPENED_NONLIFE_INS_Q
          , greatest(case when by_customer.c_opened_RISK_LIFE_INS + by_customer.c_opened_invest_life_ins
                               + by_customer.c_opened_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_y.I_OPENED_LIFE_INS_Y, 0)) as I_OPENED_LIFE_INS_Y
          , greatest(case when by_customer.c_opened_TRAVEL_INS + by_customer.c_opened_MTPL_ins
                              + by_customer.c_opened_CASCO_ins + by_customer.c_opened_household_property_ins
                              + by_customer.c_opened_card_travel_ins + by_customer.c_opened_accident_vehicle_ins
                              + by_customer.c_opened_business_risk_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_y.I_OPENED_NONLIFE_INS_Y, 0)) as I_OPENED_NONLIFE_INS_Y
                    
          , greatest(case when by_customer.c_RISK_LIFE_INS + by_customer.c_invest_life_ins
                               + by_customer.c_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_two_m.I_LIFE_INS_Q, 0)) as I_LIFE_INS_Q
          , greatest(case when by_customer.c_active_ins - by_customer.c_RISK_LIFE_INS - by_customer.c_invest_life_ins
                               - by_customer.c_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_two_m.I_NONLIFE_INS_Q, 0)) as I_NONLIFE_INS_Q
          ,  greatest(case when by_customer.c_RISK_LIFE_INS + by_customer.c_invest_life_ins
                               + by_customer.c_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_y.I_LIFE_INS_Y, 0)) as I_LIFE_INS_Y
          , greatest(case when by_customer.c_active_ins - by_customer.c_RISK_LIFE_INS - by_customer.c_invest_life_ins
                               - by_customer.c_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_y.I_NONLIFE_INS_Y, 0)) as I_NONLIFE_INS_Y
          
          , greatest(by_customer.I_REMINDER_INS_M, coalesce(agg_prev_two_m.I_REMINDER_INS_Q, 0)) as I_REMINDER_INS_Q
          , greatest(by_customer.I_REMINDER_INS_M, coalesce(agg_prev_y.I_REMINDER_INS_Y, 0)) as I_REMINDER_INS_Y
          , greatest(by_customer.I_OWED_INS_M, coalesce(agg_prev_two_m.I_OWE_INS_Q, 0)) as I_OWED_INS_Q
          , greatest(by_customer.I_OWED_INS_M, coalesce(agg_prev_y.I_OWE_INS_Y, 0))   as I_OWED_INS_Y
          , by_customer.SUM_DURATION_TRAVEL_INS + coalesce(agg_prev_y.SUM_DURATION_TRAVEL_INS_Y, 0) as SUM_DURATION_TRAVEL_INS_Y
          
          , coalesce(key_employee.I_KEY_EMPLOYEE_INS, 0) as I_KEY_EMPLOYEE_INS
          , coalesce(life_assured.I_LIFE_ASSURED, 0) as I_LIFE_ASSURED_INS
    
        from
        (
                select 
                min(b.month_id) as MONTH_ID
                , b.customerid
                , max(coalesce(insur.ACTIVE, 0)) as I_ACTIVE_INS
                , sum(coalesce(insur.ACTIVE, 0)) as C_ACTIVE_INS
                
                , least(min(coalesce(insur.DAYS_FROM_START, process_dt -process_dt_hist)), process_dt -process_dt_hist) as RECENCY_START_INS
                , least(max(case when insur.DAYS_TO_END > 0 then insur.DAYS_TO_END else process_dt -process_dt_hist end), process_dt - process_dt_hist) as DAY_END_INS
                
                , least(max(case when insur.DAYS_TO_END < 0 then -1 * insur.DAYS_TO_END else process_dt -process_dt_hist end), process_dt - process_dt_hist) as RECENCY_END_INS
                , least(min(coalesce(case when insur.DAYS_FROM_CANCELED >= 0 then insur.DAYS_FROM_CANCELED else null end, process_dt - process_dt_hist)),
                        process_dt -process_dt_hist) as RECENCY_CANCELED_INS
                , least(min(coalesce(case when insur.DAYS_FROM_PAYOUT >= 0 then insur.DAYS_FROM_PAYOUT else null end, process_dt -process_dt_hist)), process_dt - process_dt_hist) as RECENCY_PAYOUT_INS
                
                , count(distinct insur.cont_acct) as C_CONT_INS
                , sum(coalesce(insur.i_canceled, 0)) as C_CANCELED_TOTAL_INS
                , max(coalesce(insur.i_canceled, 0)) as I_CANCELED_INS
                
                
                , sum(coalesce(insur.household_property_INS_WEST, 0)) as C_household_property_WEST_INS
                , sum(coalesce(insur.household_property_ins_MIDDLE, 0)) as C_household_property_CENTRAL_INS
                , sum(coalesce(insur.household_property_ins_EAST, 0)) as C_household_property_EAST_INS
                , sum(coalesce(insur.household_property_ins_CAPITAL, 0)) as C_household_property_CAPITAL_INS
                
                , SUM(NVL(insur.domacnost_podkrovie_m2,0) + NVL(insur.domacnost_byt_m2,0) 
                              + NVL(insur.domacnost_garaz_m2,0) + NVL(insur.domacnost_vedl_budova_m2,0)
                              + NVL(insur.domacnost_prizemie_m2,0) + NVL(insur.domacnost_pivnica_m2,0)
                              + NVL(insur.domacnost_povala_m2,0) + NVL(insur.domacnost_podlazie_m2,0)
                         ) as SUM_AREA_HOUSEHOLD_INS
                         
                , MAX(NVL(insur.domacnost_podkrovie_m2,0) + NVL(insur.domacnost_byt_m2,0) + NVL(insur.domacnost_garaz_m2,0)
                          + NVL(insur.domacnost_vedl_budova_m2,0) + NVL(insur.domacnost_prizemie_m2,0) + NVL(insur.domacnost_pivnica_m2,0)
                          + NVL(insur.domacnost_povala_m2,0) + NVL(insur.domacnost_podlazie_m2,0)
                          ) as MAX_AREA_HOUSEHOLD_INS
                          
                , SUM(NVL(insur.dom_obyvana_m2,0) + NVL(insur.dom_neobyvana_m2,0) ) as SUM_AREA_HOUSE_INS                    
                , MAX(NVL(insur.dom_obyvana_m2,0) + NVL(insur.dom_neobyvana_m2,0) ) as MAX_AREA_HOUSE_INS
                          
                , SUM(NVL(insur.pivnica_plocha,0) + NVL(insur.garaz_plocha,0) + NVL(insur.povala_plocha,0)
                          + NVL(insur.vedl_budova_plocha,0) + NVL(insur.prizemie_plocha,0) + NVL(insur.podkrovie_plocha,0)
                          + NVL(insur.podlazie_plocha,0) + NVL(insur.byt_plocha,0) + NVL(insur.dom_obyvana_m2,0) + NVL(insur.dom_neobyvana_m2,0)
                          ) AS SUM_AREA_INS
                          
                , MAX(NVL(insur.pivnica_plocha,0) + NVL(insur.garaz_plocha,0) + NVL(insur.povala_plocha,0) 
                          + NVL(insur.vedl_budova_plocha,0) + NVL(insur.prizemie_plocha,0) + NVL(insur.podkrovie_plocha,0) 
                          + NVL(insur.podlazie_plocha,0) + NVL(insur.byt_plocha,0) + NVL(insur.dom_obyvana_m2,0) + NVL(insur.dom_neobyvana_m2,0)
                          ) AS MAX_AREA_INS
                
                , sum(coalesce(insur.HLASENIE_PU, 0)) as C_CLAIM_INS_M
                , sum(coalesce(insur.VZNIK_PU, 0)) as C_EVENT_INS_M
                , sum(coalesce(insur.NELIKVIDNA_PU, 0)) as C_CLAIM_DENIED_INS_M
                
                , sum(case when insur.DAYS_TO_END > 0 and insur.active = 1 then insur.LEASING else 0 end) as C_LEASING_INS
                , max(coalesce(insur.reminder, 0)) as I_REMINDER_INS_M
                
                , sum(case when coalesce(insur.PLNENIE_PU, 0) > 0 then 1 else 0 end) as C_PAYOUT_INS_M
                , sum(coalesce(insur.PLNENIE_PU, 0)) as SUM_PAYOUT_INS_M
                
                , sum(coalesce(insur.I_VEHICLE_EVENT_INS, 0)) as C_VEHICLE_EVENT_INS
                , sum(coalesce(insur.I_HOUSEHOLD_PROPERTY_EVENT_INS, 0)) as C_HOUSEHOLD_PROPERTY_EVENT_INS
                , sum(coalesce(insur.I_LIFE_EVENT_INS, 0)) as C_LIFE_EVENT_INS
                
                , max(coalesce(insur.NUM_INSURED, 0)) as MAX_C_PERSON_INS
                , max(coalesce(insur.NUM_INSURED_SENIOR, 0)) as MAX_C_SENIOR_INS
                , max(coalesce(insur.NUM_INSURED_CHILDREN, 0)) as MAX_C_CHILD_INS
                , max(coalesce(insur.NUM_INSURED_SMALL_CHILDREN, 0)) as MAX_C_SMALL_CHILD_INS
                , max(coalesce(insur.NUM_INSURED_TEEN_CHILDREN, 0)) as MAX_C_TEEN_INS
                
                , avg(coalesce(insur.NUM_INSURED, 0)) as AVG_C_PERSON_INS
                
                , SUM(case when insur.active = 1 then coalesce(insur.household_property_ins_type0, 0) else 0 end ) as c_domos_type_0_ins
                , SUM(case when insur.active = 1 then coalesce(insur.household_property_ins_type1, 0) else 0 end ) as c_domos_type_1_ins
                , SUM(case when insur.active = 1 then coalesce(insur.household_property_ins_type2, 0) else 0 end ) as c_domos_type_2_ins
                , SUM(case when insur.active = 1 then coalesce(insur.household_property_ins_type3, 0) else 0 end ) as c_domos_type_3_ins
                    
                -- active insurances
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.TRAVEL_INS else 0 end) as C_TRAVEL_INS
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.MTPL_INS else 0 end) as C_MTPL_INS
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.CASCO_INS else 0 end) as C_CASCO_INS
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.household_property_ins else 0 end) as C_household_property_ins
                --, sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.loan_ins else 0 end) as C_LOAN_INS
                , max(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.reg_expenses_ins else 0 end) as C_REG_EXPENSES_INS
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.card_travel_ins else 0 end) as C_CARD_TRAVEL_INS
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.drivers_accident_ins else 0 end) as C_ACCIDENT_VEHICLE_INS
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.business_risk_ins else 0 end) as C_BUSINESS_RISK_INS
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.RISK_LIFE_INS else 0 end) as C_RISK_LIFE_INS
                , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.INVEST_LIFE_INS else 0 end) as C_INVEST_LIFE_INS
                
                -- closed insurances
                , sum(case when insur.ACTIVE = 0 then insur.TRAVEL_INS else 0 end) as C_CLOSED_TRAVEL_INS
                , sum(case when insur.ACTIVE = 0 then insur.MTPL_INS else 0 end) as C_CLOSED_MTPL_INS
                , sum(case when insur.ACTIVE = 0 then insur.CASCO_INS else 0 end) as C_CLOSED_CASCO_INS
                , sum(case when insur.ACTIVE = 0 then insur.household_property_ins else 0 end) as C_CLOSED_household_property_ins
                --, sum(case when insur.ACTIVE = 0 then insur.loan_ins else 0 end) as C_CLOSED_LOAN_INS
                , max(case when insur.ACTIVE = 0 then insur.reg_expenses_ins else 0 end) as C_CLOSED_REG_EXPENSES_INS
                , sum(case when insur.ACTIVE = 0 then insur.card_travel_ins else 0 end) as C_CLOSED_CARD_TRAVEL_INS
                , sum(case when insur.ACTIVE = 0 then insur.drivers_accident_ins else 0 end) as C_CLOSED_ACCIDENT_VEHICLE_INS
                , sum(case when insur.ACTIVE = 0 then insur.business_risk_ins else 0 end) as C_CLOSED_BUSINESS_RISK_INS
                , sum(case when insur.ACTIVE = 0 then insur.RISK_LIFE_INS else 0 end) as C_CLOSED_RISK_LIFE_INS
                , sum(case when insur.ACTIVE = 0 then insur.INVEST_LIFE_INS else 0 end) as C_CLOSED_INVEST_LIFE_INS
                
                -- opened insurances
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.TRAVEL_INS else 0 end) as C_OPENED_TRAVEL_INS
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.MTPL_INS else 0 end) as C_OPENED_MTPL_INS
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.CASCO_INS else 0 end) as C_OPENED_CASCO_INS
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.household_property_ins else 0 end) as C_OPENED_household_property_ins
                --, sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.loan_ins else 0 end) as C_OPENED_LOAN_INS
                , max(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.reg_expenses_ins else 0 end) as C_OPENED_REG_EXPENSES_INS
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.card_travel_ins else 0 end) as C_OPENED_CARD_TRAVEL_INS
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.drivers_accident_ins else 0 end) as C_OPENED_ACCIDENT_VEHICLE_INS
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.business_risk_ins else 0 end) as C_OPENED_BUSINESS_RISK_INS
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.RISK_LIFE_INS else 0 end) as C_OPENED_RISK_LIFE_INS
                , sum(case when 0 < insur.days_from_start AND insur.days_from_start < process_dt - ADD_MONTHS(process_dt, -1) then insur.INVEST_LIFE_INS else 0 end) as C_OPENED_INVEST_LIFE_INS
                
                , sum(coalesce(case when insur.active = 1 then insur.PAYMENT_YEAR else 0 end, 0)) as SUM_PAYMENT_YEAR_INS
                , sum(case when insur.active = 1 and insur.I_NONLIFE_INS = 0 then coalesce(insur.PAYMENT_YEAR, 0) else 0 end) as SUM_PAYMENT_YEAR_LIFE_INS
                , sum(case when insur.active = 1 and insur.I_NONLIFE_INS = 1 then coalesce(insur.PAYMENT_YEAR, 0) else 0 end) as SUM_PAYMENT_YEAR_NONLIFE_INS
                , sum(case when insur.active = 1 then coalesce(insur.num_payment_year, 0) else 0 end) as C_PAYMENT_YEAR_INS
                , coalesce(count(distinct insur.prod_src_val), 0) as C_PROD_INS
                
                 ----payments
                , SUM(CASE WHEN (insur.travel_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_travel_INS
                , SUM(CASE WHEN (insur.mtpl_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_MTPL_INS
                , SUM(CASE WHEN (insur.casco_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_casco_INS
                , SUM(CASE WHEN (insur.household_property_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_household_property_ins
                --, SUM(CASE WHEN (insur.loan_ins = 1 AND insur.days_to_end>0)
                --           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_loan_INS
                , SUM(CASE WHEN (insur.reg_expenses_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_reg_expenses_INS
                , SUM(CASE WHEN (insur.card_travel_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_card_travel_INS
                , SUM(CASE WHEN (insur.drivers_accident_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_accident_vehicle_INS
                , SUM(CASE WHEN (insur.business_risk_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_business_risk_INS
                , SUM(CASE WHEN (insur.risk_life_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_risk_life_INS
                , SUM(CASE WHEN (insur.invest_life_ins = 1 AND insur.active=1)
                           THEN NVL(insur.payment_year,0) ELSE 0 END) AS payment_invest_life_INS
                
                ----time_to_end
                , LEAST(MIN(CASE WHEN (insur.travel_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0) 
                            THEN insur.days_to_end else process_dt - process_dt_hist end), process_dt - process_dt_hist) as D_END_TRAVEL_INS
                , LEAST(MIN(CASE WHEN (insur.mtpl_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0) 
                            THEN insur.days_to_end else process_dt - process_dt_hist end), process_dt - process_dt_hist) as D_END_MTPL_INS
                , LEAST(MIN(CASE WHEN (insur.casco_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0) 
                            THEN insur.days_to_end else process_dt - process_dt_hist end), process_dt - process_dt_hist) as D_END_CASCO_INS
                , LEAST(MIN(CASE WHEN (insur.reg_expenses_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                            THEN insur.days_to_end else process_dt - process_dt_hist end), process_dt - process_dt_hist) as D_END_REG_EXPENSES_INS
                , LEAST(MIN(CASE WHEN (insur.card_travel_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0) 
                            THEN insur.days_to_end else process_dt - process_dt_hist end), process_dt - process_dt_hist) as D_END_CARD_TRAVEL_INS
                , LEAST(MIN(CASE WHEN (insur.business_risk_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0) 
                            THEN insur.days_to_end else process_dt - process_dt_hist end), process_dt - process_dt_hist) as D_END_BUSINESS_RISK_INS
                , LEAST(MIN(CASE WHEN (insur.risk_life_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0) 
                            THEN insur.days_to_end else process_dt - process_dt_hist end), process_dt - process_dt_hist) as D_END_RISK_LIFE_INS
                , LEAST(MIN(CASE WHEN (insur.invest_life_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0) 
                            THEN insur.days_to_end else process_dt - process_dt_hist end), process_dt - process_dt_hist) as D_END_INVEST_LIFE_INS
                            
                    ----time_since_opened
                , MIN(CASE WHEN (insur.travel_ins = 1) 
                            THEN least(insur.DAYS_FROM_START, process_dt - process_dt_hist) else process_dt -process_dt_hist end) as RECENCY_TRAVEL_INS
                , MIN(CASE WHEN (insur.mtpl_ins = 1) 
                            THEN least(insur.DAYS_FROM_START, process_dt - process_dt_hist) else process_dt -process_dt_hist end) as RECENCY_MTPL_INS
                , MIN(CASE WHEN (insur.household_property_ins = 1) 
                            THEN least(insur.DAYS_FROM_START, process_dt - process_dt_hist) else process_dt -process_dt_hist end) as RECENCY_HOUSEHOLD_PROPERTY_INS
    
                                
                , max(coalesce(insur.flg_pool, 0)) as I_POOL_INS
                , max(coalesce(insur.FLG_LIABILITY, 0)) as I_LIABILITY_INS
                
                , sum(case when insur.active = 1 then coalesce(insur.CAPITAL_VALUE, 0) else 0 end) as SUM_CAPITAL_VALUE_INS
                , SUM(CASE WHEN (insur.active = 1 and insur.num_payment_year=1) THEN 1 else 0 end) as  C_YEARLY_PAYMENT_INS
                , SUM(CASE WHEN (insur.active = 1 and insur.num_payment_year=2) THEN 1 else 0 end) as  C_BI_YEARLY_PAYMENT_INS
                , SUM(CASE WHEN (insur.active = 1 and insur.num_payment_year=4) THEN 1 else 0 end) as  C_QUARTERLY_PAYMENT_INS
                , SUM(CASE WHEN (insur.active = 1 and insur.num_payment_year=12) THEN 1 else 0 end) as  C_MONTHLY_PAYMENT_INS
                
                , case when (SUM(coalesce(insur.travel_ins, 0)) + SUM(coalesce(insur.card_travel_ins, 0))) = 0
                           THEN 0 
                           else SUM(coalesce(insur.travel_ins_luggage_amt,0))
                                / (SUM(insur.travel_ins) + SUM(insur.card_travel_ins)) end as AVG_LUGGAGE_AMT_TRAVEL_INS                            
                , SUM(case when (insur.travel_ins = 1 
                                 and process_dt - insur.DAYS_FROM_START <= process_dt 
                                 and (process_dt - insur.DAYS_FROM_START + insur.duration_travel_ins) > ADD_MONTHS(process_dt, -1) )
                           THEN insur.duration_travel_ins
                           else 0 end) as SUM_DURATION_TRAVEL_INS
                
                , sum(case when insur.active = 1 then coalesce(insur.TRAVEL_INS_EUROPE, 0) else 0 end) as C_EUROPE_TRAVEL_INS
                , sum(coalesce(insur.travel_ins_tourist, 0)) as C_TOURIST_TRAVEL_INS
                , max(coalesce(insur.travel_ins_risksport, 0)) as I_RISKSPORT_TRAVEL_INS
                
                , greatest(max(case when insur.active = 1 then coalesce(insur.travel_ins_manual_labor, 0) else 0 end),
                           max(case when insur.active = 1 then coalesce(insur.travel_ins_nonmanual_labor, 0) else 0 end)) as I_WORK_TRAVEL_INS
                , max(coalesce(insur.travel_ins_family, 0)) as I_family_travel_ins
                , max(coalesce(insur.travel_ins_exclusive, 0)) as I_exclusive_travel_ins
                
                , sum(case when insur.active = 1 then coalesce(insur.sum_owed, 0) else 0 end) as SUM_OWED_INS_M
                , max(case when insur.active = 1 and coalesce(insur.sum_owed, 0) > 0 then 1 else 0 end) as I_OWED_INS_M
                
                from
                ( -- client base from attr_client_base, includes only bank clients. insurance clients which are not bank clients are excluded
                    select month_id, customerid
                    from pm_owner.attr_client_base
                    where month_id = to_number(to_char(process_dt, 'YYYYMM'))
                ) b
                LEFT JOIN PM_OWNER.tmp_insurance_cont insur
                ON b.customerid = insur.customerid
                group by b.customerid
            ) by_customer
            LEFT JOIN (
                        SELECT /*+ USE_HASH(CONT TMP)*/ TMP.customerid,
                        (case when count(*) > 0 then 1 else 0 end) as I_KEY_EMPLOYEE_INS
                        FROM pm_owner.stg_insurance_cont CONT
                        JOIN PM_OWNER.tmp_life_assured_c TMP
                        ON CONT.CONT_ACCT = TMP.CPZ
                        AND CONT.ACTIVE = 1
                        AND CONT.DT_START_INS <= process_dt
                        AND (CONT.DT_CANCELED_INS is null OR CONT.DT_CANCELED_INS > process_dt)
                        AND CONT.DT_END_INS > process_dt
                        GROUP BY TMP.customerid
            ) key_employee
            ON by_customer.customerid = key_employee.customerid
            LEFT JOIN (
                        SELECT /*+ USE_HASH(CONT ASSURED)*/ ASSURED.customerid,
                        (case when count(*) > 0 then 1 else 0 end) as I_LIFE_ASSURED
                        FROM pm_owner.stg_insurance_cont CONT
                        JOIN PM_OWNER.tmp_life_assured ASSURED
                        ON CONT.CONT_ACCT = ASSURED.CPZ
                        AND CONT.ACTIVE = 1
                        AND CONT.DT_START_INS <= process_dt
                        AND CONT.DT_END_INS > process_dt
                        AND (CONT.DT_CANCELED_INS is null or CONT.DT_CANCELED_INS > process_dt)
                        GROUP BY ASSURED.customerid
            ) life_assured
            ON by_customer.customerid = life_assured.customerid
            left join(
                    select customerid, C_ACTIVE_INS
                    from pm_owner.attr_insurance
                    where month_id = to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
            ) prev_vals
            ON by_customer.customerid = prev_vals.customerid
            left join(
                    select customerid, SUM_PAYMENT_YEAR_INS
                    from pm_owner.attr_insurance
                    where month_id = to_number(to_char(ADD_MONTHS(process_dt, -3), 'YYYYMM'))
            ) prev_val_q
            ON by_customer.customerid = prev_val_q.customerid
            left join(select customerid, sum(SUM_PAYOUT_INS_M) as SUM_PAYOUT_INS, sum(C_CLAIM_INS_M) as C_CLAIM_INS,
                              max(I_WORK_TRAVEL_INS) as I_WORK_TRAVEL_INS_Q,
                              max(I_OWED_INS_M) as I_OWE_INS_Q,
                              sum(C_CLAIM_DENIED_INS_M) as C_CLAIM_DENIED_INS_Q,
                              max(I_REMINDER_INS_M) as I_REMINDER_INS_Q,
                              case when sum(c_opened_RISK_LIFE_INS) + sum(c_opened_invest_life_ins)
                                         + sum(c_opened_reg_expenses_ins) > 0 then 1
                                   else 0 end as I_OPENED_LIFE_INS_Q,
                              case when sum(c_opened_ins) - sum(c_opened_RISK_LIFE_INS) - sum(c_opened_invest_life_ins)
                                         - sum(c_opened_reg_expenses_ins) > 0 then 1
                                   else 0 end as I_OPENED_NONLIFE_INS_Q,
                             case when sum(c_active_ins) - sum(c_RISK_LIFE_INS) - sum(c_invest_life_ins)
                                         - sum(c_reg_expenses_ins) > 0 then 1
                                   else 0 end as I_NONLIFE_INS_Q,
                             case when sum(c_RISK_LIFE_INS) + sum(c_invest_life_ins)
                                        + sum(c_reg_expenses_ins) > 0 then 1
                                   else 0 end as I_LIFE_INS_Q
                      from pm_owner.attr_insurance
                      where month_id >= to_number(to_char(ADD_MONTHS(process_dt, -2), 'YYYYMM'))
                      and month_id <= to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
                      group by customerid
            ) agg_prev_two_m
            ON by_customer.customerid = agg_prev_two_m.customerid
            left join(select customerid, sum(SUM_PAYOUT_INS_M) as SUM_PAYOUT_INS, sum(C_CLAIM_INS_M) as C_CLAIM_INS,
                             max(I_WORK_TRAVEL_INS) as I_WORK_TRAVEL_INS_Y,
                             sum(C_CLAIM_DENIED_INS_M) as C_CLAIM_DENIED_INS_Y,
                             max(I_REMINDER_INS_M) as I_REMINDER_INS_Y,
                             max(I_OWED_INS_M) as I_OWE_INS_Y,
                             case when sum(c_opened_RISK_LIFE_INS) + sum(c_opened_invest_life_ins)
                                        + sum(c_reg_expenses_ins) > 0 then 1
                                   else 0 end as I_OPENED_LIFE_INS_Y,
                            case when sum(c_opened_ins) - sum(c_opened_RISK_LIFE_INS) - sum(c_opened_invest_life_ins)
                                        - sum(c_reg_expenses_ins) > 0 then 1
                                   else 0 end as I_OPENED_NONLIFE_INS_Y,
                            case when sum(c_active_ins) - sum(c_RISK_LIFE_INS) - sum(c_invest_life_ins)
                                        - sum(c_reg_expenses_ins) > 0 then 1
                                   else 0 end as I_NONLIFE_INS_Y,
                           case when sum(c_RISK_LIFE_INS) + sum(c_invest_life_ins)
                                       + sum(c_reg_expenses_ins) > 0 then 1
                                   else 0 end as I_LIFE_INS_Y,
                           SUM(SUM_DURATION_TRAVEL_INS) as SUM_DURATION_TRAVEL_INS_Y
                            --max(I_OPENED_INS) as I_OPENED_INS_Y
                      from pm_owner.attr_insurance
                      where month_id >= to_number(to_char(ADD_MONTHS(process_dt, -11), 'YYYYMM'))
                      and month_id <= to_number(to_char(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
                      group by customerid
            )
            agg_prev_y
            ON by_customer.customerid = agg_prev_y.customerid
        )
        ;
        
        ETL_OWNER.etl_dbms_util.truncate_table('tmp_insurance_cont',l_owner_name);
        ETL_OWNER.etl_dbms_util.truncate_table('tmp_life_assured',l_owner_name);
        ETL_OWNER.etl_dbms_util.truncate_table('tmp_life_assured_c',l_owner_name);
       
        
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;              
        mng_stats(l_owner_name, l_table_name, l_proc_name);
        
      END;
    
  
    PROCEDURE append_ATTR_INTERACTION(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
        l_proc_name varchar2(255) := 'append_ATTR_INTERACTION';
        l_owner_name varchar2(30) := 'PM_OWNER';
        l_table_name varchar2(30) := 'ATTR_INTERACTION';
        l_month_start_dt    date  := trunc(process_dt,'MM');
        l_month_end_dt      date  := last_day(process_dt);
        initial_load_dt		date  := DATE '2019-01-01'; -- Date of initial load, necessary for computation of complete CUSTOMER_CONTACT
        min_contct_start    date  := DATE '2005-01-01'; -- Only contacts started from this date are included
        
      BEGIN
                  
        step(p_stepname => l_proc_name || ' DELETE FROM CALL_CENTRUM_LOG', p_source => 'PM_OWNER.*', p_target => 'CALL_CENTRUM_LOG');   
        DELETE FROM PM_OWNER.CALL_CENTRUM_LOG
        WHERE 1=1
            AND event_dt < process_dt + 1
            AND event_dt >= ADD_MONTHS(process_dt + 1, -1);
        COMMIT;
    
        DELETE FROM PM_OWNER.CALL_CENTRUM_HISTORY
        WHERE 1=1
            AND event_dt < process_dt + 1
            AND event_dt >= ADD_MONTHS(process_dt + 1, -1);
        COMMIT;
    
        DELETE FROM PM_OWNER.FEEDBACK_EVALUATION
        WHERE 1=1
            AND event_dt < process_dt + 1
            AND event_dt >= ADD_MONTHS(process_dt + 1, -1);
        COMMIT;

        step(p_stepname => l_proc_name || ' INSERT CALL_CENTRUM_LOG', p_source => 'N/A', p_target => 'CALL_CENTRUM_LOG');         
        INSERT INTO PM_OWNER.CALL_CENTRUM_LOG
    
            WITH CC_LOG AS (
                SELECT DISTINCT
                    a.MEDIA_TYPE,
                    a.start_timestamp AS event_dt,
                    a.interaction_type,
                    COALESCE( -- If total_duration > 31 days or NULL, fill column with respective median
                        DECODE(SIGN(2678400 - a.total_duration),
                            1, a.total_duration),
                        DECODE(a.media_type,
                            'Voice', 140,
                            'Email', 7500),
                    0) as total_duration,
                    CASE
                        WHEN SUBSTR(a.FROMM, -9, 1) = '9' AND SUBSTR(a.FROMM, 1, GREATEST(1, (length(a.FROMM)-9))) in ('0', '00', '000', '0000', '00000') THEN '+421' || SUBSTR(a.FROMM, -9, 9) --mobil SK
                        WHEN SUBSTR(a.FROMM, -9, 1) in ('2', '3', '4', '5') AND SUBSTR(a.FROMM, 1, GREATEST(1, (length(a.FROMM)-9))) in ('0', '00', '000', '0000', '00000') THEN '+421' || SUBSTR(a.FROMM, -9, 9) --pevna linka SK
                        --                       
                        WHEN SUBSTR(a.FROMM, 1, 4) = '+420' AND LENGTH(SUBSTR(a.FROMM, 5)) = 9 THEN a.FROMM -- mobil CZ
                        WHEN SUBSTR(a.FROMM, 1, 5) = '00420' AND LENGTH(SUBSTR(a.FROMM, 6)) = 9 THEN '+420' || SUBSTR(a.FROMM, -9, 9) --mobil CZ
                        WHEN LENGTH(a.FROMM) in (14, 15) AND c.DIALLING_CODE is not null AND SUBSTR(a.FROMM,1,3)='000'  THEN '+' || SUBSTR(FROMM,4,14)
                        WHEN LENGTH(a.FROMM)=14 AND c.DIALLING_CODE is not null AND SUBSTR(a.FROMM,1,2)='00'   THEN '+' || SUBSTR(FROMM,3,14)
                        WHEN LENGTH(a.FROMM)=16 AND c.DIALLING_CODE is not null AND SUBSTR(a.FROMM,1,3)='000' AND SUBSTR(a.FROMM,4,1)!='0' THEN '+' || SUBSTR(FROMM,4,14)
                        WHEN LENGTH(a.FROMM)=16 AND c.DIALLING_CODE is not null AND SUBSTR(a.FROMM,1,4)='0000' THEN '+' || SUBSTR(FROMM,5,12) 
                        WHEN LENGTH(a.FROMM)=4 THEN a.FROMM
                        WHEN a.FROMM like '%@%' THEN a.FROMM
                        ELSE NULL
                    END FROMM_FINAL,
                    CASE           
                        WHEN SUBSTR(a.TOO, -9, 1) = '9' AND SUBSTR(a.TOO, 1, GREATEST(1, (length(a.TOO)-9))) in ('0', '00', '000', '0000', '00000') THEN '+421' || SUBSTR(a.TOO, -9, 9) --mobil SK
                        WHEN SUBSTR(a.TOO, -9, 1) in ('2', '3', '4', '5') AND SUBSTR(a.TOO, 1, GREATEST(1, (length(a.TOO)-9))) in ('0', '00', '000', '0000', '00000') THEN '+421' || SUBSTR(a.TOO, -9, 9) --pevna linka SK
                        --
                        WHEN SUBSTR(a.TOO, 1, 4) = '+420' AND LENGTH(SUBSTR(a.TOO, 5)) = 9 THEN a.TOO -- mobil CZ
                        WHEN SUBSTR(a.TOO, 1, 5) = '00420' AND LENGTH(SUBSTR(a.TOO, 6)) = 9 THEN '+420' || SUBSTR(a.TOO, -9, 9) --mobil CZ
                        WHEN LENGTH(a.TOO) in (14, 15) AND e.DIALLING_CODE is not null AND SUBSTR(a.TOO,1,3)='000'  THEN '+' || SUBSTR(TOO,4,14)
                        WHEN LENGTH(a.TOO)=14 AND e.DIALLING_CODE is not null AND SUBSTR(a.TOO,1,2)='00'   THEN '+' || SUBSTR(TOO,3,14)
                        WHEN LENGTH(a.TOO)=16 AND e.DIALLING_CODE is not null AND SUBSTR(a.TOO,1,3)='000' AND SUBSTR(a.TOO,4,1)!='0' THEN '+' || SUBSTR(TOO,4,14)
                        WHEN LENGTH(a.TOO)=16 AND e.DIALLING_CODE is not null AND SUBSTR(a.TOO,1,4)='0000' THEN '+' || SUBSTR(TOO,5,12)
                        WHEN LENGTH(a.TOO)=4 THEN a.TOO
                        WHEN a.TOO like '%@%' THEN a.TOO
                        ELSE NULL
                    END as TOO_FINAL,
                    technical_result
                FROM L0_OWNER.CALL_CENTRUM_LOG a
                --INBOUND
                --medzinarodne predvolby
                LEFT JOIN UNICA_OWNER.UNICA_LIST_INT_DIAL_CODES c
                ON SUBSTR(CASE
                    WHEN SUBSTR(a.FROMM,1,3)='000'  AND LENGTH(a.FROMM) in (14, 15) THEN SUBSTR(a.FROMM,4,14)
                    WHEN SUBSTR(a.FROMM,1,2)='00'   AND LENGTH(a.FROMM) in (14) THEN SUBSTR(a.FROMM,3,14)
                    WHEN SUBSTR(a.FROMM,1,3)='000' AND SUBSTR(a.FROMM,4,1)!='0' AND LENGTH(a.FROMM) in (16) THEN SUBSTR(a.FROMM,4,14)
                    WHEN SUBSTR(a.FROMM,1,4)='0000' AND LENGTH(a.FROMM) in (16) THEN SUBSTR(a.FROMM,5,12)
                    ELSE NULL END
                    , 1, LENGTH(c.DIALLING_CODE)
                ) = c.DIALLING_CODE
                
                --OUTBOUND
                --medzinarodne predvolby      
                LEFT JOIN UNICA_OWNER.UNICA_LIST_INT_DIAL_CODES e
                ON SUBSTR(CASE
                    WHEN SUBSTR(a.TOO,1,3)='000'  AND LENGTH(a.TOO) in (14, 15) THEN SUBSTR(a.TOO,4,14)
                    WHEN SUBSTR(a.TOO,1,2)='00'   AND LENGTH(a.TOO) in (14) THEN SUBSTR(a.TOO,3,14)
                    WHEN SUBSTR(a.TOO,1,3)='000'  AND SUBSTR(a.TOO,4,1)!='0' AND LENGTH(a.TOO) in (16) THEN SUBSTR(a.TOO,4,14)
                    WHEN SUBSTR(a.TOO,1,4)='0000' AND LENGTH(a.TOO) in (16) THEN SUBSTR(a.TOO,5,12)                  
                    ELSE NULL END
                    , 1, LENGTH(e.DIALLING_CODE)
                ) = e.DIALLING_CODE
                WHERE (
                    CASE
                        WHEN TRIM(COALESCE(a.FROMM,'Unknown')) not in ('Unknown','00') THEN 1
                        ELSE 0
                        END +
                    CASE WHEN TRIM(COALESCE(a.TOO,'Unknown')) not in ('Unknown','00') THEN 1
                    ELSE 0
                    END
                ) > 0
                AND a.start_timestamp < process_dt + 1
                AND a.start_timestamp >= ADD_MONTHS(process_dt + 1, -1)
            )
            
            SELECT
                MEDIA_TYPE,
                EVENT_DT,
                INTERACTION_TYPE,
                TOTAL_DURATION,
                DECODE(MEDIA_TYPE || INTERACTION_TYPE,
                    'EmailInbound', FROMM_FINAL,
                    'EmailOutbound', NULL,
                FROMM_FINAL) AS FROMM_FINAL,
                DECODE(MEDIA_TYPE || INTERACTION_TYPE,
                    'EmailInbound', NULL,
                    'EmailOutbound', FROMM_FINAL,
                TOO_FINAL) AS TOO_FINAL,
                TECHNICAL_RESULT
            FROM CC_LOG
        ;
    
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;

        step(p_stepname => l_proc_name || ' INSERT CUSTOMER_CONTACT', p_source => 'N/A', p_target => 'CUSTOMER_CONTACT');     
        INSERT INTO PM_OWNER.CUSTOMER_CONTACT
    
            SELECT 
                cc.customerid,
                LOWER(cc.contact_value) as contact_value,
                cc.contact_type, -- In very specific cases, contact_type not properly set
                DECODE(count(distinct cc.source_system),
                    1, max(cc.source_system),
                    2, 'CRM|SCUBE', count(distinct cc.source_system)) AS source_system,
                sysdate as insert_dt,
                NULL as contact_start_dt, -- contacts' validity often starts and ends, therefore currently not included
                NULL as contact_end_dt -- contacts' validity often starts and ends, therefore currently not included
            FROM (
                select 
                    cont.customerid,
                    cont.source_system,
                    CASE
                        WHEN contact_type in ('email', 'e-mail') OR contact_value like '%@%' THEN 'Email'
                        WHEN contact_type in ('phone_number', 'mobil', 'telefon', 'fax', 'SMS') THEN 'Voice'
                        ELSE contact_type END as contact_type,
                    CASE
                        WHEN SUBSTR(cont.contact_value, -9, 1) = '9' AND SUBSTR(cont.contact_value, 1, GREATEST(1, (length(cont.contact_value)-9))) in ('0', '00', '000', '0000', '00000') THEN '+421' || SUBSTR(cont.contact_value, -9, 9) --mobil SK
                        WHEN SUBSTR(cont.contact_value, -9, 1) in ('2', '3', '4', '5') AND SUBSTR(cont.contact_value, 1, GREATEST(1, (length(cont.contact_value)-9))) in ('0', '00', '000', '0000', '00000') THEN '+421' || SUBSTR(cont.contact_value, -9, 9) --pevna linka SK
                        WHEN SUBSTR(cont.contact_value, 1, 4) = '+420' AND LENGTH(SUBSTR(cont.contact_value, 5)) = 9 THEN cont.contact_value -- mobil CZ
                        WHEN SUBSTR(cont.contact_value, 1, 5) = '00420' AND LENGTH(SUBSTR(cont.contact_value, 6)) = 9 THEN '+420' || SUBSTR(cont.contact_value, -9, 9) --mobil CZ
                        WHEN LENGTH(cont.contact_value) in (14, 15) AND c.DIALLING_CODE is not null AND SUBSTR(cont.contact_value,1,3)='000'  THEN '+' || SUBSTR(cont.contact_value,4,14)
                        WHEN LENGTH(cont.contact_value)=14 AND c.DIALLING_CODE is not null AND SUBSTR(cont.contact_value,1,2)='00'   THEN '+' || SUBSTR(cont.contact_value,3,14)
                        WHEN LENGTH(cont.contact_value)=16 AND c.DIALLING_CODE is not null AND SUBSTR(cont.contact_value,1,3)='000' AND SUBSTR(cont.contact_value,4,1)!='0' THEN '+' || SUBSTR(cont.contact_value,4,14)
                        WHEN LENGTH(cont.contact_value)=16 AND c.DIALLING_CODE is not null AND SUBSTR(cont.contact_value,1,4)='0000' THEN '+' || SUBSTR(cont.contact_value,5,12) 
                        WHEN LENGTH(cont.contact_value)=4 THEN cont.contact_value
                        WHEN cont.contact_value like '%@%' THEN cont.contact_value
                        ELSE contact_value
                    END AS contact_value,
                    
                    contact_start_dt,
                    contact_end_dt
                
                FROM (
                    select
                        PR.party_id as customerid,
                        'SCUBE' source_system,
                        case when contct.CONTCT_TYPE_CD IN (14) then 'website'
                             else ctg.contct_type_gen_ds end contact_type,
                        case
                            when substr(PARTY_CONTCT_VAL, 1, 2) in ('00') and length(PARTY_CONTCT_VAL) = 14 AND contct.CONTCT_TYPE_CD IN (10, 11)
                                then '+' || SUBSTR(PARTY_CONTCT_VAL, 3, 12)
                            when substr(PARTY_CONTCT_VAL, 1, length(PARTY_CONTCT_VAL)-9) in ('421', '0421', '420', '0420') AND length(PARTY_CONTCT_VAL) in (12, 13) AND contct.CONTCT_TYPE_CD IN (10, 11)
                                THEN '+' || substr(PARTY_CONTCT_VAL, -12, 12) 
                            when substr(PARTY_CONTCT_VAL, -9, 1) in ('2', '3', '4', '5', '9') AND length(PARTY_CONTCT_VAL) = 9 AND contct.CONTCT_TYPE_CD IN (10, 11)
                                THEN '+421' || PARTY_CONTCT_VAL
                            when substr(PARTY_CONTCT_VAL, -9, 1) in ('2', '3', '4', '5', '9') AND SUBSTR(PARTY_CONTCT_VAL, 1, GREATEST(0, (length(PARTY_CONTCT_VAL)-9))) in ('0', '00') AND contct.CONTCT_TYPE_CD IN (10, 11)
                                THEN '+421' || SUBSTR(PARTY_CONTCT_VAL, -9, 9)
                            else lower(PARTY_CONTCT_VAL) end contact_value,
                        PARTY_CONTCT_START_DT as contact_start_dt,
                        PARTY_CONTCT_END_DT as contact_end_dt
                    from (
                        SELECT
                            PARTY_ID,
                            CONTCT_TYPE_CD,
                            CONTCT_PLACE_CD,
                            PARTY_CONTCT_VAL_ID,
                            PARTY_CONTCT_START_DT,
                            PARTY_CONTCT_END_DT,
                            CASE 
                                WHEN REGEXP_LIKE(party_contct_val, '^[+ /0-9\-]+$') AND CONTCT_TYPE_CD IN (10, 11)
                                THEN TRANSLATE(party_contct_val, 'x- /', 'x')
                                ELSE party_contct_val END PARTY_CONTCT_VAL,
                            PARTY_CONTCT_PREF_IN,
                                PARTY_CONTCT_PROD_ID,
                                ETL_SRC_SYST_CD,
                                PAID_INS,
                                PAID_UPD,
                                PARTY_CONTCT_NOTE,
                                PARTY_CONTCT_BRAND_PARTY_ID,
                                ROW_SOURCE_CD
                            FROM L0_OWNER.PARTY_CONTCT
                            WHERE 1=1
                                AND PARTY_CONTCT_START_DT < process_dt + 1
                                AND PARTY_CONTCT_START_DT >= (CASE WHEN process_dt + 1 > initial_load_dt THEN ADD_MONTHS(process_dt + 1, -1) ELSE min_contct_start END)
                                AND PARTY_CONTCT_END_DT >= LEAST(initial_load_dt, PARTY_CONTCT_START_DT)
                            ) contct
                      
                    JOIN L0_OWNER.PARTY_RLTD PR
                        ON  PR.child_party_id = contct.party_id
                        AND PR.party_rltd_rsn_cd = 4
                        AND pr.party_id IS NOT NULL
                        AND PR.party_rltd_start_dt < process_dt + 1
                        AND PR.party_rltd_start_dt >= (CASE WHEN process_dt + 1 > initial_load_dt THEN ADD_MONTHS(process_dt + 1, -1) ELSE min_contct_start END)
                        AND PR.party_rltd_end_dt >= LEAST(initial_load_dt, PR.party_rltd_start_dt)
                        
                    LEFT JOIN l0_owner.contct_type_gen ctg
                        ON ctg.contct_type_cd = contct.CONTCT_TYPE_CD
                        AND contct_type_gen_start_dt < process_dt + 1
                        AND contct_type_gen_start_dt >= (CASE WHEN process_dt + 1 > initial_load_dt THEN ADD_MONTHS(process_dt + 1, -1) ELSE min_contct_start END)
                        AND contct_type_gen_end_dt >= LEAST(initial_load_dt, contct_type_gen_start_dt)
                    
                    union all
                    
                    SELECT 
                        customerid,
                        source_system,
                        contact_type,
                        contact_value,
                        contact_start_dt,
                        contact_end_dt
                    FROM (
                        select
                            CCDB.customerid,
                            'CRM' source_system,
                            t_phone_number,
                            t_email,
                            t_website,
                            PARTY_CONTACT_START_DT as CONTACT_START_DT,
                            PARTY_CONTACT_end_DT as CONTACT_end_DT
                        from CCDB_OWNER.DH_S_CCD_PARTY_CONTACT CNT --MIN START_DT 21.10.2021
                        
                        --doplnenie CUID/CUSTOMERID
                        LEFT JOIN (
                            SELECT 
                                CCDB_ID,
                                CUSTOMERID 
                            FROM (
                                SELECT
                                    pp.cd_rec_party_1 as CCDB_ID,
                                    TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) as CUSTOMERID, --1 - CDS
                                    ROW_NUMBER() OVER (PARTITION BY TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) ORDER BY dt_bus_eff_from desc, pp.cd_rec_party_1) row_num
                                FROM CCDB_OWNER.oh_w_party_party PP
                                
                                LEFT JOIN L0_OWNER.PARTY P
                                    ON P.party_src_key=pp.cd_rec_party_2
                                    AND pp.cd_rec_party_2_source='1'
                                    AND P.SRC_SYST_CD=1
                                
                                where 1=1
                                    and trim(TRANSLATE(PP.cd_rec_party_1, '0123456789-,.', ' ')) is null
                                    and PP.PARTY_PARTY_START_DT < process_dt + 1
                                    and PP.PARTY_PARTY_START_DT >= (CASE WHEN process_dt + 1 > initial_load_dt THEN ADD_MONTHS(process_dt + 1, -1) ELSE min_contct_start END)
                                    and PP.PARTY_PARTY_END_DT >= LEAST(initial_load_dt, PP.PARTY_PARTY_START_DT)
                                    and PP.cd_rec_relation_type = 'PARTY_INSTANCE'
                                    and pp.cd_rec_relation_type_source = 'CCDB'
                                    and pp.cd_rec_source = 'CCDB'
                                    and pp.cd_rec_party_1_source = 'CCDB.CRM'
                                    and TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) is not null
                            ) WHERE row_num = 1
                        ) ccdb ON ccdb.CCDB_ID=CNT.CD_REC_PARTY
                            
                        WHERE 1=1
                        AND  ccdb.customerid IS NOT NULL
                        AND cnt.party_contact_start_dt < process_dt + 1
                        AND cnt.party_contact_start_dt >= (CASE WHEN process_dt + 1 > initial_load_dt THEN ADD_MONTHS(process_dt + 1, -1) ELSE min_contct_start END)
                        AND cnt.party_contact_end_dt >= LEAST(initial_load_dt, cnt.party_contact_start_dt)
                    )
                    UNPIVOT (
                        contact_value  -- unpivot_clause
                        FOR contact_type --  unpivot_for_clause
                        IN ( -- unpivot_in_clause
                            t_phone_number AS 'phone_number', 
                            t_email AS 'email', 
                            t_website AS 'website'
                        )
                    )
                ) cont
                --medzinarodne predvolby
                LEFT JOIN UNICA_OWNER.UNICA_LIST_INT_DIAL_CODES c
                    ON SUBSTR(
                        CASE
                            WHEN SUBSTR(cont.contact_value,1,3)='000'  AND LENGTH(cont.contact_value) in (14, 15) THEN SUBSTR(cont.contact_value,4,14)
                            WHEN SUBSTR(cont.contact_value,1,2)='00'   AND LENGTH(cont.contact_value) in (14) THEN SUBSTR(cont.contact_value,3,14)
                            WHEN SUBSTR(cont.contact_value,1,3)='000' AND SUBSTR(cont.contact_value,4,1)!='0' AND LENGTH(cont.contact_value) in (16) THEN SUBSTR(cont.contact_value,4,14)
                            WHEN SUBSTR(cont.contact_value,1,4)='0000' AND LENGTH(cont.contact_value) in (16) THEN SUBSTR(cont.contact_value,5,12)
                            ELSE NULL
                        END, 1, LENGTH(c.DIALLING_CODE)
                    ) = c.DIALLING_CODE
                
                WHERE contact_type NOT IN ('swift', 'Kontaktní osoba')
            ) cc
            LEFT JOIN PM_OWNER.CUSTOMER_CONTACT cc0
                ON cc.customerid = cc0.customerid
                AND cc.contact_value = cc0.contact_value
            WHERE cc0.customerid IS NULL
            
            GROUP BY cc.customerid, cc.contact_value, cc.contact_type
        ;	
    
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;

        step(p_stepname => l_proc_name || ' INSERT CALL_CENTRUM_HISTORY', p_source => 'N/A', p_target => 'CALL_CENTRUM_HISTORY');     
        INSERT INTO PM_OWNER.CALL_CENTRUM_HISTORY
            SELECT * FROM (
                SELECT 
                    f.customerid,
                    f.contact_value,
                    media_type,
                    event_dt,
                    interaction_type,
                    total_duration,
                    technical_result, 
                    phone_description as csob_phone_number_description
                FROM (
                    SELECT
                        customerid,
                        contact_value
            --            ,contact_type -- Very rarely wrong contact_type, in general not necessary for JOINing data
                    FROM PM_OWNER.CUSTOMER_CONTACT
                ) f
                JOIN (
                    SELECT DISTINCT
                        EVENT_DT,
                        FROMM_FINAL,
                        INTERACTION_TYPE,
                        MEDIA_TYPE,
                        TECHNICAL_RESULT,
                        TOO_FINAL,
                        TOTAL_DURATION,
                        csob.phone_description
                    FROM PM_OWNER.CALL_CENTRUM_LOG cca 
                    LEFT JOIN je69050.csob_phone_number csob
                        ON cca.too_final = csob.phone_number
                        AND cca.interaction_type = 'Inbound'
                    WHERE VALIDATE_CONVERSION(cca.too_final AS NUMBER) = 1
                        AND cca.event_dt < process_dt + 1
                        AND cca.event_dt >= ADD_MONTHS(process_dt + 1, -1)
                ) cc ON (
                    f.contact_value = LOWER(DECODE( cc.interaction_type,
                        'Inbound', cc.fromm_final,
                        'Outbound', cc.too_final
                    ))
                )
            )
            PIVOT (
                COUNT(technical_result)
                FOR technical_result IN (
                    'Completed' AS I_RESULT_COMPLETED,
                    'Redirected' AS I_RESULT_REDIRECTED,
                    'Conferenced' AS I_RESULT_CONFERENCED,
                    'Diverted' AS I_RESULT_DIVERTED,
                    'Abandoned' AS I_RESULT_ABANDONED,
                    'Transferred' AS I_RESULT_TRANSFERRED,
                    'DestinationBusy' AS I_RESULT_DESTINATIONBUSY,
                    'None' AS I_RESULT_NONE,
                    'CustomerAbandoned' AS I_RESULT_CUSTOMERABANDONED,
                    'AbnormalStop' AS I_RESULT_ABNORMALSTOP
                )
            )
        ;
        
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;       

        step(p_stepname => l_proc_name || ' INSERT FEEDBACK_EVALUATION', p_source => 'N/A', p_target => 'FEEDBACK_EVALUATION');      
        INSERT INTO PM_OWNER.FEEDBACK_EVALUATION
    
            SELECT /*+ NO_PARALLEL ORDERED */
                /*  Transformation of incorrect structure of customerid, where we need to parse original customerid e.g.
                    - 123456789
                    
                    from possible values containg dates if format YYMMDD or repeated original value, e.g.
                    - 123456789
                    - 123456789231231
                    - 123456789123456789231231
                    - 123456789231231123456789231231
                */
                TO_NUMBER(CASE 
                    WHEN LENGTH(customerid) > 24 THEN SUBSTR(SUBSTR(customerid, 1, LENGTH(customerid)/2), 1, LENGTH(customerid)/2 - 6)
                    WHEN LENGTH(customerid) > 16 THEN SUBSTR(SUBSTR(customerid, 1, LENGTH(customerid) - 6), 1, LENGTH(SUBSTR(customerid, 1, LENGTH(customerid) - 6))/2)
                    WHEN LENGTH(customerid) > 12 THEN SUBSTR(customerid, 1, LENGTH(customerid) - 6)
                    ELSE customerid END) AS customerid,
                event_dt,
                feedback,
                source_table,
                NULL AS i_service_request
            FROM (
    
                --call centrum
    
                SELECT
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    ROUND(TO_NUMBER(odpoved_6)/MAX(TO_NUMBER(odpoved_6)) OVER () * 10) AS feedback,
                    'C_003189_CLF165_NETQUEST' as source_table
                FROM PM_OWNER.C_003189_CLF165_Netquest
                WHERE odpoved_6 is not null
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                UNION ALL
                 
                --loans
                
                select 
                TO_CHAR(customerid) AS customerid, 
                TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                ROUND((
                    2 * NVL(
                            DECODE(TRIM(odpoved_6),
                                'veľmi spokojný/á', 5,
                                'skôr spokojný/á', 4,
                                'neviem sa vyjadriť', 3,
                                'skôr nespokojný/á', 2,
                                'veľmi nespokojný/á', 1
                            ), 0) + NVL(odpoved_7, 0)
                    ) 
                    / (NVL2(odpoved_6, 1, 0) + NVL2(odpoved_7, 1, 0))
                ) as feedback,
                    'C_001651_CLF167_NETQUEST' as source_table
                from PM_OWNER.C_001651_CLF167_Netquest
                where not (odpoved_6 is null and odpoved_7 is null)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                            
                UNION ALL
                
                --digi products
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    TO_NUMBER(odporucili_by_ste) as feedback,
                    'C_006270_CLF329_NETQUEST' as source_table
                from PM_OWNER.C_006270_CLF329_Netquest
                where odporucili_by_ste is not null
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                    
                UNION ALL
                 
                --Smart account
                select
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    TO_NUMBER(odpoved_5) as feedback,
                    'C_001367_RET484_NETQUEST' as source_table
                from PM_OWNER.C_001367_RET484_Netquest a
                where odpoved_5 is not null
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                        
                UNION ALL
                
                --Smart account 30d
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    ROUND(
                        2 * ((NVL(odpoved_1, 0) + NVL(odpoved_2, 0)) 
                        / (NVL2(odpoved_1, 1, 0) + NVL2(odpoved_2, 1, 0)))
                    ) as feedback,
                    'C_001367_RET484l_NETQUEST' as source_table
                from PM_OWNER.C_001367_RET484l_Netquest
                where not (odpoved_1 is null and odpoved_2 is null)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                        
                UNION ALL
                
                -- prieskum po navsteve pobocky
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    ROUND(
                        (NVL(odpoved_1, 0) + NVL(odpoved_3, 0))
                        / (NVL2(odpoved_1, 1, 0) + NVL2(odpoved_3, 1, 0))
                    ) as feedback,
                    'C_001367_RET142E_NETQUEST' as source_table
                from PM_OWNER.C_001367_RET142E_NETQUEST
                where not (odpoved_1 is null and odpoved_3 is null)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                        
                UNION ALL
                
                -- prieskum vyriesenych staznosti
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    2 * DECODE(LOWER(TRIM(odpoved_1)),
                                'veľmi spokojný', 5,
                                'skôr spokojný', 4,
                                'neviem sa vyjadriť', 3,
                                'skôr nespokojný', 2,
                                'veľmi nespokojný', 1
                    ) as feedback,
                    'C_006897_CLF336_NETQUEST' as source_table
                from PM_OWNER.C_006897_CLF336_Netquest
                where odpoved_1 is not null
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                        
                UNION ALL
                
                -- prieskum po prvom nakupe fondov
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    ROUND(
                        (NVL(odpoved_1, 0) + NVL(odpoved_4, 0)) 
                        / (NVL2(odpoved_1, 1, 0) + NVL2(odpoved_4, 1, 0))
                    ) as feedback,
                    'C_003189_CLF73F_NETQUEST' as source_table
                from PM_OWNER.C_003189_CLF73f_Netquest
                where not (odpoved_1 is null and odpoved_4 is null)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                        
                UNION ALL
                
                -- prieskum po likvidacii poistnej udalosti
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    ROUND((
                        2 * (NVL(
                                DECODE(TRIM(odpoved_1),
                                    'veľmi spokojný', 5,
                                    'skôr spokojný', 4,
                                    'neviem sa vyjadriť', 3,
                                    'skôr nespokojný', 2,
                                    'veľmi nespokojný', 1
                                ), 0) +
                            NVL(
                                DECODE(TRIM(odpoved_2),
                                    'veľmi spokojný', 5,
                                    'skôr spokojný', 4,
                                    'neviem sa vyjadriť', 3,
                                    'skôr nespokojný', 2,
                                    'veľmi nespokojný', 1
                                ), 0) +
                            NVL(
                                DECODE(TRIM(odpoved_3),
                                    'veľmi spokojný', 5,
                                    'skôr spokojný', 4,
                                    'neviem sa vyjadriť', 3,
                                    'skôr nespokojný', 2,
                                    'veľmi nespokojný', 1
                                ), 0) +
                            NVL(
                                DECODE(TRIM(odpoved_4),
                                    'veľmi spokojný', 5,
                                    'skôr spokojný', 4,
                                    'neviem sa vyjadriť', 3,
                                    'skôr nespokojný', 2,
                                    'veľmi nespokojný', 1
                                ), 0)
                            ) + NVL(odpoved_5, 0)
                        ) 
                        / (NVL2(odpoved_1, 1, 0) + NVL2(odpoved_2, 1, 0) + NVL2(odpoved_3, 1, 0) + NVL2(odpoved_4, 1, 0) + NVL2(odpoved_5, 1, 0))
                    ) as feedback,
                    'C_007880_CLF342_NETQUEST' as source_table
                from PM_OWNER.C_007880_CLF342_Netquest
                where not (odpoved_1 is null and odpoved_2 is null and odpoved_3 is null and odpoved_4 is null and odpoved_5 is null)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                        
                UNION ALL
                
                -- Splátka KK v SB
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    --datum_ukoncenia,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    2 * DECODE(LOWER(TRIM(odpoved_3)),
                            'veľmi spokojný', 5,
                            'skôr spokojný', 4,
                            'neviem sa vyjadriť', 3,
                            'skôr nespokojný', 2,
                            'veľmi nespokojný', 1
                    ) as feedback,
                    'C_008009_CLF344_NETQUEST' as source_table
                from PM_OWNER.c_008009_clf344_netquest
                where odpoved_3 is not null
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                        
                UNION ALL
                
                -- eMessage CLF73
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    ROUND(
                        (NVL(odpoved_1, 0) + NVL(odpoved_3, 0)) 
                        / (NVL2(odpoved_1, 1, 0) + NVL2(odpoved_3, 1, 0))
                    ) as feedback,
                    'C_003189_CLF73_NETQUEST' as source_table
                from PM_OWNER.C_003189_CLF73_Netquest
                where not (odpoved_1 is null and odpoved_3 is null)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                        
                UNION ALL
                
                -- pravidelné investovanie
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    ROUND(
                        (NVL(odpoved_2, 0) + NVL(odpoved_4, 0))
                        / (NVL2(odpoved_2, 1, 0) + NVL2(odpoved_4, 1, 0))
                    ) as feedback,
                    'C_008125_CLF345_NETQUEST' as source_table
                from PM_OWNER.c_008125_clf345_netquest
                where not (odpoved_2 is null and odpoved_4 is null)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                
                -- Smart ucet pre klientov bez BU 
                -- (EXCLUDED) UNICA_OWNER.c_008827_clf356_netquest - last record from 2023-05-08, no numeric feedback
                                
                UNION ALL
                
                -- pravidelné investovanie novy dotaznik
                 
                select 
                    TO_CHAR(customerid) AS customerid,
                    TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') as EVENT_DT,
                    ROUND(
                        (NVL(odpoved_2, 0) + NVL(odpoved_3, 0))
                        / (NVL2(odpoved_2, 1, 0) + NVL2(odpoved_3, 1, 0))
                    ) as feedback,
                    'C_008125_CLF345_NETQUEST_20231016' as source_table
                from PM_OWNER.C_008125_CLF345_Netquest_20231016
                where not (odpoved_2 is null and odpoved_3 is null)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_ukoncenia, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
    
                
                UNION ALL
    
                select
                    TO_CHAR(uid_cuidd.party_id) AS customeridd,
                    TO_DATE(datum_navstevy, 'YYYY-MM-DD') as EVENT_DTT,
                    ROUND(
                        (NVL(o1_spokojnost_s_obsluhou, 0) + NVL(o3_odporucenie_pobocka, 0) + 10 * NVL(CASE WHEN celkom BETWEEN 0 AND 1 THEN celkom END, 0))
                        / (NVL2(o1_spokojnost_s_obsluhou, 1, 0) + NVL2(o3_odporucenie_pobocka, 1, 0) + NVL2(CASE WHEN celkom BETWEEN 0 AND 1 THEN celkom END, 1, 0))
                    ) as feedbackk,
                    'EMESSAGE_DETAIL_POBOCKY' as source_tablee
                from JE69050.emessage_detail_pobocky nps
                
                --- adding customerid through cuid based on ETL_UNICA_UID
                JOIN L0_OWNER.PARTY P
                ON P.party_src_val = nps.cuid
                    AND P.src_syst_cd = 110 
                    AND P.party_subtp_cd IN (0,1,2,3)
                    AND P.party_src_key like 'PMC%'
                JOIN L0_OWNER.PARTY_RLTD uid_cuidd
                ON  uid_cuidd.child_party_id = P.party_id
                    AND uid_cuidd.party_rltd_rsn_cd = 4
                    AND uid_cuidd.party_rltd_start_dt <= process_dt
                    AND uid_cuidd.party_rltd_end_dt > process_dt
                
                WHERE not (o1_spokojnost_s_obsluhou is null and o3_odporucenie_pobocka is null)
                    AND TO_DATE(datum_navstevy, 'YYYY-MM-DD HH24:MI:SS') >= ADD_MONTHS(process_dt + 1, -1)
                    AND TO_DATE(datum_navstevy, 'YYYY-MM-DD HH24:MI:SS') < process_dt + 1
                
                GROUP BY uid_cuidd.party_id,
                         datum_navstevy,
                         ROUND(
                            (NVL(o1_spokojnost_s_obsluhou, 0) + NVL(o3_odporucenie_pobocka, 0) + 10 * NVL(CASE WHEN celkom BETWEEN 0 AND 1 THEN celkom END, 0))
                            / (NVL2(o1_spokojnost_s_obsluhou, 1, 0) + NVL2(o3_odporucenie_pobocka, 1, 0) + NVL2(CASE WHEN celkom BETWEEN 0 AND 1 THEN celkom END, 1, 0))
                        )
                
                UNION ALL
    
                -- Branch visits
                SELECT
                    TO_CHAR(customerid) AS customerid,
                    event_dt,
                    ROUND(2 * AVG(FEEDBACK_EVALUTION)) as FEEDBACK,
                    'OH_W_EVENT' as source_table
                FROM (
                    -- crm sessions
                    SELECT /* ORDERED*/
                        CCDB.CUSTOMERID,
                        CAST(E.DT_BUS_EFF_FROM AS DATE) AS EVENT_DT,
                        CASE E.CD_REC_VALUE_PROPERTY_2
                            WHEN '1' THEN 5 -- veľmi spokojný 
                            WHEN '2' THEN 4 
                            WHEN '3' THEN 3 
                            WHEN '4' THEN 2 
                            WHEN '5' THEN 1 -- veľmi nespokojný 
                            ELSE NULL
                        END FEEDBACK_EVALUTION,
                        E.CD_REC_VALUE_PROPERTY_2 as feed_orig
                
                    from CCDB_OWNER.OH_W_EVENT E
                    
                    --dotiahnutie klienta
                    JOIN CCDB_OWNER.OH_W_PARTY P
                        on P.cd_rec_party = E.cd_rec_party_1 
                        and P.cd_rec_source = 'CCDB.CRM'
                        and P.cd_rec_object_type = E.cd_rec_party_1_object_type 
                        and P.party_end_dt = date '9999-12-31'
                        AND P.DELETED_DT   = date '9999-12-31'
                    
                    --doplnenie CUID/CUSTOMERID
                    LEFT JOIN (
                        SELECT CCDB_ID, CUSTOMERID
                                ,PARTY_PARTY_START_DT
                                ,PARTY_PARTY_END_DT 
                            FROM (
                            SELECT
                                pp.cd_rec_party_1 as CCDB_ID,
                                TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) as CUSTOMERID, --1 - CDS
            --                    ROW_NUMBER() OVER (PARTITION BY TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) ORDER BY dt_bus_eff_from desc, pp.cd_rec_party_1) row_num,
                                PP.PARTY_PARTY_START_DT,
                                PP.PARTY_PARTY_END_DT
                            FROM CCDB_OWNER.oh_w_party_party PP
                            
                            LEFT JOIN L0_OWNER.PARTY P
                                ON P.party_src_key=pp.cd_rec_party_2
                                AND pp.cd_rec_party_2_source='1'
                                AND P.SRC_SYST_CD=1
                            
                            where 1=1
                                and trim(TRANSLATE(PP.cd_rec_party_1, '0123456789-,.', ' ')) is null
                                and PP.PARTY_PARTY_END_DT >= ADD_MONTHS(process_dt + 1, -1)
                                and PP.PARTY_PARTY_START_DT < process_dt + 1
                                and PP.cd_rec_relation_type = 'PARTY_INSTANCE'
                                and pp.cd_rec_relation_type_source = 'CCDB'
                                and pp.cd_rec_source = 'CCDB'
                                and pp.cd_rec_party_1_source = 'CCDB.CRM'
                                and TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) is not null)
            --            WHERE row_num = 1
                        ) CCDB
                        ON CCDB.CCDB_ID=P.CD_REC_PARTY
                            and CCDB.PARTY_PARTY_START_DT <= E.DT_BUS_EFF_FROM
                            and CCDB.PARTY_PARTY_END_DT > E.DT_BUS_EFF_FROM  
                    
                    where 1=1
                    AND E.CD_REC_VALUE_PROPERTY_2 <> 'XNA'
                    AND E.CD_REC_VALUE_PROPERTY_2 is not null
                    AND CAST(E.DT_BUS_EFF_FROM AS DATE) >= ADD_MONTHS(process_dt + 1, -1)
                    AND CAST(E.DT_BUS_EFF_FROM AS DATE) < process_dt + 1
                    AND E.event_end_dt = date '9999-12-31'
                    AND E.cd_rec_source = 'CCDB.CRM'
                    AND E.cd_rec_object_type='OBJECT~EVENT~SESSION'
                    and CCDB.CUSTOMERID is not null
                )
                
                GROUP BY TO_CHAR(customerid),
                    event_dt
            )
    
            UNION ALL
                            
            -- sťažnosti
                SELECT 
                    CUSTOMERID as CUSTOMERID,
                    SERVICE_REQUEST_FACT_DT as event_dt,
                    NULL AS feedback,
                    'OH_W_SERVICE_REQUEST_FACT_TICK' AS source_table,
                    1 AS i_service_request
                FROM 
                (
                    SELECT 
            --            row_number() OVER(PARTITION BY T_NAME||'~'||TO_CHAR(CD_REC_PARTY) ORDER BY DT_TEC_UPDATED_MASTER DESC) row_num,
                            CUSTOMERID,
                            SERVICE_REQUEST_FACT_DT,
                            CD_REC_PARTY
                    FROM 
                    (
                        SELECT 
                            SRF.T_NAME,
                                cust.party_id AS CUSTOMERID,
                                SRF.DT_TEC_UPDATED_MASTER, 
                                SRF.SERVICE_REQUEST_FACT_DT,
                                cuid.CD_REC_PARTY
                        FROM CCDB_OWNER.OH_W_SERVICE_REQUEST_FACT_TICK SRF
                        JOIN ccdb_owner.oh_w_event_tick ET
                        ON SRF.cd_rec_event = ET.cd_rec_event
                            AND SRF.cd_rec_event_source = ET.cd_rec_source
                            AND  SRF.cd_rec_event_source  = 'CCDB.CRM'
                            AND SRF.DELETED_DT = TO_DATE('9999-12-31', 'YYYY-MM-DD')
                            AND ET.DELETED_DT  = TO_DATE('9999-12-31', 'YYYY-MM-DD')
                            AND ET.CD_REC_OBJECT_TYPE != 'OBJECT~EVENT~SERVICE~REQUEST_EXECUTION'
                            AND ET.CD_REC_VALUE_PROPERTY_2 NOT IN ('1183', '1182')
                            
                        JOIN ccdb_owner.DH_S_CCD_PARTY cuid
                        ON ET.CD_REC_PARTY_1 = cuid.CD_REC_PARTY
                            AND  cuid.cd_rec_party_source  = 'CCDB.CRM'
                            AND  cuid.DELETED_DT  = TO_DATE('9999-12-31', 'YYYY-MM-DD')
                        
                        --- adding customerid through cuid based on ETL_UNICA_UID
                        JOIN L0_OWNER.PARTY P
                        ON P.party_src_val = cuid.CD_PARTY_LEGACY_CUID
                            AND P.src_syst_cd = 110 
                            AND P.party_subtp_cd IN (0,1,2,3)
                            AND P.party_src_key like 'PMC%'
                        JOIN L0_OWNER.PARTY_RLTD cust
                        ON  cust.child_party_id = P.party_id
                            AND cust.party_rltd_rsn_cd = 4
                            AND cust.party_rltd_start_dt <= process_dt
                            AND cust.party_rltd_end_dt > process_dt
                
                        WHERE 1=1
                            AND SRF.SERVICE_REQUEST_FACT_DT >= ADD_MONTHS(process_dt + 1, -1)
                            AND SRF.SERVICE_REQUEST_FACT_DT < process_dt + 1
                            AND SRF.T_NAME NOT IN (
                                'Vystavenie bankovej informácie pre účely auditu',
                                'BB majiteľ inštalácie - nastavenie',
                                'BB majiteľ účtu - nastavenie BB',
                                'IPPID - priradenie Tokenu DP770 pre službu BB Lite',
                                'Pridanie účtu do ELB'
                            )
                            AND SUBSTR(SRF.T_NAME, 3,1) != '-'
                    )
                )
                WHERE 1=1
            --        AND row_num = 1
            
                group by customerid, SERVICE_REQUEST_FACT_DT
    
        ;
        
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;       
        
        -- ATTR_INTERACTION
        step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
        mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
        step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);
        
        INSERT INTO pm_owner.ATTR_INTERACTION
        (
            SELECT 
                main.month_id,
                main.customerid,
                NVL(AVG_INBOUND_EMAIL_DURATION_M, 0) AS AVG_INBOUND_EMAIL_DURATION_M,
                ROUND((NVL(AVG_INBOUND_EMAIL_DURATION_M, 0) + NVL(AVG_INBOUND_EMAIL_DURATION_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0))/main.c_month_q) AS AVG_INBOUND_EMAIL_DURATION_Q,
                ROUND((NVL(AVG_INBOUND_EMAIL_DURATION_M, 0) + NVL(AVG_INBOUND_EMAIL_DURATION_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0))/main.c_month_y) AS AVG_INBOUND_EMAIL_DURATION_Y,
                NVL(AVG_INBOUND_VOICE_DURATION_M, 0) AS AVG_INBOUND_VOICE_DURATION_M,
                ROUND((NVL(AVG_INBOUND_VOICE_DURATION_M, 0) + NVL(AVG_INBOUND_VOICE_DURATION_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0))/main.c_month_q) AS AVG_INBOUND_VOICE_DURATION_Q,
                ROUND((NVL(AVG_INBOUND_VOICE_DURATION_M, 0) + NVL(AVG_INBOUND_VOICE_DURATION_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0))/main.c_month_y) AS AVG_INBOUND_VOICE_DURATION_Y,
                ROUND(DECODE(((NVL(AVG_INBOUND_VOICE_DURATION_M, 0) + NVL(AVG_INBOUND_VOICE_DURATION_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0))/main.c_month_q), 0, 1, NVL(AVG_INBOUND_VOICE_DURATION_M, 0)/((NVL(AVG_INBOUND_VOICE_DURATION_M, 0) + NVL(AVG_INBOUND_VOICE_DURATION_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0))/main.c_month_q)), 2) AS RATIO_DELTA_AVG_INBOUND_VOICE_DURATION_MQ,
                ROUND(DECODE(((NVL(AVG_INBOUND_VOICE_DURATION_M, 0) + NVL(AVG_INBOUND_VOICE_DURATION_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0))/main.c_month_y), 0, 1, NVL(AVG_INBOUND_VOICE_DURATION_M, 0)/((NVL(AVG_INBOUND_VOICE_DURATION_M, 0) + NVL(AVG_INBOUND_VOICE_DURATION_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0))/main.c_month_y)), 2) AS RATIO_DELTA_AVG_INBOUND_VOICE_DURATION_MY,
                NVL(AVG_OUTBOUND_EMAIL_DURATION_M, 0) AS AVG_OUTBOUND_EMAIL_DURATION_M,
                ROUND((NVL(AVG_OUTBOUND_EMAIL_DURATION_M, 0) + NVL(AVG_OUTBOUND_EMAIL_DURATION_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0))/main.c_month_q) AS AVG_OUTBOUND_EMAIL_DURATION_Q,
                ROUND((NVL(AVG_OUTBOUND_EMAIL_DURATION_M, 0) + NVL(AVG_OUTBOUND_EMAIL_DURATION_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0))/main.c_month_y) AS AVG_OUTBOUND_EMAIL_DURATION_Y,
                NVL(AVG_OUTBOUND_VOICE_DURATION_M, 0) AS AVG_OUTBOUND_VOICE_DURATION_M,
                ROUND((NVL(AVG_OUTBOUND_VOICE_DURATION_M, 0) + NVL(AVG_OUTBOUND_VOICE_DURATION_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0))/main.c_month_q) AS AVG_OUTBOUND_VOICE_DURATION_Q,
                ROUND((NVL(AVG_OUTBOUND_VOICE_DURATION_M, 0) + NVL(AVG_OUTBOUND_VOICE_DURATION_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0))/main.c_month_y) AS AVG_OUTBOUND_VOICE_DURATION_Y,
                NVL(SUM_INBOUND_EMAIL_DURATION_M, 0) AS SUM_INBOUND_EMAIL_DURATION_M,
                NVL(SUM_INBOUND_EMAIL_DURATION_M, 0) + NVL(SUM_INBOUND_EMAIL_DURATION_M2, 0) AS SUM_INBOUND_EMAIL_DURATION_Q,
                NVL(SUM_INBOUND_EMAIL_DURATION_M, 0) + NVL(SUM_INBOUND_EMAIL_DURATION_M11, 0) AS SUM_INBOUND_EMAIL_DURATION_Y,
                NVL(SUM_INBOUND_VOICE_DURATION_M, 0) AS SUM_INBOUND_VOICE_DURATION_M,
                NVL(SUM_INBOUND_VOICE_DURATION_M, 0) + NVL(SUM_INBOUND_VOICE_DURATION_M2, 0) AS SUM_INBOUND_VOICE_DURATION_Q,
                NVL(SUM_INBOUND_VOICE_DURATION_M, 0) + NVL(SUM_INBOUND_VOICE_DURATION_M11, 0) AS SUM_INBOUND_VOICE_DURATION_Y,
                NVL(SUM_OUTBOUND_EMAIL_DURATION_M, 0) AS SUM_OUTBOUND_EMAIL_DURATION_M,
                NVL(SUM_OUTBOUND_EMAIL_DURATION_M, 0) + NVL(SUM_OUTBOUND_EMAIL_DURATION_M2, 0) AS SUM_OUTBOUND_EMAIL_DURATION_Q,
                NVL(SUM_OUTBOUND_EMAIL_DURATION_M, 0) + NVL(SUM_OUTBOUND_EMAIL_DURATION_M11, 0) AS SUM_OUTBOUND_EMAIL_DURATION_Y,
                NVL(SUM_OUTBOUND_VOICE_DURATION_M, 0) AS SUM_OUTBOUND_VOICE_DURATION_M,
                NVL(SUM_OUTBOUND_VOICE_DURATION_M, 0) + NVL(SUM_OUTBOUND_VOICE_DURATION_M2, 0) AS SUM_OUTBOUND_VOICE_DURATION_Q,
                NVL(SUM_OUTBOUND_VOICE_DURATION_M, 0) + NVL(SUM_OUTBOUND_VOICE_DURATION_M11, 0) AS SUM_OUTBOUND_VOICE_DURATION_Y,
                NVL(C_INBOUND_EMAIL_M, 0) AS C_INBOUND_EMAIL_M,
                NVL(C_INBOUND_EMAIL_M, 0) + NVL(C_INBOUND_EMAIL_M2, 0) AS C_INBOUND_EMAIL_Q,
                NVL(C_INBOUND_EMAIL_M, 0) + NVL(C_INBOUND_EMAIL_M11, 0) AS C_INBOUND_EMAIL_Y,
                ROUND(DECODE(NVL(C_INBOUND_EMAIL_M, 0) + NVL(C_INBOUND_EMAIL_M2, 0), 0, 1/3, NVL(C_INBOUND_EMAIL_M, 0)/(NVL(C_INBOUND_EMAIL_M, 0) + NVL(C_INBOUND_EMAIL_M2, 0))), 2) AS RATIO_DELTA_C_INBOUND_EMAIL_MQ,
                ROUND(DECODE(NVL(C_INBOUND_EMAIL_M, 0) + NVL(C_INBOUND_EMAIL_M11, 0), 0, 1/12, NVL(C_INBOUND_EMAIL_M, 0)/(NVL(C_INBOUND_EMAIL_M, 0) + NVL(C_INBOUND_EMAIL_M11, 0))), 2) AS RATIO_DELTA_C_INBOUND_EMAIL_MY,
                NVL(C_INBOUND_VOICE_M, 0) AS C_INBOUND_VOICE_M,
                NVL(C_INBOUND_VOICE_M, 0) + NVL(C_INBOUND_VOICE_M2, 0) AS C_INBOUND_VOICE_Q,
                NVL(C_INBOUND_VOICE_M, 0) + NVL(C_INBOUND_VOICE_M11, 0) AS C_INBOUND_VOICE_Y,
                ROUND(DECODE(NVL(C_INBOUND_VOICE_M, 0) + NVL(C_INBOUND_VOICE_M2, 0), 0, 1/3, NVL(C_INBOUND_VOICE_M, 0)/(NVL(C_INBOUND_VOICE_M, 0) + NVL(C_INBOUND_VOICE_M2, 0))), 2) AS RATIO_DELTA_C_INBOUND_VOICE_MQ,
                ROUND(DECODE(NVL(C_INBOUND_VOICE_M, 0) + NVL(C_INBOUND_VOICE_M11, 0), 0, 1/12, NVL(C_INBOUND_VOICE_M, 0)/(NVL(C_INBOUND_VOICE_M, 0) + NVL(C_INBOUND_VOICE_M11, 0))), 2) AS RATIO_DELTA_C_INBOUND_VOICE_MY,
                NVL(C_OUTBOUND_EMAIL_M, 0) AS C_OUTBOUND_EMAIL_M,
                NVL(C_OUTBOUND_EMAIL_M, 0) + NVL(C_OUTBOUND_EMAIL_M2, 0) AS C_OUTBOUND_EMAIL_Q,
                NVL(C_OUTBOUND_EMAIL_M, 0) + NVL(C_OUTBOUND_EMAIL_M11, 0) AS C_OUTBOUND_EMAIL_Y,
                ROUND(DECODE(NVL(C_OUTBOUND_EMAIL_M, 0) + NVL(C_OUTBOUND_EMAIL_M2, 0), 0, 1/3, NVL(C_OUTBOUND_EMAIL_M, 0)/(NVL(C_OUTBOUND_EMAIL_M, 0) + NVL(C_OUTBOUND_EMAIL_M2, 0))), 2) AS RATIO_DELTA_C_OUTBOUND_EMAIL_MQ,
                ROUND(DECODE(NVL(C_OUTBOUND_EMAIL_M, 0) + NVL(C_OUTBOUND_EMAIL_M11, 0), 0, 1/12, NVL(C_OUTBOUND_EMAIL_M, 0)/(NVL(C_OUTBOUND_EMAIL_M, 0) + NVL(C_OUTBOUND_EMAIL_M11, 0))), 2) AS RATIO_DELTA_C_OUTBOUND_EMAIL_MY,
                NVL(C_OUTBOUND_VOICE_M, 0) AS C_OUTBOUND_VOICE_M,
                NVL(C_OUTBOUND_VOICE_M, 0) + NVL(C_OUTBOUND_VOICE_M2, 0) AS C_OUTBOUND_VOICE_Q,
                NVL(C_OUTBOUND_VOICE_M, 0) + NVL(C_OUTBOUND_VOICE_M11, 0) AS C_OUTBOUND_VOICE_Y,
                ROUND(DECODE(NVL(C_OUTBOUND_VOICE_M, 0) + NVL(C_OUTBOUND_VOICE_M2, 0), 0, 1/3, NVL(C_OUTBOUND_VOICE_M, 0)/(NVL(C_OUTBOUND_VOICE_M, 0) + NVL(C_OUTBOUND_VOICE_M2, 0))), 2) AS RATIO_DELTA_C_OUTBOUND_VOICE_MQ,
                ROUND(DECODE(NVL(C_OUTBOUND_VOICE_M, 0) + NVL(C_OUTBOUND_VOICE_M11, 0), 0, 1/12, NVL(C_OUTBOUND_VOICE_M, 0)/(NVL(C_OUTBOUND_VOICE_M, 0) + NVL(C_OUTBOUND_VOICE_M11, 0))), 2) AS RATIO_DELTA_C_OUTBOUND_VOICE_MY,
                NVL(I_INBOUND_EMAIL_M, 0) AS I_INBOUND_EMAIL_M,
                GREATEST(NVL(I_INBOUND_EMAIL_M, 0), NVL(I_INBOUND_EMAIL_M2, 0)) AS I_INBOUND_EMAIL_Q,
                GREATEST(NVL(I_INBOUND_EMAIL_M, 0), NVL(I_INBOUND_EMAIL_M11, 0)) AS I_INBOUND_EMAIL_Y,
                NVL(I_INBOUND_VOICE_M, 0) AS I_INBOUND_VOICE_M,
                GREATEST(NVL(I_INBOUND_VOICE_M, 0), NVL(I_INBOUND_VOICE_M2, 0)) AS I_INBOUND_VOICE_Q,
                GREATEST(NVL(I_INBOUND_VOICE_M, 0), NVL(I_INBOUND_VOICE_M11, 0)) AS I_INBOUND_VOICE_Y,
                NVL(I_OUTBOUND_EMAIL_M, 0) AS I_OUTBOUND_EMAIL_M,
                GREATEST(NVL(I_OUTBOUND_EMAIL_M, 0), NVL(I_OUTBOUND_EMAIL_M2, 0)) AS I_OUTBOUND_EMAIL_Q,
                GREATEST(NVL(I_OUTBOUND_EMAIL_M, 0), NVL(I_OUTBOUND_EMAIL_M11, 0)) AS I_OUTBOUND_EMAIL_Y,
                NVL(I_OUTBOUND_VOICE_M, 0) AS I_OUTBOUND_VOICE_M,
                GREATEST(NVL(I_OUTBOUND_VOICE_M, 0), NVL(I_OUTBOUND_VOICE_M2, 0)) AS I_OUTBOUND_VOICE_Q,
                GREATEST(NVL(I_OUTBOUND_VOICE_M, 0), NVL(I_OUTBOUND_VOICE_M11, 0)) AS I_OUTBOUND_VOICE_Y,
                NVL(C_INBOUND_RESULT_COMPLETED_M, 0) AS C_INBOUND_RESULT_COMPLETED_M,
                NVL(C_INBOUND_RESULT_COMPLETED_M, 0) + NVL(C_INBOUND_RESULT_COMPLETED_M2, 0) AS C_INBOUND_RESULT_COMPLETED_Q,
                NVL(C_INBOUND_RESULT_COMPLETED_M, 0) + NVL(C_INBOUND_RESULT_COMPLETED_M11, 0) AS C_INBOUND_RESULT_COMPLETED_Y,
                NVL(C_INBOUND_RESULT_REDIRECTED_M, 0) AS C_INBOUND_RESULT_REDIRECTED_M,
                NVL(C_INBOUND_RESULT_REDIRECTED_M, 0) + NVL(C_INBOUND_RESULT_REDIRECTED_M2, 0) AS C_INBOUND_RESULT_REDIRECTED_Q,
                NVL(C_INBOUND_RESULT_REDIRECTED_M, 0) + NVL(C_INBOUND_RESULT_REDIRECTED_M11, 0) AS C_INBOUND_RESULT_REDIRECTED_Y,
                NVL(C_INBOUND_RESULT_CONFERENCED_M, 0) AS C_INBOUND_RESULT_CONFERENCED_M,
                NVL(C_INBOUND_RESULT_CONFERENCED_M, 0) + NVL(C_INBOUND_RESULT_CONFERENCED_M2, 0) AS C_INBOUND_RESULT_CONFERENCED_Q,
                NVL(C_INBOUND_RESULT_CONFERENCED_M, 0) + NVL(C_INBOUND_RESULT_CONFERENCED_M11, 0) AS C_INBOUND_RESULT_CONFERENCED_Y,
                NVL(C_INBOUND_RESULT_DIVERTED_M, 0) AS C_INBOUND_RESULT_DIVERTED_M,
                NVL(C_INBOUND_RESULT_DIVERTED_M, 0) + NVL(C_INBOUND_RESULT_DIVERTED_M2, 0) AS C_INBOUND_RESULT_DIVERTED_Q,
                NVL(C_INBOUND_RESULT_DIVERTED_M, 0) + NVL(C_INBOUND_RESULT_DIVERTED_M11, 0) AS C_INBOUND_RESULT_DIVERTED_Y,
                NVL(C_INBOUND_RESULT_ABANDONED_M, 0) AS C_INBOUND_RESULT_ABANDONED_M,
                NVL(C_INBOUND_RESULT_ABANDONED_M, 0) + NVL(C_INBOUND_RESULT_ABANDONED_M2, 0) AS C_INBOUND_RESULT_ABANDONED_Q,
                NVL(C_INBOUND_RESULT_ABANDONED_M, 0) + NVL(C_INBOUND_RESULT_ABANDONED_M11, 0) AS C_INBOUND_RESULT_ABANDONED_Y,
                NVL(C_INBOUND_RESULT_TRANSFERRED_M, 0) AS C_INBOUND_RESULT_TRANSFERRED_M,
                NVL(C_INBOUND_RESULT_TRANSFERRED_M, 0) + NVL(C_INBOUND_RESULT_TRANSFERRED_M2, 0) AS C_INBOUND_RESULT_TRANSFERRED_Q,
                NVL(C_INBOUND_RESULT_TRANSFERRED_M, 0) + NVL(C_INBOUND_RESULT_TRANSFERRED_M11, 0) AS C_INBOUND_RESULT_TRANSFERRED_Y,
                NVL(C_INBOUND_RESULT_DESTINATIONBUSY_M, 0) AS C_INBOUND_RESULT_DESTINATIONBUSY_M,
                NVL(C_INBOUND_RESULT_DESTINATIONBUSY_M, 0) + NVL(C_INBOUND_RESULT_DESTINATIONBUSY_M2, 0) AS C_INBOUND_RESULT_DESTINATIONBUSY_Q,
                NVL(C_INBOUND_RESULT_DESTINATIONBUSY_M, 0) + NVL(C_INBOUND_RESULT_DESTINATIONBUSY_M11, 0) AS C_INBOUND_RESULT_DESTINATIONBUSY_Y,
                NVL(C_INBOUND_RESULT_CUSTOMERABANDONED_M, 0) AS C_INBOUND_RESULT_CUSTOMERABANDONED_M,
                NVL(C_INBOUND_RESULT_CUSTOMERABANDONED_M, 0) + NVL(C_INBOUND_RESULT_CUSTOMERABANDONED_M2, 0) AS C_INBOUND_RESULT_CUSTOMERABANDONED_Q,
                NVL(C_INBOUND_RESULT_CUSTOMERABANDONED_M, 0) + NVL(C_INBOUND_RESULT_CUSTOMERABANDONED_M11, 0) AS C_INBOUND_RESULT_CUSTOMERABANDONED_Y,
                NVL(C_INBOUND_RESULT_ABNORMALSTOP_M, 0) AS C_INBOUND_RESULT_ABNORMALSTOP_M,
                NVL(C_INBOUND_RESULT_ABNORMALSTOP_M, 0) + NVL(C_INBOUND_RESULT_ABNORMALSTOP_M2, 0) AS C_INBOUND_RESULT_ABNORMALSTOP_Q,
                NVL(C_INBOUND_RESULT_ABNORMALSTOP_M, 0) + NVL(C_INBOUND_RESULT_ABNORMALSTOP_M11, 0) AS C_INBOUND_RESULT_ABNORMALSTOP_Y,
                NVL(C_OUTBOUND_RESULT_COMPLETED_M, 0) AS C_OUTBOUND_RESULT_COMPLETED_M,
                NVL(C_OUTBOUND_RESULT_COMPLETED_M, 0) + NVL(C_OUTBOUND_RESULT_COMPLETED_M2, 0) AS C_OUTBOUND_RESULT_COMPLETED_Q,
                NVL(C_OUTBOUND_RESULT_COMPLETED_M, 0) + NVL(C_OUTBOUND_RESULT_COMPLETED_M11, 0) AS C_OUTBOUND_RESULT_COMPLETED_Y,
                NVL(C_OUTBOUND_RESULT_REDIRECTED_M, 0) AS C_OUTBOUND_RESULT_REDIRECTED_M,
                NVL(C_OUTBOUND_RESULT_REDIRECTED_M, 0) + NVL(C_OUTBOUND_RESULT_REDIRECTED_M2, 0) AS C_OUTBOUND_RESULT_REDIRECTED_Q,
                NVL(C_OUTBOUND_RESULT_REDIRECTED_M, 0) + NVL(C_OUTBOUND_RESULT_REDIRECTED_M11, 0) AS C_OUTBOUND_RESULT_REDIRECTED_Y,
                NVL(C_OUTBOUND_RESULT_CONFERENCED_M, 0) AS C_OUTBOUND_RESULT_CONFERENCED_M,
                NVL(C_OUTBOUND_RESULT_CONFERENCED_M, 0) + NVL(C_OUTBOUND_RESULT_CONFERENCED_M2, 0) AS C_OUTBOUND_RESULT_CONFERENCED_Q,
                NVL(C_OUTBOUND_RESULT_CONFERENCED_M, 0) + NVL(C_OUTBOUND_RESULT_CONFERENCED_M11, 0) AS C_OUTBOUND_RESULT_CONFERENCED_Y,
                NVL(C_OUTBOUND_RESULT_DIVERTED_M, 0) AS C_OUTBOUND_RESULT_DIVERTED_M,
                NVL(C_OUTBOUND_RESULT_DIVERTED_M, 0) + NVL(C_OUTBOUND_RESULT_DIVERTED_M2, 0) AS C_OUTBOUND_RESULT_DIVERTED_Q,
                NVL(C_OUTBOUND_RESULT_DIVERTED_M, 0) + NVL(C_OUTBOUND_RESULT_DIVERTED_M11, 0) AS C_OUTBOUND_RESULT_DIVERTED_Y,
                NVL(C_OUTBOUND_RESULT_ABANDONED_M, 0) AS C_OUTBOUND_RESULT_ABANDONED_M,
                NVL(C_OUTBOUND_RESULT_ABANDONED_M, 0) + NVL(C_OUTBOUND_RESULT_ABANDONED_M2, 0) AS C_OUTBOUND_RESULT_ABANDONED_Q,
                NVL(C_OUTBOUND_RESULT_ABANDONED_M, 0) + NVL(C_OUTBOUND_RESULT_ABANDONED_M11, 0) AS C_OUTBOUND_RESULT_ABANDONED_Y,
                NVL(C_OUTBOUND_RESULT_TRANSFERRED_M, 0) AS C_OUTBOUND_RESULT_TRANSFERRED_M,
                NVL(C_OUTBOUND_RESULT_TRANSFERRED_M, 0) + NVL(C_OUTBOUND_RESULT_TRANSFERRED_M2, 0) AS C_OUTBOUND_RESULT_TRANSFERRED_Q,
                NVL(C_OUTBOUND_RESULT_TRANSFERRED_M, 0) + NVL(C_OUTBOUND_RESULT_TRANSFERRED_M11, 0) AS C_OUTBOUND_RESULT_TRANSFERRED_Y,
                NVL(C_OUTBOUND_RESULT_DESTINATIONBUSY_M, 0) AS C_OUTBOUND_RESULT_DESTINATIONBUSY_M,
                NVL(C_OUTBOUND_RESULT_DESTINATIONBUSY_M, 0) + NVL(C_OUTBOUND_RESULT_DESTINATIONBUSY_M2, 0) AS C_OUTBOUND_RESULT_DESTINATIONBUSY_Q,
                NVL(C_OUTBOUND_RESULT_DESTINATIONBUSY_M, 0) + NVL(C_OUTBOUND_RESULT_DESTINATIONBUSY_M11, 0) AS C_OUTBOUND_RESULT_DESTINATIONBUSY_Y,
                NVL(C_OUTBOUND_RESULT_CUSTOMERABANDONED_M, 0) AS C_OUTBOUND_RESULT_CUSTOMERABANDONED_M,
                NVL(C_OUTBOUND_RESULT_CUSTOMERABANDONED_M, 0) + NVL(C_OUTBOUND_RESULT_CUSTOMERABANDONED_M2, 0) AS C_OUTBOUND_RESULT_CUSTOMERABANDONED_Q,
                NVL(C_OUTBOUND_RESULT_CUSTOMERABANDONED_M, 0) + NVL(C_OUTBOUND_RESULT_CUSTOMERABANDONED_M11, 0) AS C_OUTBOUND_RESULT_CUSTOMERABANDONED_Y,
                NVL(C_OUTBOUND_RESULT_ABNORMALSTOP_M, 0) AS C_OUTBOUND_RESULT_ABNORMALSTOP_M,
                NVL(C_OUTBOUND_RESULT_ABNORMALSTOP_M, 0) + NVL(C_OUTBOUND_RESULT_ABNORMALSTOP_M2, 0) AS C_OUTBOUND_RESULT_ABNORMALSTOP_Q,
                NVL(C_OUTBOUND_RESULT_ABNORMALSTOP_M, 0) + NVL(C_OUTBOUND_RESULT_ABNORMALSTOP_M11, 0) AS C_OUTBOUND_RESULT_ABNORMALSTOP_Y,
                NVL(I_INBOUND_RESULT_COMPLETED_M, 0) AS I_INBOUND_RESULT_COMPLETED_M,
                GREATEST(NVL(I_INBOUND_RESULT_COMPLETED_M, 0), NVL(I_INBOUND_RESULT_COMPLETED_M2, 0)) AS I_INBOUND_RESULT_COMPLETED_Q,
                GREATEST(NVL(I_INBOUND_RESULT_COMPLETED_M, 0), NVL(I_INBOUND_RESULT_COMPLETED_M2, 0), NVL(I_INBOUND_RESULT_COMPLETED_M11, 0)) AS I_INBOUND_RESULT_COMPLETED_Y,
                NVL(I_INBOUND_RESULT_REDIRECTED_M, 0) AS I_INBOUND_RESULT_REDIRECTED_M,
                GREATEST(NVL(I_INBOUND_RESULT_REDIRECTED_M, 0), NVL(I_INBOUND_RESULT_REDIRECTED_M2, 0)) AS I_INBOUND_RESULT_REDIRECTED_Q,
                GREATEST(NVL(I_INBOUND_RESULT_REDIRECTED_M, 0), NVL(I_INBOUND_RESULT_REDIRECTED_M2, 0), NVL(I_INBOUND_RESULT_REDIRECTED_M11, 0)) AS I_INBOUND_RESULT_REDIRECTED_Y,
                NVL(I_INBOUND_RESULT_CONFERENCED_M, 0) AS I_INBOUND_RESULT_CONFERENCED_M,
                GREATEST(NVL(I_INBOUND_RESULT_CONFERENCED_M, 0), NVL(I_INBOUND_RESULT_CONFERENCED_M2, 0)) AS I_INBOUND_RESULT_CONFERENCED_Q,
                GREATEST(NVL(I_INBOUND_RESULT_CONFERENCED_M, 0), NVL(I_INBOUND_RESULT_CONFERENCED_M2, 0), NVL(I_INBOUND_RESULT_CONFERENCED_M11, 0)) AS I_INBOUND_RESULT_CONFERENCED_Y,
                NVL(I_INBOUND_RESULT_DIVERTED_M, 0) AS I_INBOUND_RESULT_DIVERTED_M,
                GREATEST(NVL(I_INBOUND_RESULT_DIVERTED_M, 0), NVL(I_INBOUND_RESULT_DIVERTED_M2, 0)) AS I_INBOUND_RESULT_DIVERTED_Q,
                GREATEST(NVL(I_INBOUND_RESULT_DIVERTED_M, 0), NVL(I_INBOUND_RESULT_DIVERTED_M2, 0), NVL(I_INBOUND_RESULT_DIVERTED_M11, 0)) AS I_INBOUND_RESULT_DIVERTED_Y,
                NVL(I_INBOUND_RESULT_ABANDONED_M, 0) AS I_INBOUND_RESULT_ABANDONED_M,
                GREATEST(NVL(I_INBOUND_RESULT_ABANDONED_M, 0), NVL(I_INBOUND_RESULT_ABANDONED_M2, 0)) AS I_INBOUND_RESULT_ABANDONED_Q,
                GREATEST(NVL(I_INBOUND_RESULT_ABANDONED_M, 0), NVL(I_INBOUND_RESULT_ABANDONED_M2, 0), NVL(I_INBOUND_RESULT_ABANDONED_M11, 0)) AS I_INBOUND_RESULT_ABANDONED_Y,
                NVL(I_INBOUND_RESULT_TRANSFERRED_M, 0) AS I_INBOUND_RESULT_TRANSFERRED_M,
                GREATEST(NVL(I_INBOUND_RESULT_TRANSFERRED_M, 0), NVL(I_INBOUND_RESULT_TRANSFERRED_M2, 0)) AS I_INBOUND_RESULT_TRANSFERRED_Q,
                GREATEST(NVL(I_INBOUND_RESULT_TRANSFERRED_M, 0), NVL(I_INBOUND_RESULT_TRANSFERRED_M2, 0), NVL(I_INBOUND_RESULT_TRANSFERRED_M11, 0)) AS I_INBOUND_RESULT_TRANSFERRED_Y,
                NVL(I_INBOUND_RESULT_DESTINATIONBUSY_M, 0) AS I_INBOUND_RESULT_DESTINATIONBUSY_M,
                GREATEST(NVL(I_INBOUND_RESULT_DESTINATIONBUSY_M, 0), NVL(I_INBOUND_RESULT_DESTINATIONBUSY_M2, 0)) AS I_INBOUND_RESULT_DESTINATIONBUSY_Q,
                GREATEST(NVL(I_INBOUND_RESULT_DESTINATIONBUSY_M, 0), NVL(I_INBOUND_RESULT_DESTINATIONBUSY_M2, 0), NVL(I_INBOUND_RESULT_DESTINATIONBUSY_M11, 0)) AS I_INBOUND_RESULT_DESTINATIONBUSY_Y,
                NVL(I_INBOUND_RESULT_CUSTOMERABANDONED_M, 0) AS I_INBOUND_RESULT_CUSTOMERABANDONED_M,
                GREATEST(NVL(I_INBOUND_RESULT_CUSTOMERABANDONED_M, 0), NVL(I_INBOUND_RESULT_CUSTOMERABANDONED_M2, 0)) AS I_INBOUND_RESULT_CUSTOMERABANDONED_Q,
                GREATEST(NVL(I_INBOUND_RESULT_CUSTOMERABANDONED_M, 0), NVL(I_INBOUND_RESULT_CUSTOMERABANDONED_M2, 0), NVL(I_INBOUND_RESULT_CUSTOMERABANDONED_M11, 0)) AS I_INBOUND_RESULT_CUSTOMERABANDONED_Y,
                NVL(I_INBOUND_RESULT_ABNORMALSTOP_M, 0) AS I_INBOUND_RESULT_ABNORMALSTOP_M,
                GREATEST(NVL(I_INBOUND_RESULT_ABNORMALSTOP_M, 0), NVL(I_INBOUND_RESULT_ABNORMALSTOP_M2, 0)) AS I_INBOUND_RESULT_ABNORMALSTOP_Q,
                GREATEST(NVL(I_INBOUND_RESULT_ABNORMALSTOP_M, 0), NVL(I_INBOUND_RESULT_ABNORMALSTOP_M2, 0), NVL(I_INBOUND_RESULT_ABNORMALSTOP_M11, 0)) AS I_INBOUND_RESULT_ABNORMALSTOP_Y,
                NVL(I_OUTBOUND_RESULT_COMPLETED_M, 0) AS I_OUTBOUND_RESULT_COMPLETED_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_COMPLETED_M, 0), NVL(I_OUTBOUND_RESULT_COMPLETED_M2, 0)) AS I_OUTBOUND_RESULT_COMPLETED_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_COMPLETED_M, 0), NVL(I_OUTBOUND_RESULT_COMPLETED_M2, 0), NVL(I_OUTBOUND_RESULT_COMPLETED_M11, 0)) AS I_OUTBOUND_RESULT_COMPLETED_Y,
                NVL(I_OUTBOUND_RESULT_REDIRECTED_M, 0) AS I_OUTBOUND_RESULT_REDIRECTED_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_REDIRECTED_M, 0), NVL(I_OUTBOUND_RESULT_REDIRECTED_M2, 0)) AS I_OUTBOUND_RESULT_REDIRECTED_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_REDIRECTED_M, 0), NVL(I_OUTBOUND_RESULT_REDIRECTED_M2, 0), NVL(I_OUTBOUND_RESULT_REDIRECTED_M11, 0)) AS I_OUTBOUND_RESULT_REDIRECTED_Y,
                NVL(I_OUTBOUND_RESULT_CONFERENCED_M, 0) AS I_OUTBOUND_RESULT_CONFERENCED_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_CONFERENCED_M, 0), NVL(I_OUTBOUND_RESULT_CONFERENCED_M2, 0)) AS I_OUTBOUND_RESULT_CONFERENCED_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_CONFERENCED_M, 0), NVL(I_OUTBOUND_RESULT_CONFERENCED_M2, 0), NVL(I_OUTBOUND_RESULT_CONFERENCED_M11, 0)) AS I_OUTBOUND_RESULT_CONFERENCED_Y,
                NVL(I_OUTBOUND_RESULT_DIVERTED_M, 0) AS I_OUTBOUND_RESULT_DIVERTED_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_DIVERTED_M, 0), NVL(I_OUTBOUND_RESULT_DIVERTED_M2, 0)) AS I_OUTBOUND_RESULT_DIVERTED_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_DIVERTED_M, 0), NVL(I_OUTBOUND_RESULT_DIVERTED_M2, 0), NVL(I_OUTBOUND_RESULT_DIVERTED_M11, 0)) AS I_OUTBOUND_RESULT_DIVERTED_Y,
                NVL(I_OUTBOUND_RESULT_ABANDONED_M, 0) AS I_OUTBOUND_RESULT_ABANDONED_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_ABANDONED_M, 0), NVL(I_OUTBOUND_RESULT_ABANDONED_M2, 0)) AS I_OUTBOUND_RESULT_ABANDONED_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_ABANDONED_M, 0), NVL(I_OUTBOUND_RESULT_ABANDONED_M2, 0), NVL(I_OUTBOUND_RESULT_ABANDONED_M11, 0)) AS I_OUTBOUND_RESULT_ABANDONED_Y,
                NVL(I_OUTBOUND_RESULT_TRANSFERRED_M, 0) AS I_OUTBOUND_RESULT_TRANSFERRED_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_TRANSFERRED_M, 0), NVL(I_OUTBOUND_RESULT_TRANSFERRED_M2, 0)) AS I_OUTBOUND_RESULT_TRANSFERRED_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_TRANSFERRED_M, 0), NVL(I_OUTBOUND_RESULT_TRANSFERRED_M2, 0), NVL(I_OUTBOUND_RESULT_TRANSFERRED_M11, 0)) AS I_OUTBOUND_RESULT_TRANSFERRED_Y,
                NVL(I_OUTBOUND_RESULT_DESTINATIONBUSY_M, 0) AS I_OUTBOUND_RESULT_DESTINATIONBUSY_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_DESTINATIONBUSY_M, 0), NVL(I_OUTBOUND_RESULT_DESTINATIONBUSY_M2, 0)) AS I_OUTBOUND_RESULT_DESTINATIONBUSY_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_DESTINATIONBUSY_M, 0), NVL(I_OUTBOUND_RESULT_DESTINATIONBUSY_M2, 0), NVL(I_OUTBOUND_RESULT_DESTINATIONBUSY_M11, 0)) AS I_OUTBOUND_RESULT_DESTINATIONBUSY_Y,
                NVL(I_OUTBOUND_RESULT_CUSTOMERABANDONED_M, 0) AS I_OUTBOUND_RESULT_CUSTOMERABANDONED_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_CUSTOMERABANDONED_M, 0), NVL(I_OUTBOUND_RESULT_CUSTOMERABANDONED_M2, 0)) AS I_OUTBOUND_RESULT_CUSTOMERABANDONED_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_CUSTOMERABANDONED_M, 0), NVL(I_OUTBOUND_RESULT_CUSTOMERABANDONED_M2, 0), NVL(I_OUTBOUND_RESULT_CUSTOMERABANDONED_M11, 0)) AS I_OUTBOUND_RESULT_CUSTOMERABANDONED_Y,
                NVL(I_OUTBOUND_RESULT_ABNORMALSTOP_M, 0) AS I_OUTBOUND_RESULT_ABNORMALSTOP_M,
                GREATEST(NVL(I_OUTBOUND_RESULT_ABNORMALSTOP_M, 0), NVL(I_OUTBOUND_RESULT_ABNORMALSTOP_M2, 0)) AS I_OUTBOUND_RESULT_ABNORMALSTOP_Q,
                GREATEST(NVL(I_OUTBOUND_RESULT_ABNORMALSTOP_M, 0), NVL(I_OUTBOUND_RESULT_ABNORMALSTOP_M2, 0), NVL(I_OUTBOUND_RESULT_ABNORMALSTOP_M11, 0)) AS I_OUTBOUND_RESULT_ABNORMALSTOP_Y,
                NVL(RATIO_INBOUND_VOICE_COMPLETED_M, 0) AS RATIO_INBOUND_VOICE_COMPLETED_M,
                ROUND((NVL(RATIO_INBOUND_VOICE_COMPLETED_M, 0) + NVL(RATIO_INBOUND_VOICE_COMPLETED_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0)) / main.c_month_q, 2)  AS RATIO_INBOUND_VOICE_COMPLETED_Q,
                ROUND((NVL(RATIO_INBOUND_VOICE_COMPLETED_M, 0) + NVL(RATIO_INBOUND_VOICE_COMPLETED_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0)) / main.c_month_y, 2)  AS RATIO_INBOUND_VOICE_COMPLETED_Y,
                NVL(RATIO_INBOUND_VOICE_ABANDONED_M, 0) AS RATIO_INBOUND_VOICE_ABANDONED_M,
                ROUND((NVL(RATIO_INBOUND_VOICE_ABANDONED_M, 0) + NVL(RATIO_INBOUND_VOICE_ABANDONED_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0)) / main.c_month_q, 2) AS RATIO_INBOUND_VOICE_ABANDONED_Q,
                ROUND((NVL(RATIO_INBOUND_VOICE_ABANDONED_M, 0) + NVL(RATIO_INBOUND_VOICE_ABANDONED_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0)) / main.c_month_y, 2) AS RATIO_INBOUND_VOICE_ABANDONED_Y,
                NVL(RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M, 0) AS RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M,
                ROUND((NVL(RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M, 0) + NVL(RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0)) / main.c_month_q, 2) AS RATIO_INBOUND_VOICE_CUSTOMERABANDONED_Q,
                ROUND((NVL(RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M, 0) + NVL(RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0)) / main.c_month_y, 2) AS RATIO_INBOUND_VOICE_CUSTOMERABANDONED_Y,
                NVL(RATIO_OUTBOUND_VOICE_COMPLETED_M, 0) AS RATIO_OUTBOUND_VOICE_COMPLETED_M,
                ROUND((NVL(RATIO_OUTBOUND_VOICE_COMPLETED_M, 0) + NVL(RATIO_OUTBOUND_VOICE_COMPLETED_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0)) / main.c_month_q, 2) AS RATIO_OUTBOUND_VOICE_COMPLETED_Q,
                ROUND((NVL(RATIO_OUTBOUND_VOICE_COMPLETED_M, 0) + NVL(RATIO_OUTBOUND_VOICE_COMPLETED_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0)) / main.c_month_y, 2) AS RATIO_OUTBOUND_VOICE_COMPLETED_Y,
                NVL(RATIO_OUTBOUND_VOICE_ABANDONED_M, 0) AS RATIO_OUTBOUND_VOICE_ABANDONED_M,
                ROUND((NVL(RATIO_OUTBOUND_VOICE_ABANDONED_M, 0) + NVL(RATIO_OUTBOUND_VOICE_ABANDONED_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0)) / main.c_month_q, 2) AS RATIO_OUTBOUND_VOICE_ABANDONED_Q,
                ROUND((NVL(RATIO_OUTBOUND_VOICE_ABANDONED_M, 0) + NVL(RATIO_OUTBOUND_VOICE_ABANDONED_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0)) / main.c_month_y, 2) AS RATIO_OUTBOUND_VOICE_ABANDONED_Y,
                NVL(RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M, 0) AS RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M,
                ROUND((NVL(RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M, 0) + NVL(RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0)) / main.c_month_q, 2) AS RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_Q,
                ROUND((NVL(RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M, 0) + NVL(RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0)) / main.c_month_y, 2) AS RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_Y,
                NVL(RATIO_INBOUND_EMAIL_M, 0) AS RATIO_INBOUND_EMAIL_M,
                ROUND((NVL(RATIO_INBOUND_EMAIL_M, 0) + NVL(RATIO_INBOUND_EMAIL_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0)) / main.c_month_q, 2) AS RATIO_INBOUND_EMAIL_Q,
                ROUND((NVL(RATIO_INBOUND_EMAIL_M, 0) + NVL(RATIO_INBOUND_EMAIL_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0)) / main.c_month_y, 2) AS RATIO_INBOUND_EMAIL_Y,
                NVL(RATIO_INBOUND_VOICE_M, 0) AS RATIO_INBOUND_VOICE_M,
                ROUND((NVL(RATIO_INBOUND_VOICE_M, 0) + NVL(RATIO_INBOUND_VOICE_M2, 0) * GREATEST(LEAST(main.c_month_q - 1, 2), 0)) / main.c_month_q, 2) AS RATIO_INBOUND_VOICE_Q,
                ROUND((NVL(RATIO_INBOUND_VOICE_M, 0) + NVL(RATIO_INBOUND_VOICE_M11, 0) * GREATEST(LEAST(main.c_month_y - 1, 11), 0)) / main.c_month_y, 2) AS RATIO_INBOUND_VOICE_Y,
                COALESCE(RECENCY_INBOUND_EMAIL, RECENCY_INBOUND_EMAIL_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_INBOUND_EMAIL,
                COALESCE(RECENCY_INBOUND_VOICE, RECENCY_INBOUND_VOICE_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_INBOUND_VOICE,
                COALESCE(RECENCY_OUTBOUND_EMAIL, RECENCY_OUTBOUND_EMAIL_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_OUTBOUND_EMAIL,
                COALESCE(RECENCY_OUTBOUND_VOICE, RECENCY_OUTBOUND_VOICE_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_OUTBOUND_VOICE,
                NVL(AVG_FEEDBACK_M, 10) AS AVG_FEEDBACK_M,
                ROUND((NVL(AVG_FEEDBACK_M, 10) * NVL(I_FEEDBACK_M, 0) + NVL(AVG_FEEDBACK_M2, 0)) / NVL(NULLIF(NVL(SUM_I_FEEDBACK_M2, 0) + NVL(I_FEEDBACK_M, 0), 0), 1)) AS AVG_FEEDBACK_Q,
                ROUND((NVL(AVG_FEEDBACK_M, 10) * NVL(I_FEEDBACK_M, 0) + NVL(AVG_FEEDBACK_M11, 0)) / NVL(NULLIF(NVL(SUM_I_FEEDBACK_M11, 0) + NVL(I_FEEDBACK_M, 0), 0), 1)) AS AVG_FEEDBACK_Y,
                NVL(MIN_FEEDBACK_M, 10) AS MIN_FEEDBACK_M,
                COALESCE(LEAST(MIN_FEEDBACK_M, MIN_FEEDBACK_M2), MIN_FEEDBACK_M, MIN_FEEDBACK_M2, 10) AS MIN_FEEDBACK_Q,
                COALESCE(LEAST(MIN_FEEDBACK_M, MIN_FEEDBACK_M11), MIN_FEEDBACK_M, MIN_FEEDBACK_M11, 10) AS MIN_FEEDBACK_Y,
                NVL(MAX_FEEDBACK_M, 10) AS MAX_FEEDBACK_M,
                COALESCE(GREATEST(MAX_FEEDBACK_M, MAX_FEEDBACK_M2), MAX_FEEDBACK_M, MAX_FEEDBACK_M2, 10) AS MAX_FEEDBACK_Q,
                COALESCE(GREATEST(MAX_FEEDBACK_M, MAX_FEEDBACK_M11), MAX_FEEDBACK_M, MAX_FEEDBACK_M11, 10) AS MAX_FEEDBACK_Y,
                NVL(C_FEEDBACK_M, 0) AS C_FEEDBACK_M,
                NVL(C_FEEDBACK_M, 0) + NVL(C_FEEDBACK_M2, 0) AS C_FEEDBACK_Q,
                NVL(C_FEEDBACK_M, 0) + NVL(C_FEEDBACK_M11, 0) AS C_FEEDBACK_Y,
                NVL(I_FEEDBACK_M, 0) AS I_FEEDBACK_M,
                GREATEST(NVL(I_FEEDBACK_M, 0), NVL(I_FEEDBACK_M2, 0)) AS I_FEEDBACK_Q,
                GREATEST(NVL(I_FEEDBACK_M, 0), NVL(I_FEEDBACK_M11, 0)) AS I_FEEDBACK_Y,
                COALESCE(RECENCY_FEEDBACK, RECENCY_FEEDBACK_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_FEEDBACK,
                NVL(C_SERVICE_REQUEST_M, 0) AS C_SERVICE_REQUEST_M,
                NVL(C_SERVICE_REQUEST_M, 0) + NVL(C_SERVICE_REQUEST_M2, 0) AS C_SERVICE_REQUEST_Q,
                NVL(C_SERVICE_REQUEST_M, 0) + NVL(C_SERVICE_REQUEST_M11, 0) AS C_SERVICE_REQUEST_Y,
                ROUND(DECODE(NVL(C_SERVICE_REQUEST_M, 0) + NVL(C_SERVICE_REQUEST_M2, 0), 0, 1/3, NVL(C_SERVICE_REQUEST_M, 0)/(NVL(C_SERVICE_REQUEST_M, 0) + NVL(C_SERVICE_REQUEST_M2, 0))), 2) AS RATIO_DELTA_C_SERVICE_REQUEST_MQ,
                ROUND(DECODE(NVL(C_SERVICE_REQUEST_M, 0) + NVL(C_SERVICE_REQUEST_M11, 0), 0, 1/12, NVL(C_SERVICE_REQUEST_M, 0)/(NVL(C_SERVICE_REQUEST_M, 0) + NVL(C_SERVICE_REQUEST_M11, 0))), 2) AS RATIO_DELTA_C_SERVICE_REQUEST_MY,
                NVL(I_SERVICE_REQUEST_M, 0) AS I_SERVICE_REQUEST_M,
                GREATEST(NVL(I_SERVICE_REQUEST_M, 0), NVL(I_SERVICE_REQUEST_M2, 0)) AS I_SERVICE_REQUEST_Q,
                GREATEST(NVL(I_SERVICE_REQUEST_M, 0), NVL(I_SERVICE_REQUEST_M11, 0)) AS I_SERVICE_REQUEST_Y,
                NVL(I_BRANCH_VISIT_SALE_M, 0) AS I_BRANCH_VISIT_SALE_M,
                GREATEST(NVL(I_BRANCH_VISIT_SALE_M, 0), NVL(I_BRANCH_VISIT_SALE_M2, 0)) AS I_BRANCH_VISIT_SALE_Q,
                GREATEST(NVL(I_BRANCH_VISIT_SALE_M, 0), NVL(I_BRANCH_VISIT_SALE_M11, 0)) AS I_BRANCH_VISIT_SALE_Y,
                NVL(I_BRANCH_VISIT_CARE_M, 0) AS I_BRANCH_VISIT_CARE_M,
                GREATEST(NVL(I_BRANCH_VISIT_CARE_M, 0), NVL(I_BRANCH_VISIT_CARE_M2, 0)) AS I_BRANCH_VISIT_CARE_Q,
                GREATEST(NVL(I_BRANCH_VISIT_CARE_M, 0), NVL(I_BRANCH_VISIT_CARE_M11, 0)) AS I_BRANCH_VISIT_CARE_Y,
                NVL(I_BRANCH_VISIT_INFORMATION_M, 0) AS I_BRANCH_VISIT_INFORMATION_M,
                GREATEST(NVL(I_BRANCH_VISIT_INFORMATION_M, 0), NVL(I_BRANCH_VISIT_INFORMATION_M2, 0)) AS I_BRANCH_VISIT_INFORMATION_Q,
                GREATEST(NVL(I_BRANCH_VISIT_INFORMATION_M, 0), NVL(I_BRANCH_VISIT_INFORMATION_M11, 0)) AS I_BRANCH_VISIT_INFORMATION_Y,
                NVL(I_BRANCH_VISIT_CASH_M, 0) AS I_BRANCH_VISIT_CASH_M,
                GREATEST(NVL(I_BRANCH_VISIT_CASH_M, 0), NVL(I_BRANCH_VISIT_CASH_M2, 0)) AS I_BRANCH_VISIT_CASH_Q,
                GREATEST(NVL(I_BRANCH_VISIT_CASH_M, 0), NVL(I_BRANCH_VISIT_CASH_M11, 0)) AS I_BRANCH_VISIT_CASH_Y,
                NVL(I_BRANCH_VISIT_OTHER_M, 0) AS I_BRANCH_VISIT_OTHER_M,
                GREATEST(NVL(I_BRANCH_VISIT_OTHER_M, 0), NVL(I_BRANCH_VISIT_OTHER_M2, 0)) AS I_BRANCH_VISIT_OTHER_Q,
                GREATEST(NVL(I_BRANCH_VISIT_OTHER_M, 0), NVL(I_BRANCH_VISIT_OTHER_M11, 0)) AS I_BRANCH_VISIT_OTHER_Y,
                NVL(C_BRANCH_VISIT_SALE_M, 0) AS C_BRANCH_VISIT_SALE_M,
                NVL(C_BRANCH_VISIT_SALE_M, 0) + NVL(C_BRANCH_VISIT_SALE_M2, 0) AS C_BRANCH_VISIT_SALE_Q,
                NVL(C_BRANCH_VISIT_SALE_M, 0) + NVL(C_BRANCH_VISIT_SALE_M11, 0) AS C_BRANCH_VISIT_SALE_Y,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_SALE_M, 0) + NVL(C_BRANCH_VISIT_SALE_M2, 0), 0, 1/3, NVL(C_BRANCH_VISIT_SALE_M, 0)/(NVL(C_BRANCH_VISIT_SALE_M, 0) + NVL(C_BRANCH_VISIT_SALE_M2, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_SALE_MQ,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_SALE_M, 0) + NVL(C_BRANCH_VISIT_SALE_M11, 0), 0, 1/12, NVL(C_BRANCH_VISIT_SALE_M, 0)/(NVL(C_BRANCH_VISIT_SALE_M, 0) + NVL(C_BRANCH_VISIT_SALE_M11, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_SALE_MY,
                NVL(C_BRANCH_VISIT_CARE_M, 0) AS C_BRANCH_VISIT_CARE_M,
                NVL(C_BRANCH_VISIT_CARE_M, 0) + NVL(C_BRANCH_VISIT_CARE_M2, 0) AS C_BRANCH_VISIT_CARE_Q,
                NVL(C_BRANCH_VISIT_CARE_M, 0) + NVL(C_BRANCH_VISIT_CARE_M11, 0) AS C_BRANCH_VISIT_CARE_Y,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_CARE_M, 0) + NVL(C_BRANCH_VISIT_CARE_M2, 0), 0, 1/3, NVL(C_BRANCH_VISIT_CARE_M, 0)/(NVL(C_BRANCH_VISIT_CARE_M, 0) + NVL(C_BRANCH_VISIT_CARE_M2, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_CARE_MQ,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_CARE_M, 0) + NVL(C_BRANCH_VISIT_CARE_M11, 0), 0, 1/12, NVL(C_BRANCH_VISIT_CARE_M, 0)/(NVL(C_BRANCH_VISIT_CARE_M, 0) + NVL(C_BRANCH_VISIT_CARE_M11, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_CARE_MY,
                NVL(C_BRANCH_VISIT_INFORMATION_M, 0) AS C_BRANCH_VISIT_INFORMATION_M,
                NVL(C_BRANCH_VISIT_INFORMATION_M, 0) + NVL(C_BRANCH_VISIT_INFORMATION_M2, 0) AS C_BRANCH_VISIT_INFORMATION_Q,
                NVL(C_BRANCH_VISIT_INFORMATION_M, 0) + NVL(C_BRANCH_VISIT_INFORMATION_M11, 0) AS C_BRANCH_VISIT_INFORMATION_Y,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_INFORMATION_M, 0) + NVL(C_BRANCH_VISIT_INFORMATION_M2, 0), 0, 1/3, NVL(C_BRANCH_VISIT_INFORMATION_M, 0)/(NVL(C_BRANCH_VISIT_INFORMATION_M, 0) + NVL(C_BRANCH_VISIT_INFORMATION_M2, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_INFORMATION_MQ,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_INFORMATION_M, 0) + NVL(C_BRANCH_VISIT_INFORMATION_M11, 0), 0, 1/12, NVL(C_BRANCH_VISIT_INFORMATION_M, 0)/(NVL(C_BRANCH_VISIT_INFORMATION_M, 0) + NVL(C_BRANCH_VISIT_INFORMATION_M11, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_INFORMATION_MY,
                NVL(C_BRANCH_VISIT_CASH_M, 0) AS C_BRANCH_VISIT_CASH_M,
                NVL(C_BRANCH_VISIT_CASH_M, 0) + NVL(C_BRANCH_VISIT_CASH_M2, 0) AS C_BRANCH_VISIT_CASH_Q,
                NVL(C_BRANCH_VISIT_CASH_M, 0) + NVL(C_BRANCH_VISIT_CASH_M11, 0) AS C_BRANCH_VISIT_CASH_Y,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_CASH_M, 0) + NVL(C_BRANCH_VISIT_CASH_M2, 0), 0, 1/3, NVL(C_BRANCH_VISIT_CASH_M, 0)/(NVL(C_BRANCH_VISIT_CASH_M, 0) + NVL(C_BRANCH_VISIT_CASH_M2, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_CASH_MQ,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_CASH_M, 0) + NVL(C_BRANCH_VISIT_CASH_M11, 0), 0, 1/12, NVL(C_BRANCH_VISIT_CASH_M, 0)/(NVL(C_BRANCH_VISIT_CASH_M, 0) + NVL(C_BRANCH_VISIT_CASH_M11, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_CASH_MY,
                NVL(C_BRANCH_VISIT_OTHER_M, 0) AS C_BRANCH_VISIT_OTHER_M,
                NVL(C_BRANCH_VISIT_OTHER_M, 0) + NVL(C_BRANCH_VISIT_OTHER_M2, 0) AS C_BRANCH_VISIT_OTHER_Q,
                NVL(C_BRANCH_VISIT_OTHER_M, 0) + NVL(C_BRANCH_VISIT_OTHER_M11, 0) AS C_BRANCH_VISIT_OTHER_Y,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_OTHER_M, 0) + NVL(C_BRANCH_VISIT_OTHER_M2, 0), 0, 1/3, NVL(C_BRANCH_VISIT_OTHER_M, 0)/(NVL(C_BRANCH_VISIT_OTHER_M, 0) + NVL(C_BRANCH_VISIT_OTHER_M2, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_OTHER_MQ,
                ROUND(DECODE(NVL(C_BRANCH_VISIT_OTHER_M, 0) + NVL(C_BRANCH_VISIT_OTHER_M11, 0), 0, 1/12, NVL(C_BRANCH_VISIT_OTHER_M, 0)/(NVL(C_BRANCH_VISIT_OTHER_M, 0) + NVL(C_BRANCH_VISIT_OTHER_M11, 0))), 2) AS RATIO_DELTA_C_BRANCH_VISIT_OTHER_MY,
                COALESCE(RECENCY_BRANCH_VISIT_SALE, RECENCY_BRANCH_VISIT_SALE_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_BRANCH_VISIT_SALE,
                COALESCE(RECENCY_BRANCH_VISIT_CARE, RECENCY_BRANCH_VISIT_CARE_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_BRANCH_VISIT_CARE,
                COALESCE(RECENCY_BRANCH_VISIT_INFORMATION, RECENCY_BRANCH_VISIT_INFORMATION_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_BRANCH_VISIT_INFORMATION,
                COALESCE(RECENCY_BRANCH_VISIT_CASH, RECENCY_BRANCH_VISIT_CASH_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_BRANCH_VISIT_CASH,
                COALESCE(RECENCY_BRANCH_VISIT_OTHER, RECENCY_BRANCH_VISIT_OTHER_PREV + process_dt - TRUNC(process_dt, 'MM') + 1, process_dt - process_dt_hist) AS RECENCY_BRANCH_VISIT_OTHER
            FROM ( -- main base
                        SELECT
                            base.month_id,
                            base.customerid,
                            base.bank_tenure,
                            LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -3)),1)*3,
                                  COALESCE(int_q.c_month, 0)+1) c_month_q,
                            LEAST(GREATEST(base.bank_tenure/(process_dt - ADD_MONTHS(process_dt, -12)),1)*12,
                                  COALESCE(int_y.c_month, 0)+1) c_month_y       
                        FROM pm_owner.attr_client_base base
                        LEFT JOIN (
                            SELECT
                                customerid,
                                COUNT(DISTINCT month_id) c_month
                            FROM PM_OWNER.attr_interaction
                            WHERE month_id BETWEEN TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-2), 'YYYYMM'))
                                                AND TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                            GROUP BY customerid) int_q
                            ON int_q.customerid = base.customerid
    
                        LEFT JOIN (
                            SELECT
                                customerid,
                                COUNT(DISTINCT month_id) c_month
                            FROM PM_OWNER.attr_interaction
                            WHERE month_id BETWEEN TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-11), 'YYYYMM'))
                                                AND TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt,-1), 'YYYYMM'))
                            GROUP BY customerid) int_y
                            ON int_y.customerid = base.customerid                   
                        WHERE base.month_id = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))
                        ) main
    
            LEFT JOIN ( -- Aggregated call_centrum_history
                SELECT
                    customerid,
                    TO_NUMBER(TO_CHAR(event_dt, 'YYYYMM')) as month_id,
                    ROUND(AVG(DECODE(interaction_type, 'Inbound', EMAIL_DURATION))) AS AVG_INBOUND_EMAIL_DURATION_M,
                    ROUND(AVG(DECODE(interaction_type, 'Inbound', VOICE_DURATION))) AS AVG_INBOUND_VOICE_DURATION_M,
                    ROUND(AVG(DECODE(interaction_type, 'Outbound', EMAIL_DURATION))) AS AVG_OUTBOUND_EMAIL_DURATION_M,
                    ROUND(AVG(DECODE(interaction_type, 'Outbound', VOICE_DURATION))) AS AVG_OUTBOUND_VOICE_DURATION_M,
                    SUM(DECODE(interaction_type, 'Inbound', EMAIL_DURATION)) AS SUM_INBOUND_EMAIL_DURATION_M,
                    SUM(DECODE(interaction_type, 'Inbound', VOICE_DURATION)) AS SUM_INBOUND_VOICE_DURATION_M,
                    SUM(DECODE(interaction_type, 'Outbound', EMAIL_DURATION)) AS SUM_OUTBOUND_EMAIL_DURATION_M,
                    SUM(DECODE(interaction_type, 'Outbound', VOICE_DURATION)) AS SUM_OUTBOUND_VOICE_DURATION_M,
                    COUNT(I_INBOUND_EMAIL) AS C_INBOUND_EMAIL_M,
                    COUNT(I_INBOUND_VOICE) AS C_INBOUND_VOICE_M,
                    COUNT(I_OUTBOUND_EMAIL) AS C_OUTBOUND_EMAIL_M,
                    COUNT(I_OUTBOUND_VOICE) AS C_OUTBOUND_VOICE_M,
                    MAX(I_INBOUND_EMAIL) AS I_INBOUND_EMAIL_M,
                    MAX(I_INBOUND_VOICE) AS I_INBOUND_VOICE_M,
                    MAX(I_OUTBOUND_EMAIL) AS I_OUTBOUND_EMAIL_M,
                    MAX(I_OUTBOUND_VOICE) AS I_OUTBOUND_VOICE_M,
                    SUM(I_INBOUND_RESULT_COMPLETED) AS C_INBOUND_RESULT_COMPLETED_M,
                    SUM(I_INBOUND_RESULT_REDIRECTED) AS C_INBOUND_RESULT_REDIRECTED_M,
                    SUM(I_INBOUND_RESULT_CONFERENCED) AS C_INBOUND_RESULT_CONFERENCED_M,
                    SUM(I_INBOUND_RESULT_DIVERTED) AS C_INBOUND_RESULT_DIVERTED_M,
                    SUM(I_INBOUND_RESULT_ABANDONED) AS C_INBOUND_RESULT_ABANDONED_M,
                    SUM(I_INBOUND_RESULT_TRANSFERRED) AS C_INBOUND_RESULT_TRANSFERRED_M,
                    SUM(I_INBOUND_RESULT_DESTINATIONBUSY) AS C_INBOUND_RESULT_DESTINATIONBUSY_M,
                    SUM(I_INBOUND_RESULT_CUSTOMERABANDONED) AS C_INBOUND_RESULT_CUSTOMERABANDONED_M,
                    SUM(I_INBOUND_RESULT_ABNORMALSTOP) AS C_INBOUND_RESULT_ABNORMALSTOP_M,
                    SUM(I_OUTBOUND_RESULT_COMPLETED) AS C_OUTBOUND_RESULT_COMPLETED_M,
                    SUM(I_OUTBOUND_RESULT_REDIRECTED) AS C_OUTBOUND_RESULT_REDIRECTED_M,
                    SUM(I_OUTBOUND_RESULT_CONFERENCED) AS C_OUTBOUND_RESULT_CONFERENCED_M,
                    SUM(I_OUTBOUND_RESULT_DIVERTED) AS C_OUTBOUND_RESULT_DIVERTED_M,
                    SUM(I_OUTBOUND_RESULT_ABANDONED) AS C_OUTBOUND_RESULT_ABANDONED_M,
                    SUM(I_OUTBOUND_RESULT_TRANSFERRED) AS C_OUTBOUND_RESULT_TRANSFERRED_M,
                    SUM(I_OUTBOUND_RESULT_DESTINATIONBUSY) AS C_OUTBOUND_RESULT_DESTINATIONBUSY_M,
                    SUM(I_OUTBOUND_RESULT_CUSTOMERABANDONED) AS C_OUTBOUND_RESULT_CUSTOMERABANDONED_M,
                    SUM(I_OUTBOUND_RESULT_ABNORMALSTOP) AS C_OUTBOUND_RESULT_ABNORMALSTOP_M,
                    MAX(I_INBOUND_RESULT_COMPLETED) AS I_INBOUND_RESULT_COMPLETED_M,
                    MAX(I_INBOUND_RESULT_REDIRECTED) AS I_INBOUND_RESULT_REDIRECTED_M,
                    MAX(I_INBOUND_RESULT_CONFERENCED) AS I_INBOUND_RESULT_CONFERENCED_M,
                    MAX(I_INBOUND_RESULT_DIVERTED) AS I_INBOUND_RESULT_DIVERTED_M,
                    MAX(I_INBOUND_RESULT_ABANDONED) AS I_INBOUND_RESULT_ABANDONED_M,
                    MAX(I_INBOUND_RESULT_TRANSFERRED) AS I_INBOUND_RESULT_TRANSFERRED_M,
                    MAX(I_INBOUND_RESULT_DESTINATIONBUSY) AS I_INBOUND_RESULT_DESTINATIONBUSY_M,
                    MAX(I_INBOUND_RESULT_CUSTOMERABANDONED) AS I_INBOUND_RESULT_CUSTOMERABANDONED_M,
                    MAX(I_INBOUND_RESULT_ABNORMALSTOP) AS I_INBOUND_RESULT_ABNORMALSTOP_M,
                    MAX(I_OUTBOUND_RESULT_COMPLETED) AS I_OUTBOUND_RESULT_COMPLETED_M,
                    MAX(I_OUTBOUND_RESULT_REDIRECTED) AS I_OUTBOUND_RESULT_REDIRECTED_M,
                    MAX(I_OUTBOUND_RESULT_CONFERENCED) AS I_OUTBOUND_RESULT_CONFERENCED_M,
                    MAX(I_OUTBOUND_RESULT_DIVERTED) AS I_OUTBOUND_RESULT_DIVERTED_M,
                    MAX(I_OUTBOUND_RESULT_ABANDONED) AS I_OUTBOUND_RESULT_ABANDONED_M,
                    MAX(I_OUTBOUND_RESULT_TRANSFERRED) AS I_OUTBOUND_RESULT_TRANSFERRED_M,
                    MAX(I_OUTBOUND_RESULT_DESTINATIONBUSY) AS I_OUTBOUND_RESULT_DESTINATIONBUSY_M,
                    MAX(I_OUTBOUND_RESULT_CUSTOMERABANDONED) AS I_OUTBOUND_RESULT_CUSTOMERABANDONED_M,
                    MAX(I_OUTBOUND_RESULT_ABNORMALSTOP) AS I_OUTBOUND_RESULT_ABNORMALSTOP_M,
                    ROUND(SUM(DECODE(media_type, 'Voice', I_INBOUND_RESULT_COMPLETED))/SUM(I_INBOUND_VOICE), 2) AS RATIO_INBOUND_VOICE_COMPLETED_M,
                    ROUND(SUM(DECODE(media_type, 'Voice', I_INBOUND_RESULT_ABANDONED))/SUM(I_INBOUND_VOICE), 2) AS RATIO_INBOUND_VOICE_ABANDONED_M,
                    ROUND(SUM(DECODE(media_type, 'Voice', I_INBOUND_RESULT_CUSTOMERABANDONED))/SUM(I_INBOUND_VOICE), 2) AS RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M,
                    ROUND(SUM(DECODE(media_type, 'Voice', I_OUTBOUND_RESULT_COMPLETED))/SUM(I_OUTBOUND_VOICE), 2) AS RATIO_OUTBOUND_VOICE_COMPLETED_M,
                    ROUND(SUM(DECODE(media_type, 'Voice', I_OUTBOUND_RESULT_ABANDONED))/SUM(I_OUTBOUND_VOICE), 2) AS RATIO_OUTBOUND_VOICE_ABANDONED_M,
                    ROUND(SUM(DECODE(media_type, 'Voice', I_OUTBOUND_RESULT_CUSTOMERABANDONED))/SUM(I_OUTBOUND_VOICE), 2) AS RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M,
                    ROUND(SUM(I_INBOUND_EMAIL)/SUM(DECODE(media_type, 'Email', 1)), 2) AS RATIO_INBOUND_EMAIL_M,
                    ROUND(SUM(I_INBOUND_VOICE)/SUM(DECODE(media_type, 'Voice', 1)), 2) AS RATIO_INBOUND_VOICE_M,
                    MIN(process_dt + 1 - DECODE(I_INBOUND_EMAIL, 1, TRUNC(event_dt))) AS RECENCY_INBOUND_EMAIL,
                    MIN(process_dt + 1 - DECODE(I_INBOUND_VOICE, 1, TRUNC(event_dt))) AS RECENCY_INBOUND_VOICE,
                    MIN(process_dt + 1 - DECODE(I_OUTBOUND_EMAIL, 1, TRUNC(event_dt))) AS RECENCY_OUTBOUND_EMAIL,
                    MIN(process_dt + 1 - DECODE(I_OUTBOUND_VOICE, 1, TRUNC(event_dt))) AS RECENCY_OUTBOUND_VOICE
                FROM (
                    select 
                        cch.*,
                        DECODE(media_type, 'Email', total_duration) AS EMAIL_DURATION,
                        DECODE(media_type, 'Voice', total_duration) AS VOICE_DURATION,
                        DECODE(interaction_type, 'Inbound', DECODE(media_type, 'Email', 1)) AS I_INBOUND_EMAIL,
                        DECODE(interaction_type, 'Inbound', DECODE(media_type, 'Voice', 1)) AS I_INBOUND_VOICE,
                        DECODE(interaction_type, 'Outbound', DECODE(media_type, 'Email', 1)) AS I_OUTBOUND_EMAIL,
                        DECODE(interaction_type, 'Outbound', DECODE(media_type, 'Voice', 1)) AS I_OUTBOUND_VOICE,
                        DECODE(interaction_type, 'Inbound', I_RESULT_COMPLETED, 0) AS I_INBOUND_RESULT_COMPLETED,
                        DECODE(interaction_type, 'Inbound', I_RESULT_REDIRECTED, 0) AS I_INBOUND_RESULT_REDIRECTED,
                        DECODE(interaction_type, 'Inbound', I_RESULT_CONFERENCED, 0) AS I_INBOUND_RESULT_CONFERENCED,
                        DECODE(interaction_type, 'Inbound', I_RESULT_DIVERTED, 0) AS I_INBOUND_RESULT_DIVERTED,
                        DECODE(interaction_type, 'Inbound', I_RESULT_ABANDONED, 0) AS I_INBOUND_RESULT_ABANDONED,
                        DECODE(interaction_type, 'Inbound', I_RESULT_TRANSFERRED, 0) AS I_INBOUND_RESULT_TRANSFERRED,
                        DECODE(interaction_type, 'Inbound', I_RESULT_DESTINATIONBUSY, 0) AS I_INBOUND_RESULT_DESTINATIONBUSY,
                        DECODE(interaction_type, 'Inbound', I_RESULT_CUSTOMERABANDONED, 0) AS I_INBOUND_RESULT_CUSTOMERABANDONED,
                        DECODE(interaction_type, 'Inbound', I_RESULT_ABNORMALSTOP, 0) AS I_INBOUND_RESULT_ABNORMALSTOP,
                        DECODE(interaction_type, 'Outbound', I_RESULT_COMPLETED, 0) AS I_OUTBOUND_RESULT_COMPLETED,
                        DECODE(interaction_type, 'Outbound', I_RESULT_REDIRECTED, 0) AS I_OUTBOUND_RESULT_REDIRECTED,
                        DECODE(interaction_type, 'Outbound', I_RESULT_CONFERENCED, 0) AS I_OUTBOUND_RESULT_CONFERENCED,
                        DECODE(interaction_type, 'Outbound', I_RESULT_DIVERTED, 0) AS I_OUTBOUND_RESULT_DIVERTED,
                        DECODE(interaction_type, 'Outbound', I_RESULT_ABANDONED, 0) AS I_OUTBOUND_RESULT_ABANDONED,
                        DECODE(interaction_type, 'Outbound', I_RESULT_TRANSFERRED, 0) AS I_OUTBOUND_RESULT_TRANSFERRED,
                        DECODE(interaction_type, 'Outbound', I_RESULT_DESTINATIONBUSY, 0) AS I_OUTBOUND_RESULT_DESTINATIONBUSY,
                        DECODE(interaction_type, 'Outbound', I_RESULT_CUSTOMERABANDONED, 0) AS I_OUTBOUND_RESULT_CUSTOMERABANDONED,
                        DECODE(interaction_type, 'Outbound', I_RESULT_ABNORMALSTOP, 0) AS I_OUTBOUND_RESULT_ABNORMALSTOP
                    FROM PM_OWNER.call_centrum_history cch
                    WHERE event_dt >= ADD_MONTHS(process_dt + 1, -1)
                        AND event_dt < process_dt + 1
                ) group by customerid, TO_NUMBER(TO_CHAR(event_dt, 'YYYYMM'))
            ) cch ON main.customerid = cch.customerid
    
            LEFT JOIN ( -- Agregated feedbacks from surveys, NPS, branch visits
                SELECT
                    customerid,
                    TO_NUMBER(TO_CHAR(event_dt, 'YYYYMM')) as month_id,
                    ROUND(AVG(feedback), 2) AS AVG_FEEDBACK_M,
                    MIN(feedback) AS MIN_FEEDBACK_M,
                    MAX(feedback) AS MAX_FEEDBACK_M,
                    COUNT(feedback) AS C_FEEDBACK_M,
                    SIGN(COUNT(feedback)) AS I_FEEDBACK_M,        
                    process_dt + 1 - TRUNC(MAX(event_dt)) AS RECENCY_FEEDBACK,
                    NVL(SUM(i_service_request), 0) AS C_SERVICE_REQUEST_M,
                    SIGN(MAX(i_service_request)) AS I_SERVICE_REQUEST_M
    
                FROM PM_OWNER.feedback_evaluation
                WHERE event_dt >= ADD_MONTHS(process_dt + 1, -1)
                    AND event_dt < process_dt + 1
                GROUP BY customerid, TO_NUMBER(TO_CHAR(event_dt, 'YYYYMM'))
            ) fe ON main.customerid = fe.customerid
    
            LEFT JOIN ( -- Categorized branch visits purpose
                SELECT
                    customerid,
                    TO_NUMBER(TO_CHAR(event_dt, 'YYYYMM')) AS month_id,
                    LEAST(MAX(CATEGORY_SALE), 1) as I_BRANCH_VISIT_SALE_M,
                    LEAST(MAX(CATEGORY_CARE), 1) AS I_BRANCH_VISIT_CARE_M,
                    LEAST(MAX(CATEGORY_INFORMATION), 1) AS I_BRANCH_VISIT_INFORMATION_M,
                    LEAST(MAX(CATEGORY_CASH), 1) AS I_BRANCH_VISIT_CASH_M,
                    LEAST(MAX(CATEGORY_OTHER), 1) AS I_BRANCH_VISIT_OTHER_M,
                    SUM(CATEGORY_SALE) AS C_BRANCH_VISIT_SALE_M,
                    SUM(CATEGORY_CARE) AS C_BRANCH_VISIT_CARE_M,
                    SUM(CATEGORY_INFORMATION) AS C_BRANCH_VISIT_INFORMATION_M,
                    SUM(CATEGORY_CASH) AS C_BRANCH_VISIT_CASH_M,
                    SUM(CATEGORY_OTHER) AS C_BRANCH_VISIT_OTHER_M,
                    process_dt + 1 - MAX(DECODE(CATEGORY_SALE, 1, TRUNC(event_dt))) AS RECENCY_BRANCH_VISIT_SALE,
                    process_dt + 1 - MAX(DECODE(CATEGORY_CARE, 1, TRUNC(event_dt))) AS RECENCY_BRANCH_VISIT_CARE,
                    process_dt + 1 - MAX(DECODE(CATEGORY_INFORMATION, 1, TRUNC(event_dt))) AS RECENCY_BRANCH_VISIT_INFORMATION,
                    process_dt + 1 - MAX(DECODE(CATEGORY_CASH, 1, TRUNC(event_dt))) AS RECENCY_BRANCH_VISIT_CASH,
                    process_dt + 1 - MAX(DECODE(CATEGORY_OTHER, 1, TRUNC(event_dt))) AS RECENCY_BRANCH_VISIT_OTHER
                 FROM(
                    SELECT
                        customerid,
                        event_dt,
                        LEAST(MAX(CATEGORY_SALE), 1) CATEGORY_SALE,
                        LEAST(MAX(CATEGORY_CARE), 1) CATEGORY_CARE,
                        LEAST(MAX(CATEGORY_INFORMATION), 1) CATEGORY_INFORMATION,
                        LEAST(MAX(CATEGORY_CASH), 1) CATEGORY_CASH,
                        LEAST(MAX(CATEGORY_OTHER), 1) CATEGORY_OTHER
    
                    FROM (
                        -- Scube sessions
                        select
                            PR.party_id customerid,
                            scl.event_log_date event_dt,
                            0 CATEGORY_SALE,
                            0 CATEGORY_CARE,
                            0 CATEGORY_INFORMATION,
                            0 CATEGORY_CASH,
                            1 CATEGORY_OTHER
    
                        FROM l0_owner.scl_identif_log scl
    
                        JOIN L0_OWNER.PARTY P
                            ON P.party_src_val = to_char(scl.cuid)
                            AND	P.src_syst_cd = 110
                            AND	P.party_src_key LIKE 'PMC%'
    
                        JOIN L0_OWNER.PARTY_RLTD PR
                            ON  PR.child_party_id = P.party_id
                            AND PR.party_rltd_rsn_cd = 4
                            AND PR.party_rltd_start_dt < process_dt + 1 
                            AND PR.party_rltd_end_dt >= ADD_MONTHS(process_dt + 1, -1)
    
                        where 1 = 1
                            -- time horizont
                            and scl.event_log_date >= ADD_MONTHS(process_dt + 1, -1)
                            AND scl.event_log_date < process_dt + 1
                            and scl.device_type_id in (1,2)   
                            and scl.cuid is not null
    
                        UNION ALL
    
                        -- crm sessions
                        SELECT
                            CCDB.CUSTOMERID,
                            CAST(E.DT_BUS_EFF_FROM AS DATE) EVENT_DT,
    
                            CASE WHEN E.CD_REC_VALUE_PROPERTY_1='100' THEN 1 ELSE 0 END CATEGORY_SALE,
                            CASE WHEN E.CD_REC_VALUE_PROPERTY_1='200' THEN 1 ELSE 0 END CATEGORY_CARE,
                            CASE WHEN E.CD_REC_VALUE_PROPERTY_1='300' THEN 1 ELSE 0 END CATEGORY_INFORMATION,
                            CASE WHEN E.CD_REC_VALUE_PROPERTY_1='400' THEN 1 ELSE 0 END CATEGORY_CASH,
                            CASE WHEN E.CD_REC_VALUE_PROPERTY_1='500' THEN 1 ELSE 0 END CATEGORY_OTHER
    
                        from CCDB_OWNER.OH_W_EVENT E 
    
                        --dotiahnutie klienta
                        JOIN CCDB_OWNER.OH_W_PARTY P
                        on P.cd_rec_party = E.cd_rec_party_1 
                            and P.cd_rec_source = 'CCDB.CRM'
                            and P.cd_rec_object_type = E.cd_rec_party_1_object_type 
                            and P.party_end_dt = date '9999-12-31'
                            AND P.DELETED_DT   = date '9999-12-31'
    
                        --doplnenie CUID/CUSTOMERID
                        LEFT JOIN (
                            SELECT CCDB_ID, CUSTOMERID 
                                    ,PARTY_PARTY_START_DT
                                    ,PARTY_PARTY_END_DT 
                                FROM (
                                SELECT
                                    pp.cd_rec_party_1 as CCDB_ID,
                                    TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) as CUSTOMERID, --1 - CDS
                                    ROW_NUMBER() OVER (PARTITION BY TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) ORDER BY dt_bus_eff_from desc, pp.cd_rec_party_1) row_num
                                    ,PP.PARTY_PARTY_START_DT
                                    ,PP.PARTY_PARTY_END_DT
                                FROM CCDB_OWNER.oh_w_party_party PP
    
                                LEFT JOIN L0_OWNER.PARTY P
                                    ON P.party_src_key=pp.cd_rec_party_2
                                    AND pp.cd_rec_party_2_source='1'
                                    AND P.SRC_SYST_CD=1
    
                                where 1=1
                                    and trim(TRANSLATE(PP.cd_rec_party_1, '0123456789-,.', ' ')) is null
                                    and PP.PARTY_PARTY_START_DT < process_dt + 1
                                    and PP.PARTY_PARTY_END_DT >= ADD_MONTHS(process_dt + 1, -1)
                                    and PP.cd_rec_relation_type = 'PARTY_INSTANCE'
                                    and pp.cd_rec_relation_type_source = 'CCDB'
                                    and pp.cd_rec_source = 'CCDB'
                                    and pp.cd_rec_party_1_source = 'CCDB.CRM'
                                    and TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) is not null)
                            WHERE row_num = 1) CCDB
                            ON CCDB.CCDB_ID=P.CD_REC_PARTY
                                and CCDB.PARTY_PARTY_START_DT <= E.DT_BUS_EFF_FROM
                                and CCDB.PARTY_PARTY_END_DT > E.DT_BUS_EFF_FROM  
    
                        where 1=1
                            AND E.CD_REC_VALUE_PROPERTY_2 <> 'XNA'
                            AND E.CD_REC_VALUE_PROPERTY_2 is not null
                            AND CAST(E.DT_BUS_EFF_FROM AS DATE) >= ADD_MONTHS(process_dt + 1, -1)
                            AND CAST(E.DT_BUS_EFF_FROM AS DATE) < process_dt + 1
                            AND E.event_end_dt = date '9999-12-31'
                            AND E.cd_rec_source = 'CCDB.CRM'
                            AND E.cd_rec_object_type='OBJECT~EVENT~SESSION'
                            and CCDB.CUSTOMERID is not null
                    ) GROUP BY customerid, event_dt
                ) GROUP BY customerid, TO_NUMBER(TO_CHAR(event_dt, 'YYYYMM'))
            ) bv ON main.customerid = bv.customerid
    
            LEFT JOIN ( -- Data for month_id between M-1 and M-2
                SELECT
                    customerid,
                    AVG(AVG_INBOUND_EMAIL_DURATION_M) AS AVG_INBOUND_EMAIL_DURATION_M2,
                    AVG(AVG_INBOUND_VOICE_DURATION_M) AS AVG_INBOUND_VOICE_DURATION_M2,
                    AVG(AVG_OUTBOUND_EMAIL_DURATION_M) AS AVG_OUTBOUND_EMAIL_DURATION_M2,
                    AVG(AVG_OUTBOUND_VOICE_DURATION_M) AS AVG_OUTBOUND_VOICE_DURATION_M2,
                    SUM(SUM_INBOUND_EMAIL_DURATION_M) AS SUM_INBOUND_EMAIL_DURATION_M2,
                    SUM(SUM_INBOUND_VOICE_DURATION_M) AS SUM_INBOUND_VOICE_DURATION_M2,
                    SUM(SUM_OUTBOUND_EMAIL_DURATION_M) AS SUM_OUTBOUND_EMAIL_DURATION_M2,
                    SUM(SUM_OUTBOUND_VOICE_DURATION_M) AS SUM_OUTBOUND_VOICE_DURATION_M2,
                    SUM(C_INBOUND_EMAIL_M) AS C_INBOUND_EMAIL_M2,
                    SUM(C_INBOUND_VOICE_M) AS C_INBOUND_VOICE_M2,
                    SUM(C_OUTBOUND_EMAIL_M) AS C_OUTBOUND_EMAIL_M2,
                    SUM(C_OUTBOUND_VOICE_M) AS C_OUTBOUND_VOICE_M2,
                    MAX(I_INBOUND_EMAIL_M) AS I_INBOUND_EMAIL_M2,
                    MAX(I_INBOUND_VOICE_M) AS I_INBOUND_VOICE_M2,
                    MAX(I_OUTBOUND_EMAIL_M) AS I_OUTBOUND_EMAIL_M2,
                    MAX(I_OUTBOUND_VOICE_M) AS I_OUTBOUND_VOICE_M2,
                    SUM(C_INBOUND_RESULT_COMPLETED_M) AS C_INBOUND_RESULT_COMPLETED_M2,
                    SUM(C_INBOUND_RESULT_REDIRECTED_M) AS C_INBOUND_RESULT_REDIRECTED_M2,
                    SUM(C_INBOUND_RESULT_CONFERENCED_M) AS C_INBOUND_RESULT_CONFERENCED_M2,
                    SUM(C_INBOUND_RESULT_DIVERTED_M) AS C_INBOUND_RESULT_DIVERTED_M2,
                    SUM(C_INBOUND_RESULT_ABANDONED_M) AS C_INBOUND_RESULT_ABANDONED_M2,
                    SUM(C_INBOUND_RESULT_TRANSFERRED_M) AS C_INBOUND_RESULT_TRANSFERRED_M2,
                    SUM(C_INBOUND_RESULT_DESTINATIONBUSY_M) AS C_INBOUND_RESULT_DESTINATIONBUSY_M2,
                    SUM(C_INBOUND_RESULT_CUSTOMERABANDONED_M) AS C_INBOUND_RESULT_CUSTOMERABANDONED_M2,
                    SUM(C_INBOUND_RESULT_ABNORMALSTOP_M) AS C_INBOUND_RESULT_ABNORMALSTOP_M2,
                    SUM(C_OUTBOUND_RESULT_COMPLETED_M) AS C_OUTBOUND_RESULT_COMPLETED_M2,
                    SUM(C_OUTBOUND_RESULT_REDIRECTED_M) AS C_OUTBOUND_RESULT_REDIRECTED_M2,
                    SUM(C_OUTBOUND_RESULT_CONFERENCED_M) AS C_OUTBOUND_RESULT_CONFERENCED_M2,
                    SUM(C_OUTBOUND_RESULT_DIVERTED_M) AS C_OUTBOUND_RESULT_DIVERTED_M2,
                    SUM(C_OUTBOUND_RESULT_ABANDONED_M) AS C_OUTBOUND_RESULT_ABANDONED_M2,
                    SUM(C_OUTBOUND_RESULT_TRANSFERRED_M) AS C_OUTBOUND_RESULT_TRANSFERRED_M2,
                    SUM(C_OUTBOUND_RESULT_DESTINATIONBUSY_M) AS C_OUTBOUND_RESULT_DESTINATIONBUSY_M2,
                    SUM(C_OUTBOUND_RESULT_CUSTOMERABANDONED_M) AS C_OUTBOUND_RESULT_CUSTOMERABANDONED_M2,
                    SUM(C_OUTBOUND_RESULT_ABNORMALSTOP_M) AS C_OUTBOUND_RESULT_ABNORMALSTOP_M2,
                    MAX(I_INBOUND_RESULT_COMPLETED_M) AS I_INBOUND_RESULT_COMPLETED_M2,
                    MAX(I_INBOUND_RESULT_REDIRECTED_M) AS I_INBOUND_RESULT_REDIRECTED_M2,
                    MAX(I_INBOUND_RESULT_CONFERENCED_M) AS I_INBOUND_RESULT_CONFERENCED_M2,
                    MAX(I_INBOUND_RESULT_DIVERTED_M) AS I_INBOUND_RESULT_DIVERTED_M2,
                    MAX(I_INBOUND_RESULT_ABANDONED_M) AS I_INBOUND_RESULT_ABANDONED_M2,
                    MAX(I_INBOUND_RESULT_TRANSFERRED_M) AS I_INBOUND_RESULT_TRANSFERRED_M2,
                    MAX(I_INBOUND_RESULT_DESTINATIONBUSY_M) AS I_INBOUND_RESULT_DESTINATIONBUSY_M2,
                    MAX(I_INBOUND_RESULT_CUSTOMERABANDONED_M) AS I_INBOUND_RESULT_CUSTOMERABANDONED_M2,
                    MAX(I_INBOUND_RESULT_ABNORMALSTOP_M) AS I_INBOUND_RESULT_ABNORMALSTOP_M2,
                    MAX(I_OUTBOUND_RESULT_COMPLETED_M) AS I_OUTBOUND_RESULT_COMPLETED_M2,
                    MAX(I_OUTBOUND_RESULT_REDIRECTED_M) AS I_OUTBOUND_RESULT_REDIRECTED_M2,
                    MAX(I_OUTBOUND_RESULT_CONFERENCED_M) AS I_OUTBOUND_RESULT_CONFERENCED_M2,
                    MAX(I_OUTBOUND_RESULT_DIVERTED_M) AS I_OUTBOUND_RESULT_DIVERTED_M2,
                    MAX(I_OUTBOUND_RESULT_ABANDONED_M) AS I_OUTBOUND_RESULT_ABANDONED_M2,
                    MAX(I_OUTBOUND_RESULT_TRANSFERRED_M) AS I_OUTBOUND_RESULT_TRANSFERRED_M2,
                    MAX(I_OUTBOUND_RESULT_DESTINATIONBUSY_M) AS I_OUTBOUND_RESULT_DESTINATIONBUSY_M2,
                    MAX(I_OUTBOUND_RESULT_CUSTOMERABANDONED_M) AS I_OUTBOUND_RESULT_CUSTOMERABANDONED_M2,
                    MAX(I_OUTBOUND_RESULT_ABNORMALSTOP_M) AS I_OUTBOUND_RESULT_ABNORMALSTOP_M2,
                    AVG(RATIO_INBOUND_VOICE_COMPLETED_M) AS RATIO_INBOUND_VOICE_COMPLETED_M2,
                    AVG(RATIO_INBOUND_VOICE_ABANDONED_M) AS RATIO_INBOUND_VOICE_ABANDONED_M2,
                    AVG(RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M) AS RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M2,
                    AVG(RATIO_OUTBOUND_VOICE_COMPLETED_M) AS RATIO_OUTBOUND_VOICE_COMPLETED_M2,
                    AVG(RATIO_OUTBOUND_VOICE_ABANDONED_M) AS RATIO_OUTBOUND_VOICE_ABANDONED_M2,
                    AVG(RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M) AS RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M2,
                    AVG(RATIO_INBOUND_EMAIL_M) AS RATIO_INBOUND_EMAIL_M2,
                    AVG(RATIO_INBOUND_VOICE_M) AS RATIO_INBOUND_VOICE_M2,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_INBOUND_EMAIL / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_INBOUND_EMAIL END)) AS RECENCY_INBOUND_EMAIL_PREV,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_INBOUND_VOICE / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_INBOUND_VOICE END)) AS RECENCY_INBOUND_VOICE_PREV,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_OUTBOUND_EMAIL / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_OUTBOUND_EMAIL END)) AS RECENCY_OUTBOUND_EMAIL_PREV,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_OUTBOUND_VOICE / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_OUTBOUND_VOICE END)) AS RECENCY_OUTBOUND_VOICE_PREV,
                    AVG(DECODE(I_FEEDBACK_M, 1, AVG_FEEDBACK_M)) * SUM(I_FEEDBACK_M) AS AVG_FEEDBACK_M2,
                    MIN(MIN_FEEDBACK_M) AS MIN_FEEDBACK_M2,
                    MAX(MAX_FEEDBACK_M) AS MAX_FEEDBACK_M2,
                    SUM(C_FEEDBACK_M) AS C_FEEDBACK_M2,
                    MAX(I_FEEDBACK_M) AS I_FEEDBACK_M2,
                    SUM(I_FEEDBACK_M) AS SUM_I_FEEDBACK_M2,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_FEEDBACK / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_FEEDBACK END)) AS RECENCY_FEEDBACK_PREV,
                    SUM(C_SERVICE_REQUEST_M) AS C_SERVICE_REQUEST_M2,
                    MAX(I_SERVICE_REQUEST_M) AS I_SERVICE_REQUEST_M2,
                    MAX(I_BRANCH_VISIT_SALE_M) AS I_BRANCH_VISIT_SALE_M2,
                    MAX(I_BRANCH_VISIT_CARE_M) AS I_BRANCH_VISIT_CARE_M2,
                    MAX(I_BRANCH_VISIT_INFORMATION_M) AS I_BRANCH_VISIT_INFORMATION_M2,
                    MAX(I_BRANCH_VISIT_CASH_M) AS I_BRANCH_VISIT_CASH_M2,
                    MAX(I_BRANCH_VISIT_OTHER_M) AS I_BRANCH_VISIT_OTHER_M2,
                    SUM(C_BRANCH_VISIT_SALE_M) AS C_BRANCH_VISIT_SALE_M2,
                    SUM(C_BRANCH_VISIT_CARE_M) AS C_BRANCH_VISIT_CARE_M2,
                    SUM(C_BRANCH_VISIT_INFORMATION_M) AS C_BRANCH_VISIT_INFORMATION_M2,
                    SUM(C_BRANCH_VISIT_CASH_M) AS C_BRANCH_VISIT_CASH_M2,
                    SUM(C_BRANCH_VISIT_OTHER_M) AS C_BRANCH_VISIT_OTHER_M2,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_BRANCH_VISIT_SALE / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_BRANCH_VISIT_SALE END)) AS RECENCY_BRANCH_VISIT_SALE_PREV,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_BRANCH_VISIT_CARE / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_BRANCH_VISIT_CARE END)) AS RECENCY_BRANCH_VISIT_CARE_PREV,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_BRANCH_VISIT_INFORMATION / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_BRANCH_VISIT_INFORMATION END)) AS RECENCY_BRANCH_VISIT_INFORMATION_PREV,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_BRANCH_VISIT_CASH / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_BRANCH_VISIT_CASH END)) AS RECENCY_BRANCH_VISIT_CASH_PREV,
                    MIN(DECODE(month_id, TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM')), CASE WHEN RECENCY_BRANCH_VISIT_OTHER / (process_dt - process_dt_hist) < 0.9 THEN RECENCY_BRANCH_VISIT_OTHER END)) AS RECENCY_BRANCH_VISIT_OTHER_PREV
                FROM PM_OWNER.attr_interaction
                WHERE month_id BETWEEN TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -2), 'YYYYMM')) AND TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
                GROUP BY customerid
            ) int_q ON main.customerid = int_q.customerid
    
            LEFT JOIN ( -- Data for month_id between M-1 and M-11
                SELECT
                    customerid,
                    AVG(AVG_INBOUND_EMAIL_DURATION_M) AS AVG_INBOUND_EMAIL_DURATION_M11,
                    AVG(AVG_INBOUND_VOICE_DURATION_M) AS AVG_INBOUND_VOICE_DURATION_M11,
                    AVG(AVG_OUTBOUND_EMAIL_DURATION_M) AS AVG_OUTBOUND_EMAIL_DURATION_M11,
                    AVG(AVG_OUTBOUND_VOICE_DURATION_M) AS AVG_OUTBOUND_VOICE_DURATION_M11,
                    SUM(SUM_INBOUND_EMAIL_DURATION_M) AS SUM_INBOUND_EMAIL_DURATION_M11,
                    SUM(SUM_INBOUND_VOICE_DURATION_M) AS SUM_INBOUND_VOICE_DURATION_M11,
                    SUM(SUM_OUTBOUND_EMAIL_DURATION_M) AS SUM_OUTBOUND_EMAIL_DURATION_M11,
                    SUM(SUM_OUTBOUND_VOICE_DURATION_M) AS SUM_OUTBOUND_VOICE_DURATION_M11,
                    SUM(C_INBOUND_EMAIL_M) AS C_INBOUND_EMAIL_M11,
                    SUM(C_INBOUND_VOICE_M) AS C_INBOUND_VOICE_M11,
                    SUM(C_OUTBOUND_EMAIL_M) AS C_OUTBOUND_EMAIL_M11,
                    SUM(C_OUTBOUND_VOICE_M) AS C_OUTBOUND_VOICE_M11,
                    MAX(I_INBOUND_EMAIL_M) AS I_INBOUND_EMAIL_M11,
                    MAX(I_INBOUND_VOICE_M) AS I_INBOUND_VOICE_M11,
                    MAX(I_OUTBOUND_EMAIL_M) AS I_OUTBOUND_EMAIL_M11,
                    MAX(I_OUTBOUND_VOICE_M) AS I_OUTBOUND_VOICE_M11,
                    SUM(C_INBOUND_RESULT_COMPLETED_M) AS C_INBOUND_RESULT_COMPLETED_M11,
                    SUM(C_INBOUND_RESULT_REDIRECTED_M) AS C_INBOUND_RESULT_REDIRECTED_M11,
                    SUM(C_INBOUND_RESULT_CONFERENCED_M) AS C_INBOUND_RESULT_CONFERENCED_M11,
                    SUM(C_INBOUND_RESULT_DIVERTED_M) AS C_INBOUND_RESULT_DIVERTED_M11,
                    SUM(C_INBOUND_RESULT_ABANDONED_M) AS C_INBOUND_RESULT_ABANDONED_M11,
                    SUM(C_INBOUND_RESULT_TRANSFERRED_M) AS C_INBOUND_RESULT_TRANSFERRED_M11,
                    SUM(C_INBOUND_RESULT_DESTINATIONBUSY_M) AS C_INBOUND_RESULT_DESTINATIONBUSY_M11,
                    SUM(C_INBOUND_RESULT_CUSTOMERABANDONED_M) AS C_INBOUND_RESULT_CUSTOMERABANDONED_M11,
                    SUM(C_INBOUND_RESULT_ABNORMALSTOP_M) AS C_INBOUND_RESULT_ABNORMALSTOP_M11,
                    SUM(C_OUTBOUND_RESULT_COMPLETED_M) AS C_OUTBOUND_RESULT_COMPLETED_M11,
                    SUM(C_OUTBOUND_RESULT_REDIRECTED_M) AS C_OUTBOUND_RESULT_REDIRECTED_M11,
                    SUM(C_OUTBOUND_RESULT_CONFERENCED_M) AS C_OUTBOUND_RESULT_CONFERENCED_M11,
                    SUM(C_OUTBOUND_RESULT_DIVERTED_M) AS C_OUTBOUND_RESULT_DIVERTED_M11,
                    SUM(C_OUTBOUND_RESULT_ABANDONED_M) AS C_OUTBOUND_RESULT_ABANDONED_M11,
                    SUM(C_OUTBOUND_RESULT_TRANSFERRED_M) AS C_OUTBOUND_RESULT_TRANSFERRED_M11,
                    SUM(C_OUTBOUND_RESULT_DESTINATIONBUSY_M) AS C_OUTBOUND_RESULT_DESTINATIONBUSY_M11,
                    SUM(C_OUTBOUND_RESULT_CUSTOMERABANDONED_M) AS C_OUTBOUND_RESULT_CUSTOMERABANDONED_M11,
                    SUM(C_OUTBOUND_RESULT_ABNORMALSTOP_M) AS C_OUTBOUND_RESULT_ABNORMALSTOP_M11,
                    MAX(I_INBOUND_RESULT_COMPLETED_M) AS I_INBOUND_RESULT_COMPLETED_M11,
                    MAX(I_INBOUND_RESULT_REDIRECTED_M) AS I_INBOUND_RESULT_REDIRECTED_M11,
                    MAX(I_INBOUND_RESULT_CONFERENCED_M) AS I_INBOUND_RESULT_CONFERENCED_M11,
                    MAX(I_INBOUND_RESULT_DIVERTED_M) AS I_INBOUND_RESULT_DIVERTED_M11,
                    MAX(I_INBOUND_RESULT_ABANDONED_M) AS I_INBOUND_RESULT_ABANDONED_M11,
                    MAX(I_INBOUND_RESULT_TRANSFERRED_M) AS I_INBOUND_RESULT_TRANSFERRED_M11,
                    MAX(I_INBOUND_RESULT_DESTINATIONBUSY_M) AS I_INBOUND_RESULT_DESTINATIONBUSY_M11,
                    MAX(I_INBOUND_RESULT_CUSTOMERABANDONED_M) AS I_INBOUND_RESULT_CUSTOMERABANDONED_M11,
                    MAX(I_INBOUND_RESULT_ABNORMALSTOP_M) AS I_INBOUND_RESULT_ABNORMALSTOP_M11,
                    MAX(I_OUTBOUND_RESULT_COMPLETED_M) AS I_OUTBOUND_RESULT_COMPLETED_M11,
                    MAX(I_OUTBOUND_RESULT_REDIRECTED_M) AS I_OUTBOUND_RESULT_REDIRECTED_M11,
                    MAX(I_OUTBOUND_RESULT_CONFERENCED_M) AS I_OUTBOUND_RESULT_CONFERENCED_M11,
                    MAX(I_OUTBOUND_RESULT_DIVERTED_M) AS I_OUTBOUND_RESULT_DIVERTED_M11,
                    MAX(I_OUTBOUND_RESULT_ABANDONED_M) AS I_OUTBOUND_RESULT_ABANDONED_M11,
                    MAX(I_OUTBOUND_RESULT_TRANSFERRED_M) AS I_OUTBOUND_RESULT_TRANSFERRED_M11,
                    MAX(I_OUTBOUND_RESULT_DESTINATIONBUSY_M) AS I_OUTBOUND_RESULT_DESTINATIONBUSY_M11,
                    MAX(I_OUTBOUND_RESULT_CUSTOMERABANDONED_M) AS I_OUTBOUND_RESULT_CUSTOMERABANDONED_M11,
                    MAX(I_OUTBOUND_RESULT_ABNORMALSTOP_M) AS I_OUTBOUND_RESULT_ABNORMALSTOP_M11,
                    AVG(RATIO_INBOUND_VOICE_COMPLETED_M) AS RATIO_INBOUND_VOICE_COMPLETED_M11,
                    AVG(RATIO_INBOUND_VOICE_ABANDONED_M) AS RATIO_INBOUND_VOICE_ABANDONED_M11,
                    AVG(RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M) AS RATIO_INBOUND_VOICE_CUSTOMERABANDONED_M11,
                    AVG(RATIO_OUTBOUND_VOICE_COMPLETED_M) AS RATIO_OUTBOUND_VOICE_COMPLETED_M11,
                    AVG(RATIO_OUTBOUND_VOICE_ABANDONED_M) AS RATIO_OUTBOUND_VOICE_ABANDONED_M11,
                    AVG(RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M) AS RATIO_OUTBOUND_VOICE_CUSTOMERABANDONED_M11,
                    AVG(RATIO_INBOUND_EMAIL_M) AS RATIO_INBOUND_EMAIL_M11,
                    AVG(RATIO_INBOUND_VOICE_M) AS RATIO_INBOUND_VOICE_M11,
                    AVG(DECODE(I_FEEDBACK_M, 1, AVG_FEEDBACK_M)) * SUM(I_FEEDBACK_M) AS AVG_FEEDBACK_M11,
                    MIN(MIN_FEEDBACK_M) AS MIN_FEEDBACK_M11,
                    MAX(MAX_FEEDBACK_M) AS MAX_FEEDBACK_M11,
                    SUM(C_FEEDBACK_M) AS C_FEEDBACK_M11,
                    MAX(I_FEEDBACK_M) AS I_FEEDBACK_M11,
                    SUM(I_FEEDBACK_M) AS SUM_I_FEEDBACK_M11,
                    SUM(C_SERVICE_REQUEST_M) AS C_SERVICE_REQUEST_M11,
                    MAX(I_SERVICE_REQUEST_M) AS I_SERVICE_REQUEST_M11,
                    MAX(I_BRANCH_VISIT_SALE_M) AS I_BRANCH_VISIT_SALE_M11,
                    MAX(I_BRANCH_VISIT_CARE_M) AS I_BRANCH_VISIT_CARE_M11,
                    MAX(I_BRANCH_VISIT_INFORMATION_M) AS I_BRANCH_VISIT_INFORMATION_M11,
                    MAX(I_BRANCH_VISIT_CASH_M) AS I_BRANCH_VISIT_CASH_M11,
                    MAX(I_BRANCH_VISIT_OTHER_M) AS I_BRANCH_VISIT_OTHER_M11,
                    SUM(C_BRANCH_VISIT_SALE_M) AS C_BRANCH_VISIT_SALE_M11,
                    SUM(C_BRANCH_VISIT_CARE_M) AS C_BRANCH_VISIT_CARE_M11,
                    SUM(C_BRANCH_VISIT_INFORMATION_M) AS C_BRANCH_VISIT_INFORMATION_M11,
                    SUM(C_BRANCH_VISIT_CASH_M) AS C_BRANCH_VISIT_CASH_M11,
                    SUM(C_BRANCH_VISIT_OTHER_M) AS C_BRANCH_VISIT_OTHER_M11
                FROM PM_OWNER.attr_interaction
                WHERE month_id BETWEEN TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -11), 'YYYYMM')) AND TO_NUMBER(TO_CHAR(ADD_MONTHS(process_dt, -1), 'YYYYMM'))
                GROUP BY customerid
            ) int_y ON main.customerid = int_y.customerid
            --WHERE COALESCE(cch.customerid, fe.customerid, bv.customerid) IS NOT NULL
        )
        ;   
        
        stat_ins(p_count => SQL%ROWCOUNT);
        commit;              
        mng_stats(l_owner_name, l_table_name, l_proc_name);
        
    END;  
    
   
      PROCEDURE append_ATTR_AFP(p_audit_id IN NUMBER, process_dt IN DATE, p_repair IN BOOLEAN) IS
    l_proc_name varchar2(255) := 'append_ATTR_AFP';
    l_owner_name varchar2(30) := 'PM_OWNER';
    l_table_name varchar2(30) := 'ATTR_AFP';
    l_month_start_dt    date  := trunc(process_dt,'MM');
    l_month_end_dt      date  := last_day(process_dt);
    
  BEGIN

    step(p_stepname => l_proc_name || ' TRUNC', p_source => 'N/A', p_target => 'STG_AFP');
    ETL_OWNER.etl_dbms_util.truncate_table('STG_AFP',l_owner_name);  
    
    step(p_stepname => l_proc_name || ' START INSERTING STG_AFP', p_source => 'N/A', p_target => 'N/A');
    INSERT INTO PM_OWNER.STG_AFP
    (  
    SELECT distinct
        process_dt AS INSERT_DT,
        CLB.CUSTOMERID,
        PEF.CD_REC_PARTY,
        MAX(PEF.PERSON_FACT_START_DT) OVER (PARTITION BY CLB.CUSTOMERID) AFP_MOST_RECENT_DT,
        PEF.PERSON_FACT_START_DT,
        ROUND(process_dt - PEF.PERSON_FACT_START_DT) AS RECENCY_AFP,
        ROUND(process_dt - MIN(PEF.PERSON_FACT_START_DT) OVER (PARTITION BY CLB.CUSTOMERID)) AS DAYS_SINCE_FIRST_AFP,
        COALESCE(PEF.VL_NET_INCOME, 0) + COALESCE(PEF.VL_OTHER_INCOME, 0) as AFP_NET_INCOME,
        COALESCE(PEF.VL_LIABILITIES, 0) as AFP_LIABILITIES,
        COALESCE(PEF.VL_SAVINGS, 0) as AFP_SAVINGS,

        CASE WHEN (COALESCE(PEF.VL_NET_INCOME, 0) + COALESCE(PEF.VL_OTHER_INCOME, 0)) > 0 
             THEN ROUND(COALESCE(PEF.VL_SAVINGS, 0) / (COALESCE(PEF.VL_NET_INCOME, 0) + COALESCE(PEF.VL_OTHER_INCOME, 0)), 4) 
             ELSE 0 END AS AFP_INCOME_RATIO_SAVED,
        CASE WHEN (COALESCE(PEF.VL_NET_INCOME, 0) + COALESCE(PEF.VL_OTHER_INCOME, 0)) > 0
             THEN ROUND(COALESCE(PEF.VL_LIABILITIES, 0) / (COALESCE(PEF.VL_NET_INCOME, 0) + COALESCE(PEF.VL_OTHER_INCOME, 0)), 4)
             ELSE 0 END AS AFP_INCOME_RATIO_LIABILITIES,

        PEF.CD_REC_VALUE_HOUSING_TYPE,

        CASE WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_01' THEN 1 ELSE 0 END AS AFP_MAIN_BANK,
        CASE WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_01' THEN 1 ELSE 0 END AS AFP_EMPLOYEE,
        CASE WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_07' THEN 1 ELSE 0 END AS AFP_PARENTALLEAVE,
        CASE WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_04' THEN 1 ELSE 0 END AS AFP_STUDENT,
        CASE WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_05' THEN 1 ELSE 0 END AS AFP_PENSION,
        CASE WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_02' THEN 1 ELSE 0 END AS AFP_ENTREPRENEUR,
        CASE WHEN INCOME_TYPE.CD_REC_VALUE NOT IN ('ET_01', 'ET_02', 'ET_04', 'ET_05', 'ET_07') 
             THEN 1 ELSE 0 END AS AFP_OTHERINCOME,

        --COALESCE(NM_CHILDREN, 0) AS NM_CHILDREN,
        PEF.CD_REC_VALUE_MARITAL_STATUS,

        CASE WHEN PEF.CD_REC_VALUE_HOUSING_TYPE = 'HT_01' THEN 'Apartment'
             WHEN PEF.CD_REC_VALUE_HOUSING_TYPE = 'HT_02' THEN 'Rented apartment'
             WHEN PEF.CD_REC_VALUE_HOUSING_TYPE = 'HT_03' THEN 'House'
             WHEN PEF.CD_REC_VALUE_HOUSING_TYPE = 'HT_04' THEN 'Rented house'
             WHEN PEF.CD_REC_VALUE_HOUSING_TYPE = 'HT_08' THEN 'Parents'
             ELSE 'UNKNOWN' END AS AFP_HOUSING_TYPE,
        CASE WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_01' THEN 'Employee'
             WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_07' THEN 'Parentalleave'
             WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_04' THEN 'Student'
             WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_05' THEN 'Pension'
             WHEN INCOME_TYPE.CD_REC_VALUE = 'ET_02' THEN 'Entrepreneur'
             ELSE 'UNKNOWN' END AS AFP_INCOME_TYPE,
         CASE WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_01' THEN 'CSOB'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_02' THEN 'SLSP'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_03' THEN 'VUB'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_04' THEN 'TB'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_05' THEN 'Prima'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_10' THEN '365'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_12' THEN 'mBank'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_21' THEN 'Fio'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_06' THEN 'UniCredit Bank'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_09' THEN 'Raiffeisen'
             WHEN PEF.CD_REC_VALUE_MAIN_BANK = 'PB_00' THEN 'UNKNOWN'
             ELSE 'OTHER' END AS AFP_PRIMARY_BANK,

        COALESCE(process_dt - RST.MOST_RECENT_REAL_ESTATE_DT_AFP, 1826) AS RECENCY_REAL_ESTATE_AFP,
        COALESCE(RST.C_HOUSE, 0) AS C_HOUSE, -- rodinny dom
        COALESCE(RST.C_APARTMENT, 0) AS C_APARTMENT, -- byt
        COALESCE(RST.C_COTTAGE, 0) AS C_COTTAGE, -- chata
        COALESCE(RST.C_BUILDING_LOT, 0) AS C_BUILDING_LOT, -- stavebny pozemok
        COALESCE(RST.C_COMMERCIAL_SPACE, 0) AS C_COMMERCIAL_SPACE, -- podnikatelsky priestor
        COALESCE(RST.C_NON_RESIDENTIAL_SPACE, 0) AS C_NON_RESIDENTIAL_SPACE, -- nebytovy priestor
        COALESCE(RST.C_GARAGE, 0) AS C_GARAGE, -- garaz/park. miesto
        COALESCE(RST.REFUSE_ANSWER_REAL_ESTATE, 0) AS REFUSE_ANSWER_REAL_ESTATE, -- klient nechce uviest
        COALESCE(RST.C_HOUSE + RST.C_APARTMENT + RST.C_COTTAGE + RST.C_BUILDING_LOT 
                 + RST.C_COMMERCIAL_SPACE + RST.C_NON_RESIDENTIAL_SPACE + RST.C_GARAGE, 0) C_TOTAL_REAL_ESTATE,
        COALESCE(RST.CAT_REAL_ESTATE, 'UNKNOWN') AS CAT_REAL_ESTATE,
        COALESCE(process_dt - VEH.MOST_RECENT_VEHICLE_DT_AFP, 1826) AS RECENCY_VEHICLE_AFP,
        COALESCE(VEH.C_OWN_CAR, 0) AS C_OWN_CAR, -- vlastni auto bez uveru
        COALESCE(VEH.C_LOAN_CAR, 0) AS C_LOAN_CAR, -- vlastni auto kupene na uver
        COALESCE(VEH.C_WORK_CAR, 0) AS C_WORK_CAR, -- ma k dispozicii sluzobne auto
        COALESCE(VEH.C_MOTORCYCLE, 0) AS C_MOTORCYCLE, -- vlastni motocykel / skuter
        COALESCE(VEH.NO_OWN_VEHICLE, 0) AS NO_OWN_VEHICLE, -- nevlastni dopravny prostriedok
        COALESCE(VEH.REFUSE_ANSWER_VEHICLE, 0) AS REFUSE_ANSWER_VEHICLE,
        COALESCE(VEH.C_OWN_CAR + VEH.C_LOAN_CAR + VEH.C_WORK_CAR + VEH.C_MOTORCYCLE, 0) AS C_TOTAL_VEHICLE,
        COALESCE(VEH.CAT_VEHICLE, 'UNKNOWN') AS CAT_VEHICLE     
    
    FROM ccdb_owner.oh_w_person_fact pef
    
    JOIN ccdb_owner.oh_w_party P
        ON pef.cd_rec_party = P.cd_rec_party
        AND pef.cd_rec_party_object_type = P.cd_rec_object_type
        AND pef.cd_rec_party_object_type_source = P.cd_rec_object_type_source
        AND pef.cd_rec_party_source = P.cd_rec_source
        AND P.party_end_dt > process_dt
        AND P.party_start_dt <= process_dt
        AND P.deleted_dt > process_dt
        --and P.dt_hst_to = '31.12.9999 23:59:59,999999000'

    LEFT JOIN ccdb_owner.oh_w_property_value income_type
        ON pef.cd_rec_value_income_type = income_type.cd_rec_value
        AND pef.cd_rec_value_income_type_source = income_type.cd_rec_value_source
        AND income_type.cd_rec_property = 'INCOME_TYPE'
        AND income_type.cd_rec_property_source = 'CCDB'
        AND income_type.cd_rec_source = 'CUSTOMER_DIAGNOSTICS'
        AND income_type.property_value_end_dt > process_dt
        AND income_type.property_value_start_dt <= process_dt
        AND income_type.deleted_dt > process_dt

    JOIN l0_owner.party p1
        ON P.cd_lgc_orn_arf_key = p1.party_src_val
        AND p1.src_syst_cd = 110
        AND p1.party_src_key LIKE 'PMC%'
    
    JOIN l0_owner.party_rltd pr
        ON pr.child_party_id = p1.party_id
        AND pr.party_rltd_rsn_cd = 4
        AND pr.party_rltd_start_dt <= process_dt
        AND pr.party_rltd_end_dt > process_dt
    
    JOIN (SELECT customerid FROM pm_owner.attr_client_base
          WHERE month_id = to_number(to_char(process_dt, 'YYYYMM')) ) clb
        ON pr.party_id = clb.customerid

    LEFT JOIN (
                SELECT 
                    CUID,
                    MAX(START_DT) AS MOST_RECENT_REAL_ESTATE_DT_AFP,
                    SUM(CASE WHEN CD_REC_VALUE_REAL_ESTATE_TYPE = 'RE_TY_02' THEN 1 ELSE 0 END) AS C_HOUSE, -- rodinny dom
                    SUM(CASE WHEN CD_REC_VALUE_REAL_ESTATE_TYPE = 'RE_TY_01' THEN 1 ELSE 0 END) AS C_APARTMENT, -- byt
                    SUM(CASE WHEN CD_REC_VALUE_REAL_ESTATE_TYPE = 'RE_TY_03' THEN 1 ELSE 0 END) AS C_COTTAGE, -- chata
                    SUM(CASE WHEN CD_REC_VALUE_REAL_ESTATE_TYPE = 'RE_TY_04' THEN 1 ELSE 0 END) AS C_BUILDING_LOT, -- stavebny pozemok
                    SUM(CASE WHEN CD_REC_VALUE_REAL_ESTATE_TYPE = 'RE_TY_05' THEN 1 ELSE 0 END) AS C_COMMERCIAL_SPACE, -- podnikatelsky priestor
                    SUM(CASE WHEN CD_REC_VALUE_REAL_ESTATE_TYPE = 'RE_TY_06' THEN 1 ELSE 0 END) AS C_NON_RESIDENTIAL_SPACE, -- nebytovy priestor
                    SUM(CASE WHEN CD_REC_VALUE_REAL_ESTATE_TYPE = 'RE_TY_07' THEN 1 ELSE 0 END) AS C_GARAGE, -- garaz/park. miesto
                    MAX(CASE WHEN CD_REC_VALUE_REAL_ESTATE_TYPE = 'RE_TY_00' THEN 1 ELSE 0 END) AS REFUSE_ANSWER_REAL_ESTATE, -- klient nechce uviest
                    CASE WHEN LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_01%' AND LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_02%' THEN 'OWNER_HOUSE_AND_APARTMENT'
                         WHEN LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_02%' THEN 'OWNER_HOUSE'
                         WHEN LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_01%' THEN 'OWNER_APARTMENT'
                         WHEN LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_03%' THEN 'OWNER_COTTAGE'
                         WHEN LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_04%' THEN 'OWNER_BUILDING_LOT'
                         WHEN LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_05%' THEN 'OWNER_COMMERCIAL_SPACE'
                         WHEN LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_06%' OR LISTAGG(CD_REC_VALUE_REAL_ESTATE_TYPE) LIKE '%RE_TY_07%' THEN 'OTHER'
                         ELSE 'UNKNOWN' END AS CAT_REAL_ESTATE
                FROM
                (
                    SELECT
                        RES.CD_REC_VALUE_REAL_ESTATE_TYPE,
                        MAX(RES.REAL_ESTATE_FACT_START_DT) OVER (PARTITION BY EVT.CUID) RE_MOST_RECENT_DT,
                        EVT.CUID AS CUID,
                        RES.REAL_ESTATE_FACT_START_DT AS START_DT,
                        RES.REAL_ESTATE_FACT_END_DT

                    FROM CCDB_OWNER.OH_W_REAL_ESTATE_FACT RES
                    
                    JOIN (SELECT
                            CD_REC_THING_1,
                            MAX(SUBSTR (EVT.CD_REC_PARTY_1, 5, 10)) AS CUID
                          FROM CCDB_OWNER.OH_W_EVENT EVT
                            WHERE 1=1
                            AND EVT.CD_REC_OBJECT_TYPE_SOURCE = 'CCDB'
                            AND EVT.CD_REC_SOURCE = 'CUSTOMER_DIAGNOSTICS'
                            AND EVT.EVENT_START_DT <= process_dt
                            AND EVT.EVENT_END_DT > process_dt
                            AND EVT.DELETED_DT > process_dt
                            AND EVT.CD_REC_THING_1_OBJECT_TYPE = 'OBJECT~THING~REAL_ESTATE'
                            AND EVT.CD_REC_PARTY_1_SOURCE = '110'
                            GROUP BY CD_REC_THING_1
                    ) EVT
                        ON RES.CD_REC_THING = EVT.CD_REC_THING_1
                        AND RES.CD_REC_THING_OBJECT_TYPE_SOURCE = 'CCDB'
                        AND RES.CD_REC_SOURCE = 'CUSTOMER_DIAGNOSTICS'
                        AND RES.REAL_ESTATE_FACT_START_DT <= process_dt
                        AND RES.REAL_ESTATE_FACT_END_DT > process_dt
                        AND RES.DELETED_DT > process_dt
                        AND RES.CD_REC_THING_OBJECT_TYPE = 'OBJECT~THING~REAL_ESTATE'        
                )                            
                WHERE START_DT = RE_MOST_RECENT_DT
                GROUP BY CUID
    ) RST
        ON RST.CUID = P1.PARTY_SRC_VAL
    
    LEFT JOIN (
                SELECT 
                    CUID,
                    MAX(start_dt) AS MOST_RECENT_VEHICLE_DT_AFP,
                    SUM(CASE WHEN cd_rec_value_vehicle_ownership = 'VO_01' THEN 1 ELSE 0 END) AS C_OWN_CAR, -- vlastni auto bez uveru
                    SUM(CASE WHEN cd_rec_value_vehicle_ownership = 'VO_02' THEN 1 ELSE 0 END) AS C_LOAN_CAR, -- vlastni auto kupene na uver
                    SUM(CASE WHEN cd_rec_value_vehicle_ownership = 'VO_03' THEN 1 ELSE 0 END) AS C_WORK_CAR, -- ma k dispozicii sluzobne auto
                    SUM(CASE WHEN cd_rec_value_vehicle_ownership = 'VO_04' THEN 1 ELSE 0 END) AS C_MOTORCYCLE, -- vlastni motocykel / skuter
                    MAX(CASE WHEN cd_rec_value_vehicle_ownership = 'VO_05' THEN 1 ELSE 0 END) AS NO_OWN_VEHICLE, -- nevlastni dopravny prostriedok
                    SUM(CASE WHEN cd_rec_value_vehicle_ownership = 'VO_00' THEN 1 ELSE 0 END) AS REFUSE_ANSWER_VEHICLE, -- klient nechce uviest
                    CASE WHEN LISTAGG(cd_rec_value_vehicle_ownership) LIKE '%VO_01%' AND LISTAGG(cd_rec_value_vehicle_ownership) LIKE '%VO_04%' THEN 'OWNER_CAR_AND_MOTORCYCLE'
                         WHEN LISTAGG(cd_rec_value_vehicle_ownership) LIKE '%VO_01%' THEN 'OWNER_CAR'
                         WHEN LISTAGG(cd_rec_value_vehicle_ownership) LIKE '%VO_02%' THEN 'OWNER_LOANED_CAR'
                         WHEN LISTAGG(cd_rec_value_vehicle_ownership) LIKE '%VO_03%' THEN 'WORK_CAR'
                         WHEN LISTAGG(cd_rec_value_vehicle_ownership) LIKE '%VO_04%' THEN 'OWNER_MOTORCYCLE'
                         WHEN LISTAGG(cd_rec_value_vehicle_ownership) LIKE '%VO_05%' THEN 'NO_VEHICLE'
                         ELSE 'UNKNOWN' END AS CAT_VEHICLE
                FROM
                (
                    SELECT
                        veh.cd_rec_value_vehicle_ownership,
                        MAX(veh.vehicle_fact_start_dt) OVER (PARTITION BY evt.CUID) VEH_MOST_RECENT_DT,
                        evt.CUID as CUID,
                        veh.vehicle_fact_start_dt AS START_DT,
                        veh.vehicle_fact_end_dt

                    FROM CCDB_OWNER.OH_W_VEHICLE_FACT veh
                    
                    JOIN (SELECT
                            CD_REC_THING_1,
                            MAX(SUBSTR (evt.cd_rec_party_1, 5, 10)) AS CUID
                          FROM CCDB_OWNER.OH_W_EVENT evt
                            WHERE 1=1
                            AND evt.cd_rec_object_type_source = 'CCDB'
                            AND evt.cd_rec_source = 'CUSTOMER_DIAGNOSTICS'
                            AND evt.event_start_dt <= process_dt
                            AND evt.event_end_dt > process_dt
                            AND evt.deleted_dt > process_dt
                            AND evt.cd_rec_thing_1_object_type = 'OBJECT~THING~VEHICLE'
                            AND evt.cd_rec_party_1_source = '110'
                            GROUP BY CD_REC_THING_1
                    ) evt
                        ON veh.CD_REC_THING = evt.CD_REC_THING_1
                        AND veh.cd_rec_thing_object_type_source = 'CCDB'
                        AND veh.cd_rec_source = 'CUSTOMER_DIAGNOSTICS'
                        AND veh.vehicle_fact_start_dt <= process_dt
                        AND veh.vehicle_fact_end_dt > process_dt
                        AND veh.deleted_dt > process_dt
                        AND veh.cd_rec_thing_object_type = 'OBJECT~THING~VEHICLE'
                )
                WHERE start_dt = VEH_MOST_RECENT_DT
                GROUP BY CUID
    ) VEH
        ON VEH.CUID = P1.party_src_val
    
    WHERE 1=1
        AND PEF.cd_rec_source = 'CUSTOMER_DIAGNOSTICS'
        AND PEF.cd_rec_party_source = '110'
        AND PEF.cd_rec_party_object_type = 'OBJECT~PARTY'
        AND PEF.PERSON_FACT_END_DT > process_dt
        AND PEF.PERSON_FACT_START_DT <= process_dt
        AND PEF.PERSON_FACT_START_DT > process_dt_hist
        AND PEF.DELETED_DT > process_dt
        --AND INCOME_TYPE.t_name_override IS NOT NULL
    );

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;  

    step(p_stepname => l_proc_name || ' TRUNC', p_source => 'N/A', p_target => 'STG_PRODUCT_INTEREST_AFP');  
    ETL_OWNER.etl_dbms_util.truncate_table('STG_PRODUCT_INTEREST_AFP',l_owner_name);
    
    step(p_stepname => l_proc_name || ' START INSERTING STG_PRODUCT_INTEREST_AFP', p_source => 'N/A', p_target => 'N/A');
	INSERT INTO pm_owner.STG_PRODUCT_INTEREST_AFP
    (  
        SELECT
            MAX(process_dt) AS INSERT_DT,
            CUSTOMERID,
            MAX(CASE WHEN product = 'PROD_01' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END ) AS RECENCY_WANT_CA_AFP,
            MAX(CASE WHEN product = 'PROD_02' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END ) AS RECENCY_WANT_STUDENTACC_AFP,
            MAX(CASE WHEN product in ('PROD_03', 'PROD_28') THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END ) AS RECENCY_WANT_CHILDACC_AFP,
            MAX(CASE WHEN product in ('PROD_04', 'PROD_28') THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_SA_AFP,
            MAX(CASE WHEN product = 'PROD_07' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_MTG_AFP,
            MAX(CASE WHEN product = 'PROD_09' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_CL_AFP,
            MAX(CASE WHEN product = 'PROD_10' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_CC_AFP,
            MAX(CASE WHEN product = 'PROD_11' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_OVD_AFP,
            MAX(CASE WHEN product in ('PROD_13', 'PROD_26', 'PROD_27') THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_FUND_AFP,
            MAX(CASE WHEN product in ('PROD_15', 'PROD_16') THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_TRAVELINS_AFP,
            MAX(CASE WHEN product in ('PROD_17', 'PROD_24', 'PROD_33') THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_LIFEINS_AFP,
            MAX(CASE WHEN product = 'PROD_18' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_HOMEINS_AFP,
            MAX(CASE WHEN product = 'PROD_22' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_MTPL_AFP,
            MAX(CASE WHEN product = 'PROD_21' THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN ROUND(event_dt - process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_CASCO_AFP,
            MAX(CASE WHEN product in ('PROD_23', 'PROD_30') THEN ( CASE WHEN interest_shown = 'INTERESTED' THEN TRUNC(event_dt) - TRUNC(process_dt_hist)
                                                          WHEN interest_shown = 'NOT_INTERESTED' THEN ROUND(process_dt_hist - event_dt)
                                                          WHEN interest_shown = 'LATER' THEN GREATEST(ROUND(event_dt - process_dt_hist) - 120, 0)
                                                          ELSE NULL END )
                     ELSE NULL END) AS RECENCY_WANT_PENSION_AFP
        FROM
        ( ---- zaujem/nezaujem o produkt
            SELECT
               CLB.CUSTOMERID,
               --SUBSTR (a.cd_rec_party_1, 5, 10) AS CUID,
               MAX(TRUNC(a.dt_bus_eff_from)) OVER (PARTITION BY CLB.CUSTOMERID, a.cd_rec_value_purpose) as most_recent_afp_prod_int_dt,
               TRUNC(a.dt_bus_eff_from) AS EVENT_DT,
               a.cd_variable AS interest_shown,
               a.cd_rec_value_purpose AS product
            FROM CCDB_OWNER.OH_W_EVENT_TICK a
            
            LEFT JOIN L0_OWNER.PARTY P
            ON P.party_src_val = SUBSTR(a.cd_rec_party_1, 5, 10)
            AND P.src_syst_cd = 110
            AND P.party_src_key like 'PMC%'
            
            LEFT JOIN L0_OWNER.PARTY_RLTD PR
            ON  PR.child_party_id = P.party_id
            AND PR.party_rltd_rsn_cd = 4
            AND PR.party_rltd_start_dt <= process_dt
            AND PR.party_rltd_end_dt > process_dt
            
            JOIN (SELECT CUSTOMERID
                  FROM PM_OWNER.ATTR_CLIENT_BASE 
                  WHERE MONTH_ID = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))
                ) CLB
            ON CLB.CUSTOMERID = PR.party_id
            
            WHERE a.cd_rec_source = 'CUSTOMER_DIAGNOSTICS'
            AND a.cd_rec_object_type = 'OBJECT~EVENT~OPPORTUNITY'
            AND a.dt_bus_eff_from <= process_dt
            AND a.dt_bus_eff_from > process_dt_hist
            AND a.DELETED_DT > process_dt
            AND a.cd_variable IN ('INTERESTED', 'LATER', 'NOT_INTERESTED')
        )
        
        WHERE event_dt = most_recent_afp_prod_int_dt
        GROUP BY CUSTOMERID
    );

    stat_ins(p_count => SQL%ROWCOUNT);
    commit;  

    step(p_stepname => l_proc_name || ' TRUNC', p_source => 'N/A', p_target => 'STG_EXTRA_INFO');    
    ETL_OWNER.etl_dbms_util.truncate_table('STG_EXTRA_INFO',l_owner_name);
    
    step(p_stepname => l_proc_name || ' START INSERTING STG_EXTRA_INFO', p_source => 'N/A', p_target => 'N/A');
    INSERT INTO PM_OWNER.STG_EXTRA_INFO
    (
        ---- rodinny stav a investicny profil
            SELECT DISTINCT
                MAX(process_dt) as INSERT_DT,
                ZAKLAD.CUSTOMERID,
                MAX(IP.INVESTICNY_PROFIL) INVESTICNY_PROFIL, --2019-09-13 - novy stlpec
                MAX(ZAKLAD.ROD_STAV_DRUH) ROD_STAV_DRUH,
                MAX(ZAKLAD.ROD_STAV_ROZVEDENY) ROD_STAV_ROZVEDENY,
                MAX(ZAKLAD.ROD_STAV_SLOBODNY) ROD_STAV_SLOBODNY,
                MAX(ZAKLAD.ROD_STAV_VDOVEC) ROD_STAV_VDOVEC,
                MAX(ZAKLAD.ROD_STAV_NIE_JE_ZNAME) ROD_STAV_NIE_JE_ZNAME,
                MAX(ZAKLAD.ROD_STAV_ZENATY) ROD_STAV_ZENATY,
                MAX(ZAKLAD.ROD_STAV_REG_PAR) ROD_STAV_REG_PAR,
                
                MAX(ZAKLAD.P_STAV_LIKVIDACIA) P_STAV_LIKVIDACIA,
                MAX(ZAKLAD.P_STAV_ZANIKLY_SUBJEKT) P_STAV_ZANIKLY_SUBJEKT,
                MAX(ZAKLAD.P_STAV_KONKURZ) P_STAV_KONKURZ,
                MAX(ZAKLAD.P_STAV_UKONCENE_PODNIKANIE) P_STAV_UKONCENE_PODNIKANIE
                
            FROM
            ( ---- rodinny stav
                SELECT
                    CLB.CUSTOMERID,
                    P.party_id AS PARTY_ID_CUID,
                    MAX(CASE WHEN PRS.party_role_cd = 79 and party_stat_type_cd = 10013 THEN 1 ELSE 0 END) as ROD_STAV_DRUH,
                    MAX(CASE WHEN PRS.party_role_cd = 79 and party_stat_type_cd = 10015 THEN 1 ELSE 0 END) as ROD_STAV_ROZVEDENY,
                    MAX(CASE WHEN PRS.party_role_cd = 79 and party_stat_type_cd = 10016 THEN 1 ELSE 0 END) as ROD_STAV_SLOBODNY,
                    MAX(CASE WHEN PRS.party_role_cd = 79 and party_stat_type_cd = 10017 THEN 1 ELSE 0 END) as ROD_STAV_VDOVEC,
                    MAX(CASE WHEN PRS.party_role_cd = 79 and party_stat_type_cd = 10018 THEN 1 ELSE 0 END) as ROD_STAV_NIE_JE_ZNAME,
                    MAX(CASE WHEN PRS.party_role_cd = 79 and party_stat_type_cd = 10019 THEN 1 ELSE 0 END) as ROD_STAV_ZENATY,
                    MAX(CASE WHEN PRS.party_role_cd = 79 and party_stat_type_cd = 10145 THEN 1 ELSE 0 END) as ROD_STAV_REG_PAR,
                    -- info o pravnickych osobach
                    MAX(CASE WHEN PRS.party_role_cd = 81 and party_stat_type_cd = 10006 THEN 1 ELSE 0 END) as P_STAV_LIKVIDACIA,
                    MAX(CASE WHEN PRS.party_role_cd = 81 and party_stat_type_cd = 10007 THEN 1 ELSE 0 END) as P_STAV_ZANIKLY_SUBJEKT,
                    MAX(CASE WHEN PRS.party_role_cd = 81 and party_stat_type_cd IN (10009, 10027) THEN 1 ELSE 0 END) as P_STAV_KONKURZ,
                    MAX(CASE WHEN PRS.party_role_cd = 81 and party_stat_type_cd = 10146 THEN 1 ELSE 0 END) as P_STAV_UKONCENE_PODNIKANIE
            
                FROM (SELECT
                        CUSTOMERID
                      FROM PM_OWNER.ATTR_CLIENT_BASE
                      WHERE MONTH_ID = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))
                ) CLB
                
                JOIN L0_OWNER.PARTY_RLTD PR
                    ON  PR.party_id = CLB.CUSTOMERID
                    AND PR.party_rltd_rsn_cd = 4
                    AND PR.party_rltd_start_dt <= process_dt
                    AND PR.party_rltd_end_dt > process_dt
                
                JOIN L0_OWNER.PARTY P
                    ON PR.child_party_id = P.party_id
                    AND P.src_syst_cd = 110
                    AND P.party_src_key like 'PMC%'
                
                -----   STAVY
                JOIN L0_OWNER.PARTY_ROLE_STAT PRS
                    ON  PRS.party_id = P.party_id
                    AND PRS.party_role_stat_start_dt <= process_dt
                    AND PRS.party_role_stat_end_dt > process_dt
                    AND PRS.party_stat_rsn_cd = 1 --Jiny
                    AND PRS.party_role_cd IN (79,81,256)  
                GROUP BY
                    CLB.CUSTOMERID, P.party_id
            
            ) ZAKLAD
            
            --2019-09-13 - START
            LEFT JOIN (
                      SELECT
                      PS.party_id,
                      MAX(SG.SEG_GEN_DS) as INVESTICNY_PROFIL
                      FROM L0_owner.party_seg PS
                      
                      join L0_owner.SEG_GEN SG
                      on SG.seg_cd = PS.seg_cd
                      and SG.seg_gen_end_dt > process_dt   
                      and SG.seg_gen_start_dt <= process_dt
                      
                      WHERE 
                      PS.model_id in (520,213) AND
                      PS.party_seg_end_dt > process_dt AND
                      PS.party_seg_start_dt <= process_dt
                      
                      GROUP BY PS.party_id
            ) IP
                ON IP.party_id = ZAKLAD.party_id_cuid
                
            GROUP BY ZAKLAD.CUSTOMERID
  );
  
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;  
  
    -- ATTR_AFP
    step(p_stepname => l_proc_name || ' START', p_source => 'N/A', p_target => 'N/A');   
    mng_partitions(l_owner_name, l_table_name, l_proc_name, process_dt);   
    step(p_stepname => l_proc_name || ' INSERT ' || l_table_name, p_source => 'L0.*', p_target => l_table_name);  
    INSERT INTO PM_OWNER.ATTR_AFP(
            MONTH_ID,
            CUSTOMERID,
            I_AFP_Y,
            I_AFP_5Y,
            RECENCY_AFP,
            CAT_HOUSING_TYPE_AFP_Y,
            CAT_INCOME_TYPE_AFP_Y,
            CAT_HOUSING_TYPE_AFP,
            CAT_INCOME_TYPE_AFP,
            CAT_MARITAL_STATUS,
            CAT_PRIMARY_BANK_AFP,
            CAT_REAL_ESTATE_AFP,
            CAT_VEHICLE_AFP,
            CAT_TITLE_PD,
            LEN_STUDY_Y_PD,
            NET_INCOME_AFP_Y,
            LIABILITIES_AFP_Y,
            SAVINGS_AFP_Y,
            INCOME_RATIO_SAVED_AFP_Y,
            INCOME_RATIO_LIABILITIES_AFP_Y,
            I_MAIN_BANK_AFP_Y,
            I_EMPLOYEE_AFP_Y,
            I_PARENTALLEAVE_AFP_Y,
            I_STUDENT_AFP_Y,
            I_ENTREPRENEUR_AFP_Y,
            I_OTHERINCOME_AFP_Y,
            NET_INCOME_AFP,
            LIABILITIES_AFP,
            SAVINGS_AFP,
            INCOME_RATIO_SAVED_AFP,
            INCOME_RATIO_LIABILITIES_AFP,
            I_MAIN_BANK_AFP,
            I_EMPLOYEE_AFP,
            I_PARENTALLEAVE_AFP,
            I_STUDENT_AFP,
            I_PENSION_AFP,
            I_ENTREPRENEUR_AFP,
            I_OTHERINCOME_AFP,
            RECENCY_WANT_BANK_PRODUCT_AFP,
            RECENCY_WANT_INS_PRODUCT_AFP,
            RECENCY_WANT_LOAN_AFP,
            RECENCY_WANT_DDA_AFP,
            RECENCY_WANT_DEPOSIT_AFP,
            RECENCY_WANT_INS_CAR_AFP,
            RECENCY_WANT_CA_AFP,
            RECENCY_WANT_STUDENTACC_AFP,
            RECENCY_WANT_CHILDACC_AFP,
            RECENCY_WANT_SA_AFP,
            RECENCY_WANT_MTG_AFP,
            RECENCY_WANT_CL_AFP,
            RECENCY_WANT_CC_AFP,
            RECENCY_WANT_OVD_AFP,
            RECENCY_WANT_FUND_AFP,
            RECENCY_WANT_TRAVELINS_AFP,
            RECENCY_WANT_LIFEINS_AFP,
            RECENCY_WANT_HOMEINS_AFP,
            RECENCY_WANT_MTPL_AFP,
            RECENCY_WANT_CASCO_AFP,
            RECENCY_WANT_PENSION_AFP,
            I_WANT_BANK_PRODUCT_AFP_M,
            I_WANT_INS_PRODUCT_AFP_M,
            I_WANT_LOAN_AFP_M,
            I_WANT_DDA_AFP_M,
            I_WANT_DEPOSIT_AFP_M,
            I_WANT_INS_CAR_AFP_M,
            I_WANT_CA_AFP_M,
            I_WANT_STUDENTACC_AFP_M,
            I_WANT_CHILDACC_AFP_M,
            I_WANT_SA_AFP_M,
            I_WANT_MTG_AFP_M,
            I_WANT_CL_AFP_M,
            I_WANT_CC_AFP_M,
            I_WANT_OVD_AFP_M,
            I_WANT_FUND_AFP_M,
            I_WANT_TRAVELINS_AFP_M,
            I_WANT_LIFEINS_AFP_M,
            I_WANT_HOMEINS_AFP_M,
            I_WANT_MTPL_AFP_M,
            I_WANT_CASCO_AFP_M,
            I_WANT_PENSION_AFP_M,
            I_WANT_BANK_PRODUCT_AFP_Q,
            I_WANT_INS_PRODUCT_AFP_Q,
            I_WANT_LOAN_AFP_Q,
            I_WANT_DDA_AFP_Q,
            I_WANT_DEPOSIT_AFP_Q,
            I_WANT_INS_CAR_AFP_Q,
            I_WANT_CA_AFP_Q,
            I_WANT_STUDENTACC_AFP_Q,
            I_WANT_CHILDACC_AFP_Q,
            I_WANT_SA_AFP_Q,
            I_WANT_MTG_AFP_Q,
            I_WANT_CL_AFP_Q,
            I_WANT_CC_AFP_Q,
            I_WANT_OVD_AFP_Q,
            I_WANT_FUND_AFP_Q,
            I_WANT_TRAVELINS_AFP_Q,
            I_WANT_LIFEINS_AFP_Q,
            I_WANT_HOMEINS_AFP_Q,
            I_WANT_MTPL_AFP_Q,
            I_WANT_CASCO_AFP_Q,
            I_WANT_PENSION_AFP_Q,
            INVESTING_PROFILE,
            I_MARITAL_STATUS_COMPANION,
            I_MARITAL_STATUS_DIVORCED,
            I_MARITAL_STATUS_SINGLE,
            I_MARITAL_STATUS_WIDOWED,
            I_MARITAL_STATUS_UNKNOWN,
            I_MARITAL_STATUS_MARRIED,
            I_COMPANY_LIQ_OR_BANKRUPT,
            I_COMPANY_OUT_OF_BUSINESS,
            I_COMPANY_LIQ_BANKRUPT_OOB,
            RECENCY_REAL_ESTATE_AFP,
            C_HOUSE_AFP,
            C_APARTMENT_AFP,
            C_COTTAGE_AFP,
            C_BUILDING_LOT_AFP,
            C_COMMERCIAL_SPACE_AFP,
            C_GARAGE_AFP,
            I_REFUSE_ANSWER_REAL_ESTATE_AFP,
            C_TOTAL_REAL_ESTATE_AFP,
            RECENCY_VEHICLE_AFP,
            C_OWN_CAR_AFP,
            C_LOAN_CAR_AFP,
            C_WORK_CAR_AFP,
            C_MOTORCYCLE_AFP,
            I_NO_OWN_VEHICLE_AFP,
            I_REFUSE_ANSWER_VEHICLE_AFP,
            C_TOTAL_VEHICLE_AFP  
      )
      (
            SELECT 
                BASE.MONTH_ID,
                BASE.CUSTOMERID,
                
                CASE WHEN COALESCE(AFP.RECENCY_AFP, process_dt - process_dt_hist) < 366 THEN 1 ELSE 0 END I_AFP_Y,
                CASE WHEN COALESCE(AFP.RECENCY_AFP, process_dt - process_dt_hist) < 1826 THEN 1 ELSE 0 END I_AFP_5Y,
                COALESCE(AFP.RECENCY_AFP, process_dt - process_dt_hist) RECENCY_AFP,
                
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_HOUSING_TYPE, 'UNKNOWN') ELSE 'UNKNOWN' END CAT_HOUSING_TYPE_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_INCOME_TYPE, 'UNKNOWN') ELSE 'UNKNOWN' END CAT_INCOME_TYPE_AFP_Y,
                COALESCE(AFP.AFP_HOUSING_TYPE, 'UNKNOWN') CAT_HOUSING_TYPE_AFP,
                COALESCE(AFP.AFP_INCOME_TYPE, 'UNKNOWN') CAT_INCOME_TYPE_AFP,
                CASE WHEN SEI.ROD_STAV_DRUH = 1 THEN 'COMPANION'
                     WHEN SEI.ROD_STAV_ROZVEDENY = 1 THEN 'DIVORCED'
                     WHEN SEI.ROD_STAV_SLOBODNY = 1 THEN 'SINGLE'
                     WHEN SEI.ROD_STAV_VDOVEC = 1 THEN 'WIDOWED'
                     WHEN (SEI.ROD_STAV_ZENATY = 1 OR SEI.ROD_STAV_REG_PAR = 1) THEN 'MARRIED'
                     ELSE 'UNKNOWN' END AS CAT_MARITAL_STATUS,
                COALESCE(AFP.AFP_PRIMARY_BANK, 'UNKNOWN') CAT_PRIMARY_BANK_AFP,
                COALESCE(AFP.CAT_REAL_ESTATE, 'UNKNOWN') CAT_REAL_ESTATE_AFP,
                COALESCE(AFP.CAT_VEHICLE, 'UNKNOWN') CAT_VEHICLE_AFP,
                COALESCE(TTL.TITLE, 'NONE') CAT_TITLE_PD,
    
                CASE WHEN TTL.TITLE = 'Bc.' THEN 3
                     WHEN TTL.TITLE = 'Mgr.' THEN 5
                     WHEN TTL.TITLE = 'PhDr.' THEN 6
                     WHEN TTL.TITLE = 'PhD.' THEN 8
                     WHEN TTL.TITLE = 'MUDr.' THEN 6
                     WHEN TTL.TITLE = 'JUDr.' THEN 6
                     WHEN TTL.TITLE = 'Academic' THEN 10
                     WHEN TTL.TITLE = 'OTHER' THEN 3 --- most of the titles in category OTHER require at least 3 years to achieve
                     ELSE 0 END AS LEN_STUDY_Y_PD,
                
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_NET_INCOME, -1) ELSE -1 END NET_INCOME_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_LIABILITIES, -1) ELSE -1 END LIABILITIES_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_SAVINGS, -1) ELSE -1 END SAVINGS_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_INCOME_RATIO_SAVED, 0) ELSE 0 END INCOME_RATIO_SAVED_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_INCOME_RATIO_LIABILITIES, 0) ELSE 0 END INCOME_RATIO_LIABILITIES_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_MAIN_BANK, -1) ELSE -1 END I_MAIN_BANK_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_EMPLOYEE, -1) ELSE -1 END I_EMPLOYEE_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_PARENTALLEAVE, -1) ELSE -1 END I_PARENTALLEAVE_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_STUDENT, -1) ELSE -1 END I_STUDENT_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_ENTREPRENEUR, -1) ELSE -1 END I_ENTREPRENEUR_AFP_Y,
                CASE WHEN AFP.RECENCY_AFP < 366 THEN COALESCE(AFP.AFP_OTHERINCOME, -1) ELSE -1 END I_OTHERINCOME_AFP_Y,
    
                COALESCE(AFP.AFP_NET_INCOME, -1) NET_INCOME_AFP,
                COALESCE(AFP.AFP_LIABILITIES, -1) LIABILITIES_AFP,
                COALESCE(AFP.AFP_SAVINGS, -1) SAVINGS_AFP,
                COALESCE(AFP.AFP_INCOME_RATIO_SAVED, 0) INCOME_RATIO_SAVED_AFP,
                COALESCE(AFP.AFP_INCOME_RATIO_LIABILITIES, 0) INCOME_RATIO_LIABILITIES_AFP,
                COALESCE(AFP.AFP_MAIN_BANK, -1) I_MAIN_BANK_AFP,
                COALESCE(AFP.AFP_EMPLOYEE, -1) I_EMPLOYEE_AFP,
                COALESCE(AFP.AFP_PARENTALLEAVE, -1) I_PARENTALLEAVE_AFP,
                COALESCE(AFP.AFP_STUDENT, -1) I_STUDENT_AFP,
                COALESCE(AFP.AFP_PENSION, -1) I_PENSION_AFP,
                COALESCE(AFP.AFP_ENTREPRENEUR, -1) I_ENTREPRENEUR_AFP,
                COALESCE(AFP.AFP_OTHERINCOME, -1) I_OTHERINCOME_AFP,
    
                CASE WHEN ABS(GREATEST(PINT.RECENCY_WANT_CA_AFP, PINT.RECENCY_WANT_STUDENTACC_AFP,
                                       PINT.RECENCY_WANT_CHILDACC_AFP, PINT.RECENCY_WANT_SA_AFP,
                                       PINT.RECENCY_WANT_MTG_AFP, PINT.RECENCY_WANT_CL_AFP,
                                       PINT.RECENCY_WANT_CC_AFP, PINT.RECENCY_WANT_OVD_AFP,
                                       PINT.RECENCY_WANT_FUND_AFP)) >= ABS(LEAST(PINT.RECENCY_WANT_CA_AFP, PINT.RECENCY_WANT_STUDENTACC_AFP,
                                                                                 PINT.RECENCY_WANT_CHILDACC_AFP, PINT.RECENCY_WANT_SA_AFP,
                                                                                 PINT.RECENCY_WANT_MTG_AFP, PINT.RECENCY_WANT_CL_AFP,
                                                                                 PINT.RECENCY_WANT_CC_AFP, PINT.RECENCY_WANT_OVD_AFP,
                                                                                 PINT.RECENCY_WANT_FUND_AFP)) 
                     THEN GREATEST(PINT.RECENCY_WANT_CA_AFP, PINT.RECENCY_WANT_STUDENTACC_AFP,
                                   PINT.RECENCY_WANT_CHILDACC_AFP, PINT.RECENCY_WANT_SA_AFP,
                                   PINT.RECENCY_WANT_MTG_AFP, PINT.RECENCY_WANT_CL_AFP,
                                   PINT.RECENCY_WANT_CC_AFP, PINT.RECENCY_WANT_OVD_AFP,
                                   PINT.RECENCY_WANT_FUND_AFP)
                     ELSE LEAST(PINT.RECENCY_WANT_CA_AFP, PINT.RECENCY_WANT_STUDENTACC_AFP,
                                PINT.RECENCY_WANT_CHILDACC_AFP, PINT.RECENCY_WANT_SA_AFP,
                                PINT.RECENCY_WANT_MTG_AFP, PINT.RECENCY_WANT_CL_AFP,
                                PINT.RECENCY_WANT_CC_AFP, PINT.RECENCY_WANT_OVD_AFP,
                                PINT.RECENCY_WANT_FUND_AFP) END AS RECENCY_WANT_BANK_PRODUCT_AFP,
                
                CASE WHEN ABS(GREATEST(PINT.RECENCY_WANT_TRAVELINS_AFP, PINT.RECENCY_WANT_LIFEINS_AFP,
                                       PINT.RECENCY_WANT_HOMEINS_AFP, PINT.RECENCY_WANT_MTPL_AFP,
                                       PINT.RECENCY_WANT_CASCO_AFP)) >= ABS(LEAST(PINT.RECENCY_WANT_TRAVELINS_AFP, PINT.RECENCY_WANT_LIFEINS_AFP,
                                                                                  PINT.RECENCY_WANT_HOMEINS_AFP, PINT.RECENCY_WANT_MTPL_AFP,
                                                                                  PINT.RECENCY_WANT_CASCO_AFP))
                     THEN GREATEST(PINT.RECENCY_WANT_TRAVELINS_AFP, PINT.RECENCY_WANT_LIFEINS_AFP,
                                   PINT.RECENCY_WANT_HOMEINS_AFP, PINT.RECENCY_WANT_MTPL_AFP,
                                   PINT.RECENCY_WANT_CASCO_AFP)
                     ELSE LEAST(PINT.RECENCY_WANT_TRAVELINS_AFP, PINT.RECENCY_WANT_LIFEINS_AFP,
                                PINT.RECENCY_WANT_HOMEINS_AFP, PINT.RECENCY_WANT_MTPL_AFP,
                                PINT.RECENCY_WANT_CASCO_AFP) END AS RECENCY_WANT_INS_PRODUCT_AFP,
                               
                CASE WHEN ABS(GREATEST(PINT.RECENCY_WANT_MTG_AFP, PINT.RECENCY_WANT_CL_AFP,
                                       PINT.RECENCY_WANT_CC_AFP, PINT.RECENCY_WANT_OVD_AFP)) >= ABS(LEAST(PINT.RECENCY_WANT_MTG_AFP, PINT.RECENCY_WANT_CL_AFP,
                                                                                                          PINT.RECENCY_WANT_CC_AFP, PINT.RECENCY_WANT_OVD_AFP))
                     THEN GREATEST(PINT.RECENCY_WANT_MTG_AFP, PINT.RECENCY_WANT_CL_AFP,
                                   PINT.RECENCY_WANT_CC_AFP, PINT.RECENCY_WANT_OVD_AFP)
                     ELSE LEAST(PINT.RECENCY_WANT_MTG_AFP, PINT.RECENCY_WANT_CL_AFP,
                                PINT.RECENCY_WANT_CC_AFP, PINT.RECENCY_WANT_OVD_AFP) END AS RECENCY_WANT_LOAN_AFP,
                                
                CASE WHEN ABS(GREATEST(PINT.RECENCY_WANT_CA_AFP, PINT.RECENCY_WANT_STUDENTACC_AFP,
                                       PINT.RECENCY_WANT_CHILDACC_AFP, PINT.RECENCY_WANT_SA_AFP)) >= ABS(LEAST(PINT.RECENCY_WANT_CA_AFP, PINT.RECENCY_WANT_STUDENTACC_AFP,
                                                                                                               PINT.RECENCY_WANT_CHILDACC_AFP, PINT.RECENCY_WANT_SA_AFP))
                     THEN GREATEST(PINT.RECENCY_WANT_CA_AFP, PINT.RECENCY_WANT_STUDENTACC_AFP,
                                   PINT.RECENCY_WANT_CHILDACC_AFP, PINT.RECENCY_WANT_SA_AFP)
                     ELSE LEAST(PINT.RECENCY_WANT_CA_AFP, PINT.RECENCY_WANT_STUDENTACC_AFP,
                                PINT.RECENCY_WANT_CHILDACC_AFP, PINT.RECENCY_WANT_SA_AFP) END AS RECENCY_WANT_DDA_AFP,
        
                CASE WHEN ABS(GREATEST(PINT.RECENCY_WANT_SA_AFP, PINT.RECENCY_WANT_FUND_AFP)) >= ABS(LEAST(PINT.RECENCY_WANT_SA_AFP, PINT.RECENCY_WANT_FUND_AFP))
                     THEN GREATEST(PINT.RECENCY_WANT_SA_AFP, PINT.RECENCY_WANT_FUND_AFP)
                     ELSE LEAST(PINT.RECENCY_WANT_SA_AFP, PINT.RECENCY_WANT_FUND_AFP) END AS RECENCY_WANT_DEPOSIT_AFP,
    
                CASE WHEN ABS(GREATEST(PINT.RECENCY_WANT_MTPL_AFP, PINT.RECENCY_WANT_CASCO_AFP)) >= ABS(LEAST(PINT.RECENCY_WANT_SA_AFP, PINT.RECENCY_WANT_FUND_AFP))
                     THEN GREATEST(PINT.RECENCY_WANT_SA_AFP, PINT.RECENCY_WANT_FUND_AFP)
                     ELSE LEAST(PINT.RECENCY_WANT_SA_AFP, PINT.RECENCY_WANT_FUND_AFP) END AS RECENCY_WANT_INS_CAR_AFP,
    
                PINT.RECENCY_WANT_CA_AFP,
                PINT.RECENCY_WANT_STUDENTACC_AFP,
                PINT.RECENCY_WANT_CHILDACC_AFP,
                PINT.RECENCY_WANT_SA_AFP,
                PINT.RECENCY_WANT_MTG_AFP,
                PINT.RECENCY_WANT_CL_AFP,
                PINT.RECENCY_WANT_CC_AFP,
                PINT.RECENCY_WANT_OVD_AFP,
                PINT.RECENCY_WANT_FUND_AFP,
                PINT.RECENCY_WANT_TRAVELINS_AFP,
                PINT.RECENCY_WANT_LIFEINS_AFP,
                PINT.RECENCY_WANT_HOMEINS_AFP,
                PINT.RECENCY_WANT_MTPL_AFP,
                PINT.RECENCY_WANT_CASCO_AFP,
                PINT.RECENCY_WANT_PENSION_AFP,
                
                CASE WHEN ABS(GREATEST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M,
                                       PINT.I_WANT_SA_AFP_M, PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M,
                                       PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M, PINT.I_WANT_FUND_AFP_M)) >= ABS(LEAST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M,
                                                                                                                             PINT.I_WANT_SA_AFP_M, PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M,
                                                                                                                             PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M, PINT.I_WANT_FUND_AFP_M))
                     THEN GREATEST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M,
                                   PINT.I_WANT_SA_AFP_M, PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M,
                                   PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M, PINT.I_WANT_FUND_AFP_M)
                     ELSE LEAST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M,
                                PINT.I_WANT_SA_AFP_M, PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M,
                                PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M, PINT.I_WANT_FUND_AFP_M) END AS I_WANT_BANK_PRODUCT_AFP_M,
                                
                CASE WHEN ABS(GREATEST(PINT.I_WANT_TRAVELINS_AFP_M, PINT.I_WANT_LIFEINS_AFP_M, PINT.I_WANT_HOMEINS_AFP_M,
                                       PINT.I_WANT_MTPL_AFP_M, PINT.I_WANT_CASCO_AFP_M)) >= ABS(LEAST(PINT.I_WANT_TRAVELINS_AFP_M, PINT.I_WANT_LIFEINS_AFP_M, PINT.I_WANT_HOMEINS_AFP_M,
                                                                                                      PINT.I_WANT_MTPL_AFP_M, PINT.I_WANT_CASCO_AFP_M))
                    THEN GREATEST(PINT.I_WANT_TRAVELINS_AFP_M, PINT.I_WANT_LIFEINS_AFP_M, PINT.I_WANT_HOMEINS_AFP_M,
                                  PINT.I_WANT_MTPL_AFP_M, PINT.I_WANT_CASCO_AFP_M)
                    ELSE LEAST(PINT.I_WANT_TRAVELINS_AFP_M, PINT.I_WANT_LIFEINS_AFP_M, PINT.I_WANT_HOMEINS_AFP_M,
                               PINT.I_WANT_MTPL_AFP_M, PINT.I_WANT_CASCO_AFP_M) END AS I_WANT_INS_PRODUCT_AFP_M,
                                             
                CASE WHEN ABS(GREATEST(PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M, PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M)) >= ABS(LEAST(PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M, PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M))
                    THEN GREATEST(PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M, PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M)
                    ELSE LEAST(PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M, PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M) END AS I_WANT_LOAN_AFP_M,
                               
                CASE WHEN ABS(GREATEST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M)) >= ABS(LEAST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M))
                     THEN GREATEST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M)
                     ELSE LEAST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M) END AS I_WANT_DDA_AFP_M,
                
                CASE WHEN ABS(GREATEST(PINT.I_WANT_SA_AFP_M, PINT.I_WANT_FUND_AFP_M)) >= ABS(LEAST(PINT.I_WANT_SA_AFP_M, PINT.I_WANT_FUND_AFP_M))
                     THEN GREATEST(PINT.I_WANT_SA_AFP_M, PINT.I_WANT_FUND_AFP_M)
                     ELSE LEAST(PINT.I_WANT_SA_AFP_M, PINT.I_WANT_FUND_AFP_M) END AS I_WANT_DEPOSIT_AFP_M,
                    
                CASE WHEN ABS(GREATEST(PINT.I_WANT_CASCO_AFP_M, PINT.I_WANT_MTPL_AFP_M)) >= ABS(LEAST(PINT.I_WANT_CASCO_AFP_M, PINT.I_WANT_MTPL_AFP_M))
                     THEN GREATEST(PINT.I_WANT_CASCO_AFP_M, PINT.I_WANT_MTPL_AFP_M)
                     ELSE LEAST(PINT.I_WANT_CASCO_AFP_M, PINT.I_WANT_MTPL_AFP_M) END AS I_WANT_INS_CAR_AFP_M,
    
                PINT.I_WANT_CA_AFP_M,
                PINT.I_WANT_STUDENTACC_AFP_M,
                PINT.I_WANT_CHILDACC_AFP_M,
                PINT.I_WANT_SA_AFP_M,
                PINT.I_WANT_MTG_AFP_M,
                PINT.I_WANT_CL_AFP_M,
                PINT.I_WANT_CC_AFP_M,
                PINT.I_WANT_OVD_AFP_M,
                PINT.I_WANT_FUND_AFP_M,
                PINT.I_WANT_TRAVELINS_AFP_M,
                PINT.I_WANT_LIFEINS_AFP_M,
                PINT.I_WANT_HOMEINS_AFP_M,
                PINT.I_WANT_MTPL_AFP_M,
                PINT.I_WANT_CASCO_AFP_M,
                PINT.I_WANT_PENSION_AFP_M,
    
                CASE WHEN ABS(GREATEST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M,
                                       PINT.I_WANT_SA_AFP_M, PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M,
                                       PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M, PINT.I_WANT_FUND_AFP_M)) >= ABS(LEAST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M,
                                                                                                                             PINT.I_WANT_SA_AFP_M, PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M,
                                                                                                                             PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M, PINT.I_WANT_FUND_AFP_M))
                     THEN GREATEST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M,
                                   PINT.I_WANT_SA_AFP_M, PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M,
                                   PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M, PINT.I_WANT_FUND_AFP_M)
                     ELSE LEAST(PINT.I_WANT_CA_AFP_M, PINT.I_WANT_STUDENTACC_AFP_M, PINT.I_WANT_CHILDACC_AFP_M,
                                PINT.I_WANT_SA_AFP_M, PINT.I_WANT_MTG_AFP_M, PINT.I_WANT_CL_AFP_M,
                                PINT.I_WANT_CC_AFP_M, PINT.I_WANT_OVD_AFP_M, PINT.I_WANT_FUND_AFP_M) END AS I_WANT_BANK_PRODUCT_AFP_M,
                                
                CASE WHEN ABS(GREATEST(PINT.I_WANT_TRAVELINS_AFP_Q, PINT.I_WANT_LIFEINS_AFP_Q, PINT.I_WANT_HOMEINS_AFP_Q,
                                       PINT.I_WANT_MTPL_AFP_Q, PINT.I_WANT_CASCO_AFP_Q)) >= ABS(LEAST(PINT.I_WANT_TRAVELINS_AFP_Q, PINT.I_WANT_LIFEINS_AFP_Q, PINT.I_WANT_HOMEINS_AFP_Q,
                                                                                                      PINT.I_WANT_MTPL_AFP_Q, PINT.I_WANT_CASCO_AFP_Q))
                    THEN GREATEST(PINT.I_WANT_TRAVELINS_AFP_Q, PINT.I_WANT_LIFEINS_AFP_Q, PINT.I_WANT_HOMEINS_AFP_Q,
                                  PINT.I_WANT_MTPL_AFP_Q, PINT.I_WANT_CASCO_AFP_Q)
                    ELSE LEAST(PINT.I_WANT_TRAVELINS_AFP_Q, PINT.I_WANT_LIFEINS_AFP_Q, PINT.I_WANT_HOMEINS_AFP_Q,
                               PINT.I_WANT_MTPL_AFP_Q, PINT.I_WANT_CASCO_AFP_Q) END AS I_WANT_INS_PRODUCT_AFP_Q,
                                             
                CASE WHEN ABS(GREATEST(PINT.I_WANT_MTG_AFP_Q, PINT.I_WANT_CL_AFP_Q, PINT.I_WANT_CC_AFP_Q, PINT.I_WANT_OVD_AFP_Q)) >= ABS(LEAST(PINT.I_WANT_MTG_AFP_Q, PINT.I_WANT_CL_AFP_Q, PINT.I_WANT_CC_AFP_Q, PINT.I_WANT_OVD_AFP_Q))
                    THEN GREATEST(PINT.I_WANT_MTG_AFP_Q, PINT.I_WANT_CL_AFP_Q, PINT.I_WANT_CC_AFP_Q, PINT.I_WANT_OVD_AFP_Q)
                    ELSE LEAST(PINT.I_WANT_MTG_AFP_Q, PINT.I_WANT_CL_AFP_Q, PINT.I_WANT_CC_AFP_Q, PINT.I_WANT_OVD_AFP_Q) END AS I_WANT_LOAN_AFP_Q,
                               
                CASE WHEN ABS(GREATEST(PINT.I_WANT_CA_AFP_Q, PINT.I_WANT_STUDENTACC_AFP_Q, PINT.I_WANT_CHILDACC_AFP_Q)) >= ABS(LEAST(PINT.I_WANT_CA_AFP_Q, PINT.I_WANT_STUDENTACC_AFP_Q, PINT.I_WANT_CHILDACC_AFP_Q))
                     THEN GREATEST(PINT.I_WANT_CA_AFP_Q, PINT.I_WANT_STUDENTACC_AFP_Q, PINT.I_WANT_CHILDACC_AFP_Q)
                     ELSE LEAST(PINT.I_WANT_CA_AFP_Q, PINT.I_WANT_STUDENTACC_AFP_Q, PINT.I_WANT_CHILDACC_AFP_Q) END AS I_WANT_DDA_AFP_Q,
                
                CASE WHEN ABS(GREATEST(PINT.I_WANT_SA_AFP_Q, PINT.I_WANT_FUND_AFP_Q)) >= ABS(LEAST(PINT.I_WANT_SA_AFP_Q, PINT.I_WANT_FUND_AFP_Q))
                     THEN GREATEST(PINT.I_WANT_SA_AFP_Q, PINT.I_WANT_FUND_AFP_Q)
                     ELSE LEAST(PINT.I_WANT_SA_AFP_Q, PINT.I_WANT_FUND_AFP_Q) END AS I_WANT_DEPOSIT_AFP_Q,
                    
                CASE WHEN ABS(GREATEST(PINT.I_WANT_CASCO_AFP_Q, PINT.I_WANT_MTPL_AFP_Q)) >= ABS(LEAST(PINT.I_WANT_CASCO_AFP_Q, PINT.I_WANT_MTPL_AFP_Q))
                     THEN GREATEST(PINT.I_WANT_CASCO_AFP_Q, PINT.I_WANT_MTPL_AFP_Q)
                     ELSE LEAST(PINT.I_WANT_CASCO_AFP_Q, PINT.I_WANT_MTPL_AFP_Q) END AS I_WANT_INS_CAR_AFP_Q,
    
                PINT.I_WANT_CA_AFP_Q,
                PINT.I_WANT_STUDENTACC_AFP_Q,
                PINT.I_WANT_CHILDACC_AFP_Q,
                PINT.I_WANT_SA_AFP_Q,
                PINT.I_WANT_MTG_AFP_Q,
                PINT.I_WANT_CL_AFP_Q,
                PINT.I_WANT_CC_AFP_Q,
                PINT.I_WANT_OVD_AFP_Q,
                PINT.I_WANT_FUND_AFP_Q,
                PINT.I_WANT_TRAVELINS_AFP_Q,
                PINT.I_WANT_LIFEINS_AFP_Q,
                PINT.I_WANT_HOMEINS_AFP_Q,
                PINT.I_WANT_MTPL_AFP_Q,
                PINT.I_WANT_CASCO_AFP_Q,
                PINT.I_WANT_PENSION_AFP_Q,
    
                CASE WHEN SEI.INVESTICNY_PROFIL = 'Velmi opatrný' THEN 1
                     WHEN SEI.INVESTICNY_PROFIL = 'Opatrný' THEN 2
                     WHEN SEI.INVESTICNY_PROFIL = 'Dočasný' THEN 3
                     WHEN SEI.INVESTICNY_PROFIL = 'Odvážný' THEN 4
                     WHEN SEI.INVESTICNY_PROFIL = 'Velmi odvážný' THEN 5
                     ELSE 0 END AS INVESTING_PROFILE,
    
                COALESCE(SEI.ROD_STAV_DRUH, 0) AS I_MARITAL_STATUS_COMPANION,
                COALESCE(SEI.ROD_STAV_ROZVEDENY, 0) AS I_MARITAL_STATUS_DIVORCED,
                COALESCE(SEI.ROD_STAV_SLOBODNY, 0) AS I_MARITAL_STATUS_SINGLE,
                COALESCE(SEI.ROD_STAV_VDOVEC, 0) AS I_MARITAL_STATUS_WIDOWED,
                COALESCE(SEI.ROD_STAV_NIE_JE_ZNAME, 0) AS I_MARITAL_STATUS_UNKNOWN,
                COALESCE(GREATEST(SEI.ROD_STAV_ZENATY, SEI.ROD_STAV_REG_PAR), 0) AS I_MARITAL_STATUS_MARRIED,
     
                -- info pravnicke osoby
                COALESCE(GREATEST(SEI.P_STAV_LIKVIDACIA, SEI.P_STAV_KONKURZ), 0) AS I_COMPANY_LIQ_OR_BANKRUPT,
                COALESCE(GREATEST(SEI.P_STAV_UKONCENE_PODNIKANIE, SEI.P_STAV_ZANIKLY_SUBJEKT), 0) AS I_COMPANY_OUT_OF_BUSINESS,
                COALESCE(GREATEST(SEI.P_STAV_ZANIKLY_SUBJEKT, SEI.P_STAV_KONKURZ, SEI.P_STAV_UKONCENE_PODNIKANIE, SEI.P_STAV_ZANIKLY_SUBJEKT), 0) AS I_COMPANY_LIQ_BANKRUPT_OOB,
    
                COALESCE(AFP.RECENCY_REAL_ESTATE_AFP, process_dt - process_dt_hist) AS RECENCY_REAL_ESTATE_AFP,
                COALESCE(AFP.C_HOUSE, 0) C_HOUSE_AFP, -- rodinny dom
                COALESCE(AFP.C_APARTMENT, 0) C_APARTMENT_AFP, -- byt
                COALESCE(AFP.C_COTTAGE, 0) C_COTTAGE_AFP, -- chata
                COALESCE(AFP.C_BUILDING_LOT, 0) C_BUILDING_LOT_AFP, -- stavebny pozemok
                COALESCE(AFP.C_COMMERCIAL_SPACE + AFP.C_NON_RESIDENTIAL_SPACE, 0) C_COMMERCIAL_SPACE_AFP, -- podnikatelsky priestor alebo nebytovy priestor
                COALESCE(AFP.C_GARAGE, 0) AS C_GARAGE_AFP, -- garaz/park. miesto
                COALESCE(AFP.REFUSE_ANSWER_REAL_ESTATE, 0) I_REFUSE_ANSWER_REAL_ESTATE_AFP, -- klient nechce uviest
                COALESCE(AFP.C_TOTAL_REAL_ESTATE, 0) C_TOTAL_REAL_ESTATE_AFP,
    
                COALESCE(AFP.RECENCY_VEHICLE_AFP, process_dt - process_dt_hist) AS RECENCY_VEHICLE_AFP,
                COALESCE(AFP.C_OWN_CAR, 0) C_OWN_CAR_AFP,
                COALESCE(AFP.C_LOAN_CAR, 0) C_LOAN_CAR_AFP,
                COALESCE(AFP.C_WORK_CAR, 0) C_WORK_CAR_AFP,
                COALESCE(AFP.C_MOTORCYCLE, 0) C_MOTORCYCLE_AFP,
                COALESCE(AFP.NO_OWN_VEHICLE, 0) I_NO_OWN_VEHICLE_AFP,
                COALESCE(AFP.REFUSE_ANSWER_VEHICLE, 0) I_REFUSE_ANSWER_VEHICLE_AFP,
                COALESCE(AFP.C_TOTAL_VEHICLE, 0) C_TOTAL_VEHICLE_AFP
    
            FROM (SELECT CUSTOMERID, MONTH_ID
                  FROM PM_OWNER.ATTR_CLIENT_BASE
                  WHERE MONTH_ID = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))
            ) BASE
    
            LEFT JOIN
            ( --- AFP
                SELECT
                    CUSTOMERID,
                    MIN(AFP_MOST_RECENT_DT) AFP_MOST_RECENT_DT,
                    MIN(DAYS_SINCE_FIRST_AFP) DAYS_SINCE_FIRST_AFP,
                    MIN(RECENCY_AFP) RECENCY_AFP,
                    MAX(AFP_NET_INCOME) AFP_NET_INCOME,
                    MAX(AFP_LIABILITIES) AFP_LIABILITIES,
                    MAX(AFP_SAVINGS) AFP_SAVINGS,
                    MAX(AFP_INCOME_RATIO_SAVED) AFP_INCOME_RATIO_SAVED,
                    MAX(AFP_INCOME_RATIO_LIABILITIES) AFP_INCOME_RATIO_LIABILITIES,
                    MAX(AFP_MAIN_BANK) AFP_MAIN_BANK,
                    MAX(AFP_EMPLOYEE) AFP_EMPLOYEE,
                    MAX(AFP_PARENTALLEAVE) AFP_PARENTALLEAVE,
                    MAX(AFP_STUDENT) AFP_STUDENT,
                    MAX(AFP_PENSION) AFP_PENSION, 
                    MAX(AFP_ENTREPRENEUR) AFP_ENTREPRENEUR,
                    MAX(AFP_OTHERINCOME) AFP_OTHERINCOME, 
                    --MAX(NM_CHILDREN) NM_CHILDREN,
                    MAX(AFP_HOUSING_TYPE) AFP_HOUSING_TYPE,
                    MAX(AFP_INCOME_TYPE) AFP_INCOME_TYPE,
                    MAX(AFP_PRIMARY_BANK) AFP_PRIMARY_BANK,
                    MIN(RECENCY_REAL_ESTATE_AFP) RECENCY_REAL_ESTATE_AFP,
                    MAX(C_HOUSE) C_HOUSE,
                    MAX(C_APARTMENT) C_APARTMENT,
                    MAX(C_COTTAGE) C_COTTAGE,
                    MAX(C_BUILDING_LOT) C_BUILDING_LOT,
                    MAX(C_COMMERCIAL_SPACE) C_COMMERCIAL_SPACE,
                    MAX(C_NON_RESIDENTIAL_SPACE) C_NON_RESIDENTIAL_SPACE,
                    MAX(C_GARAGE) C_GARAGE,
                    MAX(REFUSE_ANSWER_REAL_ESTATE) REFUSE_ANSWER_REAL_ESTATE,
                    MAX(C_TOTAL_REAL_ESTATE) C_TOTAL_REAL_ESTATE,
                    MAX(CAT_REAL_ESTATE) CAT_REAL_ESTATE,
                    MAX(RECENCY_VEHICLE_AFP) RECENCY_VEHICLE_AFP,
                    MAX(C_OWN_CAR) C_OWN_CAR,
                    MAX(C_LOAN_CAR) C_LOAN_CAR,
                    MAX(C_WORK_CAR) C_WORK_CAR,
                    MAX(C_MOTORCYCLE) C_MOTORCYCLE,
                    MAX(NO_OWN_VEHICLE) NO_OWN_VEHICLE,
                    MAX(REFUSE_ANSWER_VEHICLE) REFUSE_ANSWER_VEHICLE,
                    MAX(C_TOTAL_VEHICLE) C_TOTAL_VEHICLE,
                    MAX(CAT_VEHICLE) CAT_VEHICLE
                FROM PM_OWNER.STG_AFP
                WHERE PERSON_FACT_START_DT = AFP_MOST_RECENT_DT
                GROUP BY CUSTOMERID
            ) AFP
                ON BASE.CUSTOMERID = AFP.CUSTOMERID
    
            LEFT JOIN (SELECT
                            CLB.CUSTOMERID,
                            COALESCE(RECENCY_WANT_CA_AFP, 0) RECENCY_WANT_CA_AFP,
                            COALESCE(RECENCY_WANT_STUDENTACC_AFP, 0) RECENCY_WANT_STUDENTACC_AFP,
                            COALESCE(RECENCY_WANT_CHILDACC_AFP, 0) RECENCY_WANT_CHILDACC_AFP,
                            COALESCE(RECENCY_WANT_SA_AFP, 0) RECENCY_WANT_SA_AFP,
                            COALESCE(RECENCY_WANT_MTG_AFP, 0) RECENCY_WANT_MTG_AFP,
                            COALESCE(RECENCY_WANT_CL_AFP, 0) RECENCY_WANT_CL_AFP,
                            COALESCE(RECENCY_WANT_CC_AFP, 0) RECENCY_WANT_CC_AFP,
                            COALESCE(RECENCY_WANT_OVD_AFP, 0) RECENCY_WANT_OVD_AFP,
                            COALESCE(RECENCY_WANT_FUND_AFP, 0) RECENCY_WANT_FUND_AFP,
                            COALESCE(RECENCY_WANT_TRAVELINS_AFP, 0) RECENCY_WANT_TRAVELINS_AFP,
                            COALESCE(RECENCY_WANT_LIFEINS_AFP, 0) RECENCY_WANT_LIFEINS_AFP,
                            COALESCE(RECENCY_WANT_HOMEINS_AFP, 0) RECENCY_WANT_HOMEINS_AFP,
                            COALESCE(RECENCY_WANT_MTPL_AFP, 0) RECENCY_WANT_MTPL_AFP,
                            COALESCE(RECENCY_WANT_CASCO_AFP, 0) RECENCY_WANT_CASCO_AFP,
                            COALESCE(RECENCY_WANT_PENSION_AFP, 0) RECENCY_WANT_PENSION_AFP,
                            
                            CASE WHEN COALESCE(RECENCY_WANT_CA_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CA_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_CA_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_STUDENTACC_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_STUDENTACC_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_STUDENTACC_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_CHILDACC_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CHILDACC_AFP, 0) < process_dt_hist -process_dt + 31 THEN -1
                                 ELSE 0 END AS I_WANT_CHILDACC_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_SA_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_SA_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_SA_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_MTG_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_MTG_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_MTG_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_CL_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CL_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_CL_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_CC_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CC_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_CC_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_OVD_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_OVD_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_OVD_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_FUND_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_FUND_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_FUND_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_TRAVELINS_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_TRAVELINS_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_TRAVELINS_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_LIFEINS_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_LIFEINS_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_LIFEINS_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_HOMEINS_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_HOMEINS_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_HOMEINS_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_MTPL_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_MTPL_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_MTPL_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_CASCO_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CASCO_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_CASCO_AFP_M,
                            CASE WHEN COALESCE(RECENCY_WANT_PENSION_AFP, 0) > (process_dt - process_dt_hist) - 31 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_PENSION_AFP, 0) < (process_dt_hist - process_dt) + 31 THEN -1
                                 ELSE 0 END AS I_WANT_PENSION_AFP_M,
                            
                            CASE WHEN COALESCE(RECENCY_WANT_CA_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CA_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_CA_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_STUDENTACC_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_STUDENTACC_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_STUDENTACC_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_CHILDACC_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CHILDACC_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_CHILDACC_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_SA_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_SA_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_SA_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_MTG_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_MTG_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_MTG_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_CL_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CL_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_CL_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_CC_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CC_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_CC_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_OVD_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_OVD_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_OVD_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_FUND_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_FUND_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_FUND_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_TRAVELINS_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_TRAVELINS_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_TRAVELINS_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_LIFEINS_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_LIFEINS_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_LIFEINS_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_HOMEINS_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_HOMEINS_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_HOMEINS_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_MTPL_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_MTPL_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_MTPL_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_CASCO_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_CASCO_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_CASCO_AFP_Q,
                            CASE WHEN COALESCE(RECENCY_WANT_PENSION_AFP, 0) > (process_dt - process_dt_hist) - 93 THEN 1
                                 WHEN COALESCE(RECENCY_WANT_PENSION_AFP, 0) < (process_dt_hist - process_dt) + 93 THEN -1
                                 ELSE 0 END AS I_WANT_PENSION_AFP_Q
                
                FROM (SELECT CUSTOMERID
                      FROM PM_OWNER.ATTR_CLIENT_BASE
                      WHERE MONTH_ID = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM')) ) CLB
                
                LEFT JOIN PM_OWNER.STG_PRODUCT_INTEREST_AFP SPI
                    ON SPI.CUSTOMERID = CLB.CUSTOMERID
    
            ) PINT
                ON BASE.CUSTOMERID = PINT.CUSTOMERID
    
            LEFT JOIN
            ( --- najvysie dosiahnuty titul
                SELECT 
                    CUSTOMERID,
                    CASE WHEN (TITLE LIKE '%MUDR%' OR TITLE LIKE '%MDDR%' OR TITLE LIKE '%MVDR%') THEN 'MUDr.'
                         WHEN TITLE LIKE '%JUDR%' THEN 'JUDr.'
                         WHEN (TITLE LIKE '%PHD%' OR TITLE LIKE '%DRSC%' OR TITLE LIKE '%CSC%') THEN 'PhD.'
                         WHEN (TITLE LIKE '%PHDR%' OR TITLE LIKE '%PHARMDR%' OR TITLE LIKE '%RNDR%' OR TITLE LIKE '%DR%' OR TITLE LIKE '%PAEDDR%') THEN 'PhDr.'
                         WHEN (TITLE LIKE '%MGR%' OR TITLE LIKE '%MGRART%' OR TITLE LIKE '%ING%' OR TITLE LIKE '%INGARCH%' OR TITLE LIKE '%MSC%' 
                               OR TITLE LIKE '%MA%' OR TITLE LIKE '%LLM%' OR TITLE LIKE '%MBA%' OR TITLE LIKE '%MGA%') THEN 'Mgr.'
                         WHEN (TITLE LIKE '%DOC%' OR TITLE LIKE '%PROF%' OR TITLE LIKE '%DIS%') THEN 'Academic'
                         WHEN (TITLE LIKE '%BC%' OR TITLE LIKE '%BA%' OR TITLE LIKE '%BSC%') THEN 'Bc.'
                         WHEN TITLE <> 'NONENONE' THEN 'OTHER'
                         ELSE 'NONE' END AS TITLE
                FROM
                (
                    SELECT
                        CLB.CUSTOMERID,
                        --- uppercase of title before name + title after name in one string with removed whitespaces and punctuation
                        UPPER(REPLACE(REPLACE(CONCAT(COALESCE(MAX(PIN.party_indiv_nm_tl_tx), 'NONE'),
                                                     COALESCE(MAX(CASE WHEN PIN.party_indiv_nm_suffix_tx NOT IN ('ml.', 'st.') THEN PIN.party_indiv_nm_suffix_tx ELSE NULL END), 'NONE')),' ',''),'.','')) AS TITLE
    
                    FROM L0_OWNER.PARTY P
    
                    JOIN L0_OWNER.PARTY_ROLE_STAT PRS
                        ON  P.party_id = PRS.party_id
                        AND PRS.party_role_cd IN (13, 28) --Party CMD
                        AND PRS.party_role_stat_start_dt <= process_dt
                        AND PRS.party_role_stat_end_dt > process_dt
    
                    JOIN L0_OWNER.PARTY_RLTD PR
                        ON  PR.child_party_id = P.party_id
                        AND PR.party_rltd_rsn_cd = 4
                        AND PR.party_rltd_start_dt <= process_dt
                        AND PR.party_rltd_end_dt > process_dt
                    
                    JOIN (SELECT
                            CUSTOMERID 
                          FROM PM_OWNER.ATTR_CLIENT_BASE
                          WHERE MONTH_ID = TO_NUMBER(TO_CHAR(process_dt, 'YYYYMM'))
                          ) CLB
                        ON CLB.CUSTOMERID = PR.party_id
                    
                    LEFT JOIN L0_OWNER.PARTY_INDIV_NM PIN
                        ON  PIN.party_id = P.party_id
                        AND PIN.nm_type_cd = 1
                        AND PIN.party_indiv_nm_start_dt <= process_dt
                        AND PIN.party_indiv_nm_end_dt > process_dt
                    
                    WHERE
                        P.src_syst_cd = 110 
                        AND P.party_subtp_cd IN (0)
                        AND P.party_src_key like 'PMC%'
                    
                    GROUP BY
                        CLB.CUSTOMERID
                )
            ) TTL
                ON BASE.CUSTOMERID = TTL.CUSTOMERID
    
            LEFT JOIN PM_OWNER.STG_EXTRA_INFO SEI
                ON BASE.CUSTOMERID = SEI.CUSTOMERID
    
        );
    
    stat_ins(p_count => SQL%ROWCOUNT);
    commit;              
    mng_stats(l_owner_name, l_table_name, l_proc_name);
    
    END;
  
                    
  PROCEDURE on_process(p_audit_id IN NUMBER, p_audit_date IN DATE, p_repair IN BOOLEAN) IS

  process_dt date := p_audit_date;

  BEGIN
  
		-- set audit_date
		if last_day(p_audit_date) <> p_audit_date then
			process_dt := last_day(add_months(least(sysdate, p_audit_date),-1));
		else
			process_dt := least(process_dt, last_day(add_months(sysdate,-1)));
		end if;
  
        step(p_stepname => 'START', p_source => 'N/A', p_target => 'N/A');

        -----   GLOBAL VARIABLE   -----
        step(p_stepname => 'init_global_par', p_source => 'N/A', p_target => 'N/A');
        init_global_par(process_dt); 
		
        -----   PROCESSING   ----- 
        step(p_stepname => 'append_ATTR_CLIENT_BASE', p_source => 'N/A', p_target => 'N/A');   
        append_ATTR_CLIENT_BASE(p_audit_id, process_dt, p_repair);
        
        step(p_stepname => 'append_CARD', p_source => 'N/A', p_target => 'N/A');   
        append_CARD(p_audit_id, process_dt, p_repair);
        
        step(p_stepname => 'append_CARD_EXPIRED', p_source => 'N/A', p_target => 'N/A');   
        append_CARD_EXPIRED(p_audit_id, process_dt, p_repair);

        step(p_stepname => 'append_attr_card', p_source => 'N/A', p_target => 'N/A');   
        append_attr_card(p_audit_id, process_dt, p_repair);
        
        step(p_stepname => 'append_bank_account_detail', p_source => 'N/A', p_target => 'N/A');   
        append_bank_account_detail(p_audit_id, process_dt, p_repair);

        step(p_stepname => 'append_bank_account_closed', p_source => 'N/A', p_target => 'N/A');   
        append_bank_account_closed(p_audit_id, process_dt, p_repair);

        step(p_stepname => 'append_attr_bank_account', p_source => 'N/A', p_target => 'N/A');   
        append_attr_bank_account(p_audit_id, process_dt, p_repair);

        step(p_stepname => 'append_attr_txn', p_source => 'N/A', p_target => 'N/A');   
        append_attr_txn(p_audit_id, process_dt, p_repair);

        step(p_stepname => 'append_terminal', p_source => 'N/A', p_target => 'N/A');   
        append_terminal(p_audit_id, ADD_MONTHS(process_dt, -1), p_repair);
        
        step(p_stepname => 'append_terminal_txn_type_list', p_source => 'N/A', p_target => 'N/A');   
        append_terminal_txn_type_list(p_audit_id, ADD_MONTHS(process_dt, -1), p_repair);

        step(p_stepname => 'append_attr_terminal', p_source => 'N/A', p_target => 'N/A');   
        append_attr_terminal(p_audit_id, ADD_MONTHS(process_dt, -1), p_repair);

        step(p_stepname => 'append_attr_standing_order', p_source => 'N/A', p_target => 'N/A');   
        append_attr_standing_order(p_audit_id, process_dt, p_repair);

        step(p_stepname => 'append_attr_elb', p_source => 'N/A', p_target => 'N/A');   
        append_attr_elb(p_audit_id, process_dt, p_repair);

        step(p_stepname => 'append_attr_fund', p_source => 'N/A', p_target => 'N/A');   
        append_attr_fund(p_audit_id, process_dt, p_repair);
          
        step(p_stepname => 'append_attr_insurance', p_source => 'N/A', p_target => 'N/A');   
        append_attr_insurance(p_audit_id, process_dt, p_repair);
        
        step(p_stepname => 'append_ATTR_INTERACTION', p_source => 'N/A', p_target => 'N/A');
        append_attr_interaction(p_audit_id, process_dt, p_repair);
        
        step(p_stepname => 'append_attr_afp', p_source => 'N/A', p_target => 'N/A');   
        append_attr_afp(p_audit_id, process_dt, p_repair);
                          
  END;

  FUNCTION version RETURN VARCHAR2 IS BEGIN RETURN cVERSION; END;

  procedure main(p_audit_id in number, p_audit_date in date, p_repair in boolean) is
  begin
    on_process(p_audit_id, p_audit_date, p_repair);    
  end;   
    
  
  BEGIN  
    init_all;  
END ETL_ATTR_COLLECTION;
/