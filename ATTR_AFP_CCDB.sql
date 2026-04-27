--------------------------------------------------------------------------------
--1) mapping pre PERSON_FACT
SELECT 
  --PROCESS_DT AS INSERT_DT   --> toto si musite pripravit sami, nie je z CCDB tabulky
  --, CLB.CUSTOMERID          --> toto si musite pripravit sami, nie je z CCDB tabulky
  pef_p.cd_record             AS cd_rec_party
  --, MAX(PEF.PERSON_FACT_START_DT) OVER (PARTITION BY CLB.CUSTOMERID) AFP_MOST_RECENT_DT                              --> toto si musite pripravit sami, nepozname logiku plnenia PERSON_FACT_START_DT v DMSK
  --, PEF.PERSON_FACT_START_DT                                                                                         --> toto si musite pripravit sami, nepozname logiku plnenia PERSON_FACT_START_DT v DMSK
  --, ROUND(PROCESS_DT - PEF.PERSON_FACT_START_DT) AS RECENCY_AFP                                                      --> toto si musite pripravit sami, nepozname logiku plnenia PERSON_FACT_START_DT v DMSK
  --, ROUND(PROCESS_DT - MIN(PEF.PERSON_FACT_START_DT) OVER (PARTITION BY CLB.CUSTOMERID)) AS DAYS_SINCE_FIRST_AFP     --> toto si musite pripravit sami, nepozname logiku plnenia PERSON_FACT_START_DT v DMSK
  , COALESCE(pef.vl_net_income, 0) + COALESCE(pef.vl_other_income, 0)                       AS afp_net_income
  , COALESCE(pef.vl_liabilities, 0)                                                         AS afp_liabilities
  , COALESCE(pef.vl_savings, 0)                                                             AS afp_savings

  , CASE WHEN (COALESCE(pef.vl_net_income, 0) + COALESCE(pef.vl_other_income, 0)) > 0 
       THEN ROUND(COALESCE(pef.vl_savings, 0) / (COALESCE(pef.vl_net_income, 0) + COALESCE(pef.vl_other_income, 0)), 4) 
       ELSE 0 END                                                                           AS afp_income_ratio_saved
  , CASE WHEN (COALESCE(pef.vl_net_income, 0) + COALESCE(pef.vl_other_income, 0)) > 0
       THEN ROUND(COALESCE(pef.vl_liabilities, 0) / (COALESCE(pef.vl_net_income, 0) + COALESCE(pef.vl_other_income, 0)), 4)
       ELSE 0 END                                                                           AS afp_income_ratio_liabilities

  , vl_hous_typ.cd_record                                         AS cd_rec_value_housing_type
  , vl_hous_typ.cd_record                                         AS cd_rec_value_income_type
  , vl_main_bank.cd_record                                         AS cd_rec_value_main_bank

  , CASE WHEN vl_main_bank.cd_record = 'PB_01' THEN 1 ELSE 0 END  AS afp_main_bank
  , CASE WHEN vl_inc_typ.cd_record = 'ET_01' THEN 1 ELSE 0 END    AS afp_employee                                   
  , CASE WHEN vl_inc_typ.cd_record = 'ET_07' THEN 1 ELSE 0 END    AS afp_parentalleave
  , CASE WHEN vl_inc_typ.cd_record = 'ET_04' THEN 1 ELSE 0 END    AS afp_student
  , CASE WHEN vl_inc_typ.cd_record = 'ET_05' THEN 1 ELSE 0 END    AS afp_pension
  , CASE WHEN vl_inc_typ.cd_record = 'ET_02' THEN 1 ELSE 0 END    AS afp_entrepreneur
  , CASE WHEN vl_inc_typ.cd_record NOT IN ('ET_01', 'ET_02', 'ET_04', 'ET_05', 'ET_07') 
       THEN 1 ELSE 0 END                                          AS afp_otherincome

  , COALESCE(nm_children, 0)                                      AS nm_children
  , vl_marit_status.cd_record                                     AS cd_rec_value_marital_status

  --CASE na AFP_HOUSING_TYPE  --> vid. mapping atributu CD_REC_VALUE_HOUSING_TYPE (vl_hous_typ.CD_RECORD)
  --CASE na AFP_INCOME_TYPE   --> vid. mapping atributu CD_REC_VALUE_INCOME_TYPE (vl_inc_typ.CD_RECORD)
  --CASE na AFP_PRIMARY_BANK  --> vid. mapping atributu CD_REC_VALUE_MAIN_BANK (vl_main_bank.CD_RECORD)

  --ostatne atributy          --> vid. LEFT JOINy na REAL_ESTATE a VEHICLE nizsie
FROM od.od_f_person_fact pef
  INNER JOIN od.od_l_source pef_src
    ON pef.id_source = pef_src.id_source
      AND pef_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
  INNER JOIN od.od_o_party pef_p
    ON pef.id_party = pef_p.id_party
      AND pef_p.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source pef_p_src
    ON pef_p.id_source = pef_p_src.id_source
      AND pef_p_src.cd_record = '110'   
  INNER JOIN od.od_l_object_type pef_p_ot
    ON pef_p.id_object_type = pef_p_ot.id_object_type
      AND pef_p_ot.cd_record = 'OBJECT~PARTY'
  INNER JOIN od.od_l_source pef_p_ot_src
    ON pef_p_ot.id_source = pef_p_ot_src.id_source
      AND pef_p_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_r_property_value pry_val
    ON pef.id_value_income_type = pry_val.id_value
      AND pry_val.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source pry_val_src
    ON pry_val.id_source = pry_val_src.id_source
      AND pry_val_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
  INNER JOIN od.od_l_property prp_inc_typ
    ON pry_val.id_property = prp_inc_typ.id_property
      AND prp_inc_typ.cd_record = 'INCOME_TYPE'
  INNER JOIN od.od_l_source prp_inc_typ_src
    ON prp_inc_typ.id_source = prp_inc_typ_src.id_source
      AND prp_inc_typ_src.cd_record = 'CCDB'
  INNER JOIN od.od_l_value vl_inc_typ
    ON pry_val.id_value = vl_inc_typ.id_value
  INNER JOIN od.od_l_value vl_hous_typ
    ON pef.id_value_housing_type = vl_hous_typ.id_value
  INNER JOIN od.od_l_value vl_main_bank
    ON pef.id_value_main_bank = vl_main_bank.id_value
  INNER JOIN od.od_l_value vl_marit_status
    ON pef.id_value_marital_status = vl_marit_status.id_value
WHERE 1=1
  --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
;

--------------------------------------------------------------------------------
--2) mapping LEFT JOINu pre REAL_ESTATE
SELECT 
  res_typ.cd_record     AS cd_rec_value_real_estate_type
  --RE_MOST_RECENT_DT   --> toto si musite pripravit sami, nepozname logiku plnenia REAL_ESTATE_FACT_START_DT v DMSK
  , evt.cuid            AS cuid
  --START_DT            --> toto si musite pripravit sami, nepozname logiku plnenia REAL_ESTATE_FACT_START_DT v DMSK
  --END_DT            --> toto si musite pripravit sami, nepozname logiku plnenia REAL_ESTATE_FACT_END_DT v DMSK
FROM od.od_f_real_estate_fact res
  INNER JOIN od.od_l_source res_src
    ON res.id_source = res_src.id_source
      AND res_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
  INNER JOIN od.od_o_thing res_th
    ON res.id_thing = res_th.id_thing
      AND res_th.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  INNER JOIN od.od_l_source res_th_src
    ON res_th.id_source = res_th_src.id_source
      AND res_th_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
  INNER JOIN od.od_l_object_type res_th_ot
    ON res_th.id_object_type = res_th_ot.id_object_type
      AND res_th_ot.cd_record = 'OBJECT~THING~REAL_ESTATE'
  INNER JOIN od.od_l_source res_th_ot_src
    ON res_th_ot.id_source = res_th_ot_src.id_source
      AND res_th_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_l_value res_typ
    ON res.id_value_real_estate_type = res_typ.id_value
  INNER JOIN (
    SELECT
      evt_th1.cd_record                       AS cd_rec_thing_1,
      MAX(substr (evt_p1.cd_record, 5, 10))   AS cuid
    FROM od.od_e_event evt
      INNER JOIN od.od_l_source evt_src
        ON evt.id_source = evt_src.id_source
          AND evt_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
      INNER JOIN od.od_o_thing evt_th1
        ON evt.id_thing_1 = evt_th1.id_thing
          AND evt_th1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      INNER JOIN od.od_l_source evt_th1_src
        ON evt_th1.id_source = evt_th1_src.id_source
          AND evt_th1_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
      INNER JOIN od.od_l_object_type evt_th1_ot
        ON evt_th1.id_object_type = evt_th1_ot.id_object_type
          AND evt_th1_ot.cd_record = 'OBJECT~THING~REAL_ESTATE'
      INNER JOIN od.od_l_source evt_th1_ot_src
        ON evt_th1_ot.id_source = evt_th1_ot_src.id_source
          AND evt_th1_ot_src.cd_record = 'CCDB'
      INNER JOIN od.od_o_party evt_p1
        ON evt.id_party_1 = evt_p1.id_party
          AND evt_p1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
          --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
      INNER JOIN od.od_l_source evt_p1_src
        ON evt_p1.id_source = evt_p1_src.id_source
          AND evt_p1_src.cd_record = '110'    
    WHERE 1=1
      AND evt.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
    GROUP BY
      evt_th1.cd_record
  ) evt ON res_th.cd_record = evt.cd_rec_thing_1
WHERE 1=1
  AND res.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
;

--------------------------------------------------------------------------------
--3) mapping LEFT JOINu pre VEHICLE
SELECT 
  veh_own.cd_record     AS cd_rec_value_vehicle_ownership
  --VEH_MOST_RECENT_DT   --> toto si musite pripravit sami, nepozname logiku plnenia VEHICLE_FACT_START_DT v DMSK
  , evt.cuid            AS cuid
  --START_DT            --> toto si musite pripravit sami, nepozname logiku plnenia VEHICLE_FACT_START_DT v DMSK
  --END_DT            --> toto si musite pripravit sami, nepozname logiku plnenia VEHICLE_FACT_END_DT v DMSK
FROM od.od_f_vehicle_fact veh
  INNER JOIN od.od_l_source veh_src
    ON veh.id_source = veh_src.id_source
      AND veh_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
  INNER JOIN od.od_o_thing veh_th
    ON veh.id_thing = veh_th.id_thing
      AND veh_th.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source veh_th_src
    ON veh_th.id_source = veh_th_src.id_source
      AND veh_th_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
  INNER JOIN od.od_l_object_type veh_th_ot
    ON veh_th.id_object_type = veh_th_ot.id_object_type
      AND veh_th_ot.cd_record = 'OBJECT~THING~VEHICLE'
  INNER JOIN od.od_l_source veh_th_ot_src
    ON veh_th_ot.id_source = veh_th_ot_src.id_source
      AND veh_th_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_l_value veh_own
    ON veh.id_value_vehicle_ownership = veh_own.id_value
  INNER JOIN (
    SELECT
      evt_th1.cd_record                       AS cd_rec_thing_1,
      MAX(substr (evt_p1.cd_record, 5, 10))   AS cuid
    FROM od.od_e_event evt
      INNER JOIN od.od_l_source evt_src
        ON evt.id_source = evt_src.id_source
          AND evt_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
      INNER JOIN od.od_o_thing evt_th1
        ON evt.id_thing_1 = evt_th1.id_thing
          AND evt_th1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
          --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
      INNER JOIN od.od_l_source evt_th1_src
        ON evt_th1.id_source = evt_th1_src.id_source
          AND evt_th1_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
      INNER JOIN od.od_l_object_type evt_th1_ot
        ON evt_th1.id_object_type = evt_th1_ot.id_object_type
          AND evt_th1_ot.cd_record = 'OBJECT~THING~VEHICLE'
      INNER JOIN od.od_l_source evt_th1_ot_src
        ON evt_th1_ot.id_source = evt_th1_ot_src.id_source
          AND evt_th1_ot_src.cd_record = 'CCDB'
      INNER JOIN od.od_o_party evt_p1
        ON evt.id_party_1 = evt_p1.id_party
          AND evt_p1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
          --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
      INNER JOIN od.od_l_source evt_p1_src
        ON evt_p1.id_source = evt_p1_src.id_source
          AND evt_p1_src.cd_record = '110'    
    WHERE 1=1
      AND evt.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
    GROUP BY
      evt_th1.cd_record
  ) evt ON veh_th.cd_record = evt.cd_rec_thing_1
WHERE 1=1
  AND veh.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
;

--------------------------------------------------------------------------------
--4) mapping pre filter opportunit
SELECT
  --clb.customerid        --> toto si musite pripravit sami, nie je z CCDB tabulky
  SUBSTR (evt_p1.cd_record, 5, 10) AS CUID
  --, MAX(TRUNC(a.dt_bus_eff_from)) OVER (PARTITION BY clb.customerid, A.cd_rec_value_purpose) AS most_recent_afp_prod_int_dt --> toto si musite pripravit sami, atribut CLB.CUSTOMERID nie je z CCDB tabulky
  , TRUNC(evt.dt_bus_eff_from) AS event_dt
  , evt.cd_variable AS interest_shown
  , vl_prp.cd_record AS product
FROM od.od_e_event evt
  INNER JOIN od.od_l_source evt_src
    ON evt.id_source = evt_src.id_source
      AND evt_src.cd_record = 'CUSTOMER_DIAGNOSTICS'
  INNER JOIN od.od_l_object_type evt_ot
    ON evt.id_object_type = evt_ot.id_object_type
      AND evt_ot.cd_record = 'OBJECT~EVENT~OPPORTUNITY'
  INNER JOIN od.od_l_source evt_ot_src
    ON evt_ot.id_source = evt_ot_src.id_source
      AND evt_ot_src.cd_record = 'CCDB' 
  INNER JOIN od.od_o_party evt_p1
    ON evt.id_party_1 = evt_p1.id_party
      AND evt_p1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source evt_p1_src
    ON evt_p1.id_source = evt_p1_src.id_source
      AND evt_p1_src.cd_record = '110'    
  INNER JOIN od.od_l_value vl_prp
    ON evt.id_value_purpose = vl_prp.id_value
WHERE 1=1
  AND evt.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  AND evt.cd_variable IN ('INTERESTED', 'LATER', 'NOT_INTERESTED')
;