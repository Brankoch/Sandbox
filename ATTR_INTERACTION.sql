--------------------------------------------------------------------------------
--1) mapping pre DH_S_CCD_PARTY_CONTACT
SELECT 
  --ccdb.customerid       --> toto si musite pripravit sami, je tam pouzita DMSK tabulka
  pa.cd_record          AS cd_rec_party
  ,'CRM'                AS source_system
  , CASE WHEN pty_ext_ot.cd_record = 'OBJECT~PARTY_FACT~CONTACT~EMAIL'
           THEN 'E_MAIL'
         WHEN pty_ext_ot.cd_record = 'OBJECT~PARTY_FACT~CONTACT~PHONE'
           THEN 'PHONE'
         WHEN pty_ext_ot.cd_record = 'OBJECT~PARTY_FACT~CONTACT~WEB_ADDRESS'
           THEN 'WEB_ADDRESS'
    END                 AS cd_contact_type
  , pty_ext.t_value_1   AS t_contact_value
  --, PARTY_CONTACT_START_DT as CONTACT_START_DT      --> toto si musite pripravit sami, nepozname logiku plnenia CONTACT_START_DT v DMSK
  --, PARTY_CONTACT_end_DT as CONTACT_end_DT          --> toto si musite pripravit sami, nepozname logiku plnenia CONTACT_end_DT v DMSK
FROM od.od_o_party pa
  INNER JOIN od.od_l_source pa_src
  ON pa.id_source = pa_src.id_source
    AND pa_src.cd_record = 'CCDB.UNF'
  INNER JOIN od.od_l_object_type pa_ot
    ON pa.id_object_type = pa_ot.id_object_type
      AND pa_ot.cd_record = 'OBJECT~PARTY'
  INNER JOIN od.od_l_source pa_ot_src
    ON pa_ot.id_source = pa_ot_src.id_source
      AND pa_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_o_party pa_crm
    ON pa_crm.cd_record = pa.cd_record
      AND pa_crm.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source pa_crm_src
    ON pa_crm.id_source = pa_crm_src.id_source
      AND pa_crm_src.cd_record = 'CCDB.CRM'
  INNER JOIN od.od_l_object_type pa_crm_ot
    ON pa_crm.id_object_type = pa_crm_ot.id_object_type
      AND pa_crm_ot.cd_record = 'OBJECT~PARTY'
  INNER JOIN od.od_l_source pa_crm_ot_src
    ON pa_crm_ot.id_source = pa_crm_ot_src.id_source
      AND pa_crm_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_f_party_extension pty_ext
    ON pty_ext.id_party = pa.id_party
      AND pty_ext.id_source = pa.id_source
      AND pty_ext.nm_order = 1 -- nm_order is an incremental value coming from T24
      AND pty_ext.dt_bus_eff_to = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF')
      AND pty_ext.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_object_type pty_ext_ot
    ON pty_ext.id_object_type = pty_ext_ot.id_object_type
      AND pty_ext_ot.cd_record IN (
                                    'OBJECT~PARTY_FACT~CONTACT~EMAIL'
                                    , 'OBJECT~PARTY_FACT~CONTACT~PHONE'
                                    , 'OBJECT~PARTY_FACT~CONTACT~WEB_ADDRESS'
                                  )
  INNER JOIN od.od_l_source pty_ext_ot_src
    ON pty_ext_ot.id_source = pty_ext_ot_src.id_source
      AND pty_ext_ot_src.cd_record = 'CCDB'
WHERE 1=1
  AND pa.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
;

--------------------------------------------------------------------------------
--2) mapping pre OH_W_PARTY_PARTY - vyskytuje s LEFT JOINoch viackrat v scripte
SELECT 
  pp_p1.cd_record         AS ccdb_id
  --, TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) as CUSTOMERID        --> toto si musite pripravit sami, je tam pouzita DMSK tabulka
  --, ROW_NUMBER() OVER (PARTITION BY TO_NUMBER(CASE WHEN pp.cd_rec_party_2_source='1' THEN P.party_id  ELSE NULL END) ORDER BY dt_bus_eff_from desc, pp.cd_rec_party_1) row_num    --> toto si musite pripravit sami, je tam pouzita DMSK tabulka
FROM od.od_r_party_party pp
  INNER JOIN od.od_l_source pp_src
    ON pp.id_source = pp_src.id_source
      AND pp_src.cd_record = 'CCDB'
  INNER JOIN od.od_l_relation_type pp_typ
    ON pp.id_relation_type = pp_typ.id_relation_type
      AND pp_typ.cd_record = 'PARTY_INSTANCE'
  INNER JOIN od.od_l_source pp_typ_src
    ON pp_typ.id_source = pp_typ_src.id_source
      AND pp_typ_src.cd_record = 'CCDB'
  INNER JOIN od.od_o_party pp_p1
    ON pp.id_party_1 = pp_p1.id_party
      AND pp_p1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source pp_p1_src
    ON pp_p1.id_source = pp_p1_src.id_source
      AND pp_p1_src.cd_record = 'CCDB.CRM'    
  LEFT JOIN od.od_o_party pp_p2
    ON pp.id_party_2 = pp_p2.id_party
      AND pp_p2.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  LEFT JOIN od.od_l_source pp_p2_src
    ON pp_p2.id_source = pp_p2_src.id_source
      AND pp_p2_src.cd_record = '1'   
WHERE 1=1
  AND TRIM(TRANSLATE(pp_p1.cd_record, '0123456789-,.', ' ')) IS NULL
  --zvysne podmienky si musite urobit sami, je tam pouzita DMSK tabulka
  AND pp.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
;

--------------------------------------------------------------------------------
--3) mapping pre "BRANCH VISIT"
SELECT
  --CCDB.CUSTOMERID                             --> dotiahnute z LEFT JOINoch na PARTY_PARTY, vid. vyssie vid.
  CAST(evt.dt_bus_eff_from AS DATE) AS event_dt
  , CASE vl_prp2.cd_record
      WHEN '1' THEN 5 -- veľmi spokojný 
      WHEN '2' THEN 4 
      WHEN '3' THEN 3 
      WHEN '4' THEN 2 
      WHEN '5' THEN 1 -- veľmi nespokojný 
      ELSE NULL
  END                               AS feedback_evalution
  , vl_prp2.cd_record               AS feed_orig
FROM od.od_e_event evt
  INNER JOIN od.od_l_source evt_src
    ON evt.id_source = evt_src.id_source
      AND evt_src.cd_record = 'CCDB.CRM'
  INNER JOIN od.od_l_object_type evt_ot
    ON evt.id_object_type = evt_ot.id_object_type
      AND evt_ot.cd_record = 'OBJECT~EVENT~SESSION'
  INNER JOIN od.od_l_source evt_ot_src
    ON evt_ot.id_source = evt_ot_src.id_source
      AND evt_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_o_party evt_p1
    ON evt.id_party_1 = evt_p1.id_party
      AND evt_p1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source evt_p1_src
    ON evt_p1.id_source = evt_p1_src.id_source
      AND evt_p1_src.cd_record = 'CCDB.CRM'    
  INNER JOIN od.od_l_value vl_prp2
    ON evt.id_value_property_2 = vl_prp2.id_value
      AND vl_prp2.cd_record <> 'XNA'
WHERE 1=1
  AND evt.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
;

--------------------------------------------------------------------------------
--4) mapping pre SERVICE_REQUEST (staznosti)
SELECT
  srf.t_name
  --cust.party_id AS CUSTOMERID       --> odporucam dotiahnut z rovnakeho LEFT JOINu ako v pripade "BRANCH VISIT"
  , srf.dt_tec_updated_master       
  --, SRF.SERVICE_REQUEST_FACT_DT       --> toto si musite pripravit sami, nepozname logiku plnenia SERVICE_REQUEST_FACT_DT v DMSK
  , evt_srq_p1.cd_record              AS cd_rec_party
FROM od.od_e_event evt_srq
  INNER JOIN od.od_l_source evt_srq_src
    ON evt_srq.id_source = evt_srq_src.id_source
      AND evt_srq_src.cd_record = 'CCDB.CRM'
  INNER JOIN od.od_l_object_type evt_srq_ot
    ON evt_srq.id_object_type = evt_srq_ot.id_object_type
      AND evt_srq_ot.cd_record = 'OBJECT~EVENT~SERVICE~REQUEST'
  INNER JOIN od.od_l_source evt_srq_ot_src
    ON evt_srq_ot.id_source = evt_srq_ot_src.id_source
      AND evt_srq_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_l_value vl_prp2
    ON evt_srq.id_value_property_2 = vl_prp2.id_value
      AND vl_prp2.cd_record NOT IN ('1183', '1182')
  INNER JOIN od.od_e_event evt_srq_exe
    ON evt_srq.cd_record = evt_srq_exe.cd_record
      AND evt_srq_exe.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  INNER JOIN od.od_l_source evt_srq_exe_src
    ON evt_srq_exe.id_source = evt_srq_exe_src.id_source
      AND evt_srq_exe_src.cd_record = 'CCDB.CRM'
  INNER JOIN od.od_l_object_type evt_srq_exe_ot
    ON evt_srq_exe.id_object_type = evt_srq_exe_ot.id_object_type
      AND evt_srq_exe_ot.cd_record = 'OBJECT~EVENT~SERVICE~REQUEST_EXECUTION'
  INNER JOIN od.od_l_source evt_srq_exe_ot_src
    ON evt_srq_exe_ot.id_source = evt_srq_exe_ot_src.id_source
      AND evt_srq_exe_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_f_service_request_fact srf
    ON evt_srq_exe.id_event = srf.id_event
  INNER JOIN od.od_l_source srf_src
    ON srf.id_source = srf_src.id_source
      AND srf_src.cd_record = 'CCDB.CRM'
  INNER JOIN od.od_o_party evt_srq_p1
    ON evt_srq.id_party_1 = evt_srq_p1.id_party
      AND evt_srq_p1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source evt_srq_p1_src
    ON evt_srq_p1.id_source = evt_srq_p1_src.id_source
      AND evt_srq_p1_src.cd_record = 'CCDB.CRM'    
WHERE 1=1
  AND evt_srq.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  AND srf.t_name NOT IN (
                            'Vystavenie bankovej informácie pre účely auditu',
                            'BB majiteľ inštalácie - nastavenie',
                            'BB majiteľ účtu - nastavenie BB',
                            'IPPID - priradenie Tokenu DP770 pre službu BB Lite',
                            'Pridanie účtu do ELB'
                        )
  AND substr(srf.t_name, 3,1) != '-'
;

--------------------------------------------------------------------------------
--5) mapping pre CRM SESSION
SELECT
  --CCDB.CUSTOMERID                             --> dotiahnute z LEFT JOINoch na PARTY_PARTY, vid. vyssie vid.
  CAST(evt.dt_bus_eff_from AS DATE) AS event_dt
  ,CASE WHEN vl_prp1.cd_record = '100' THEN 1 ELSE 0 END category_sale
  , CASE WHEN vl_prp1.cd_record = '200' THEN 1 ELSE 0 END category_care
  , CASE WHEN vl_prp1.cd_record = '300' THEN 1 ELSE 0 END category_information
  , CASE WHEN vl_prp1.cd_record = '400' THEN 1 ELSE 0 END category_cash
  , CASE WHEN vl_prp1.cd_record = '500' THEN 1 ELSE 0 END category_other
FROM od.od_e_event evt
  INNER JOIN od.od_l_source evt_src
    ON evt.id_source = evt_src.id_source
      AND evt_src.cd_record = 'CCDB.CRM'
  INNER JOIN od.od_l_object_type evt_ot
    ON evt.id_object_type = evt_ot.id_object_type
      AND evt_ot.cd_record = 'OBJECT~EVENT~SESSION'
  INNER JOIN od.od_l_source evt_ot_src
    ON evt_ot.id_source = evt_ot_src.id_source
      AND evt_ot_src.cd_record = 'CCDB'
  INNER JOIN od.od_o_party evt_p1
    ON evt.id_party_1 = evt_p1.id_party
      AND evt_p1.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
      --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
  INNER JOIN od.od_l_source evt_p1_src
    ON evt_p1.id_source = evt_p1_src.id_source
      AND evt_p1_src.cd_record = 'CCDB.CRM'    
  INNER JOIN od.od_l_value vl_prp1
    ON evt.id_value_property_1 = vl_prp1.id_value
  INNER JOIN od.od_l_value vl_prp2
    ON evt.id_value_property_2 = vl_prp2.id_value
      AND vl_prp2.cd_record <> 'XNA'
WHERE 1=1
  AND evt.dt_tec_deleted = TO_TIMESTAMP('99991231235959999999', 'YYYYMMDDHH24MISSFF6')
  --filtracia na START_DT a END_DT (DMSK atributy) nie je zahrnuta - potrebne urobit analogicky ako v inych SELECToch z CCDB dat pre EDN
;