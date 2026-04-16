from pyspark.sql import functions as F


def transform(sources_df, modelling_date, spark):
    """
    transform
    :param sources_df\
    :param modelling_date\
    :param spark\
    :return feature__attr_insurance
    """
    dmsk__l0_owner__card_svc = sources_df['master']['dmsk__l0_owner__card_svc']
    dmsk__l0_owner__cont = sources_df['master']['dmsk__l0_owner__cont']
    dmsk__l0_owner__cont_dt_actl = sources_df['master']['dmsk__l0_owner__cont_dt_actl']
    dmsk__l0_owner__cont_dt_hist = sources_df['master']['dmsk__l0_owner__cont_dt_hist']
    dmsk__l0_owner__cont_gen = sources_df['master']['dmsk__l0_owner__cont_gen']
    dmsk__l0_owner__cont_prod = sources_df['master']['dmsk__l0_owner__cont_prod']
    dmsk__l0_owner__cont_rltd = sources_df['master']['dmsk__l0_owner__cont_rltd']
    dmsk__l0_owner__cont_stat = sources_df['master']['dmsk__l0_owner__cont_stat']
    dmsk__l0_owner__cont_stat_type_gen = sources_df['master']['dmsk__l0_owner__cont_stat_type_gen']
    dmsk__l0_owner__cont_val = sources_df['master']['dmsk__l0_owner__cont_val']
    dmsk__l0_owner__party = sources_df['master']['dmsk__l0_owner__party']
    dmsk__l0_owner__party_indiv = sources_df['master']['dmsk__l0_owner__party_indiv']
    dmsk__l0_owner__party_cont = sources_df['master']['dmsk__l0_owner__party_cont']
    dmsk__l0_owner__party_rltd = sources_df['master']['dmsk__l0_owner__party_rltd']
    dmsk__l0_owner__prod = sources_df['master']['dmsk__l0_owner__prod']
    dmsk__l0_owner__prod_hier = sources_df['master']['dmsk__l0_owner__prod_hier']
    dmsk__l0_owner__prod_nm = sources_df['master']['dmsk__l0_owner__prod_nm']

    dmsk__je69050__administrative_structure_svk = sources_df['feature']['dmsk__je69050__administrative_structure_svk']
    dmsk__je69050__area_code_gen = sources_df['feature']['dmsk__je69050__area_code_gen']
    feature__card = sources_df['feature']['card']
    feature__attr_client_base = sources_df['feature']['attr_client_base']
    feature__attr_insurance = sources_df['feature']['attr_insurance']
    feature__product_level_category = sources_df['feature']['product_level_category']

    mis__ins__cakaren_storien_detail_dat = sources_df['clean']['mis__ins__cakaren_storien_detail_dat']
    mis__ins__pzp_storna_dat = sources_df['clean']['mis__ins__pzp_storna_dat']
    mis__ins__prieskum_spokojnosti_dat = sources_df['clean']['mis__ins__prieskum_spokojnosti_dat']
    mis__ins__banka_upomienky_dat = sources_df['clean']['mis__ins__banka_upomienky_dat']
    mis__ins__poistovna_zmluvy_dat = sources_df['clean']['mis__ins__poistovna_zmluvy_dat']
    mis__ins__pz_detail_cp_detail = sources_df['clean']['mis__ins__pz_detail_cp_detail']
    mis__ins__pz_detail_cp_osoby = sources_df['clean']['mis__ins__pz_detail_cp_osoby']
    mis__ins__pz_detail_domos_detail = sources_df['clean']['mis__ins__pz_detail_domos_detail']
    mis__ins__pz_detail_domos_plochy = sources_df['clean']['mis__ins__pz_detail_domos_plochy']
    mis__ins__pz_detail_life_person = sources_df['clean']['mis__ins__pz_detail_life_person']

    df_dtsk = prepare_dtsk(dmsk__l0_owner__party, dmsk__l0_owner__party_rltd, spark)
    df_cpz = prepare_cpz(dmsk__l0_owner__cont, dmsk__l0_owner__cont_rltd, spark)
    df_ukaz1 = prepare_ukaz1(dmsk__l0_owner__cont, dmsk__l0_owner__cont_val, dmsk__l0_owner__cont_dt_hist, spark)
    df_cont_stav = prepare_cont_stav(dmsk__l0_owner__cont_stat_type_gen, spark)
    df_stg_insurance_cont = prepare_stg_insurance_cont(dmsk__l0_owner__party, df_dtsk, dmsk__l0_owner__party_cont,
                                                       dmsk__l0_owner__cont, df_cpz, dmsk__l0_owner__cont_gen,
                                                       dmsk__l0_owner__cont_prod, dmsk__l0_owner__prod,
                                                       dmsk__l0_owner__cont_stat, df_cont_stav,
                                                       dmsk__l0_owner__cont_dt_actl, df_ukaz1, spark)
    df_csv = prepare_csv(dmsk__l0_owner__card_svc, dmsk__l0_owner__prod, dmsk__l0_owner__prod_nm,
                         dmsk__l0_owner__prod_hier, spark)

    df_tmp_insurance_cont1 = prepare_tmp_insurance_cont1(df_csv, feature__card,
                                                         feature__product_level_category,
                                                         dmsk__l0_owner__prod, df_stg_insurance_cont, spark)
    df_c = prepare_c(df_stg_insurance_cont, spark)
    df_contract = prepare_contract(df_c, mis__ins__cakaren_storien_detail_dat,
                                   mis__ins__pzp_storna_dat,
                                   mis__ins__poistovna_zmluvy_dat, spark)
    df_cp_info = prepare_cp_info(mis__ins__pz_detail_cp_detail, spark)
    df_cp_osoby2 = prepare_cp_osoby2(mis__ins__pz_detail_cp_osoby, spark)
    df_cp_osoby3 = prepare_cp_osoby3(mis__ins__pz_detail_cp_osoby, spark)
    df_cp_osoby4 = prepare_cp_osoby4(mis__ins__pz_detail_cp_osoby, spark)
    df_domos = prepare_domos(mis__ins__pz_detail_domos_detail, spark)
    df_domos_plocha = prepare_domos_plocha(mis__ins__pz_detail_domos_plochy, spark)
    df_domos_ps = prepare_domos_ps(mis__ins__pz_detail_domos_plochy, spark)
    df_finukaz = prepare_finukaz(dmsk__l0_owner__cont, dmsk__l0_owner__cont_val, dmsk__l0_owner__cont_dt_hist, spark)
    df_upom = prepare_upom(mis__ins__banka_upomienky_dat, spark)
    df_pu = prepare_pu(mis__ins__prieskum_spokojnosti_dat, spark)
    df_tmp_insurance_cont2 = prepare_tmp_insurance_cont2(df_contract, feature__product_level_category,
                                                         df_cp_info, df_cp_osoby2, df_cp_osoby3,
                                                         df_cp_osoby4, df_domos, df_domos_plocha,
                                                         df_domos_ps, df_finukaz, df_upom, df_pu,
                                                         dmsk__je69050__administrative_structure_svk,
                                                         dmsk__je69050__area_code_gen, spark)
    df_tmp_insurance_cont = df_tmp_insurance_cont1.union(df_tmp_insurance_cont2)
    df_tmp_life_assured_prep = prepare_tmp_life_assured_prep(dmsk__l0_owner__party,
                                                             dmsk__l0_owner__party_indiv,
                                                             dmsk__l0_owner__party_rltd, spark)
    df_tmp_life_assured = prepare_tmp_life_assured(mis__ins__pz_detail_life_person, df_tmp_life_assured_prep, spark)
    df_tmp_life_assured_c = prepare_tmp_life_assured_c(mis__ins__pz_detail_life_person, df_tmp_life_assured_prep, spark)
    feature__attr_insurance = prepare_attr_insurance(feature__attr_client_base, df_tmp_insurance_cont,
                                                     df_tmp_life_assured_c, df_tmp_life_assured,
                                                     df_stg_insurance_cont, feature__attr_insurance, spark)
    return feature__attr_insurance


def prepare_attr_insurance(feature__attr_client_base, df_tmp_insurance_cont,
                           df_tmp_life_assured_c, df_tmp_life_assured,
                           df_stg_insurance_cont, feature__attr_insurance, spark):
    """
    prepare_attr_insurance
    :param feature__attr_client_base\
    :param df_tmp_insurance_cont\
    :param df_tmp_life_assured_c\
    :param df_tmp_life_assured\
    :param df_stg_insurance_cont\
    :param feature__attr_insurance\
    :param spark\
    :return df_attr_insurance
    """
    df_by_customer = prepare_by_customer(feature__attr_client_base, df_tmp_insurance_cont, spark)
    df_by_customer.createOrReplaceTempView("by_customer")
    df_key_employee = prepare_key_employee(df_stg_insurance_cont, df_tmp_life_assured_c, spark)
    df_key_employee.createOrReplaceTempView("key_employee")
    df_life_assured = prepare_life_assured(df_stg_insurance_cont, df_tmp_life_assured, spark)
    df_life_assured.createOrReplaceTempView("life_assured")
    df_prev_vals = prepare_prev_vals(feature__attr_insurance, spark)
    df_prev_vals.createOrReplaceTempView("prev_vals")
    df_prev_val_q = prepare_prev_val_q(feature__attr_insurance, spark)
    df_prev_val_q.createOrReplaceTempView("prev_val_q")
    df_agg_prev_two_m = prepare_agg_prev_two_m(feature__attr_insurance, spark)
    df_agg_prev_two_m.createOrReplaceTempView("agg_prev_two_m")
    df_agg_prev_y = prepare_agg_prev_y(feature__attr_insurance, spark)
    df_agg_prev_y.createOrReplaceTempView("agg_prev_y")

    df_attr_insurance = spark.sql("""
    select
        month_id,
        customerid,
        i_active_ins,
        c_active_ins,
        recency_start_ins,
        day_end_ins,
        recency_end_ins,
        recency_canceled_ins,
        recency_payout_ins,
        c_cont_ins as c_total_ins,
        c_canceled_total_ins,
        i_canceled_ins,
        c_household_property_west_ins,
        c_household_property_central_ins,
        c_household_property_east_ins,
        c_household_property_capital_ins,
        sum_area_household_ins,
        max_area_household_ins,
        sum_area_house_ins,
        max_area_house_ins,
        sum_area_ins,
        max_area_ins,
        c_claim_ins_m,
        c_event_ins_m,
        c_claim_denied_ins_m,
        c_leasing_ins,
        i_reminder_ins_m,
        c_payout_ins_m,
        sum_payout_ins_m,
        c_vehicle_event_ins as c_vehicle_event_ins_m,
        c_household_property_event_ins as c_household_property_event_ins_m,
        c_life_event_ins as c_life_event_ins_m,
        max_c_person_ins,
        max_c_senior_ins,
        max_c_child_ins,
        max_c_small_child_ins,
        max_c_teen_ins,
        avg_c_person_ins,
        c_domos_type_0_ins,
        c_domos_type_1_ins,
        c_domos_type_2_ins,
        c_domos_type_3_ins,
        c_travel_ins,
        c_mtpl_ins,
        c_casco_ins,
        c_household_property_ins,
        c_reg_expenses_ins,
        c_card_travel_ins,
        c_accident_vehicle_ins,
        c_business_risk_ins,
        c_risk_life_ins,
        c_invest_life_ins,
        c_closed_travel_ins,
        c_closed_mtpl_ins,
        c_closed_casco_ins,
        c_closed_household_property_ins,
        c_closed_reg_expenses_ins,
        c_closed_card_travel_ins,
        c_closed_accident_vehicle_ins,
        c_closed_business_risk_ins,
        c_closed_risk_life_ins,
        c_closed_invest_life_ins,
        c_opened_travel_ins,
        c_opened_mtpl_ins,
        c_opened_casco_ins,
        c_opened_household_property_ins,
        c_opened_reg_expenses_ins,
        c_opened_card_travel_ins,
        c_opened_accident_vehicle_ins,
        c_opened_business_risk_ins,
        c_opened_risk_life_ins,
        c_opened_invest_life_ins,
        sum_payment_year_ins,
        sum_payment_year_life_ins,
        sum_payment_year_nonlife_ins,
        c_payment_year_ins,
        c_prod_ins,
        payment_travel_ins,
        payment_mtpl_ins,
        payment_casco_ins,
        payment_household_property_ins,
        payment_reg_expenses_ins,
        payment_card_travel_ins,
        payment_accident_vehicle_ins,
        payment_business_risk_ins,
        payment_risk_life_ins,
        payment_invest_life_ins,
        d_end_travel_ins,
        d_end_mtpl_ins,
        d_end_casco_ins,
        d_end_reg_expenses_ins,
        d_end_card_travel_ins,
        d_end_business_risk_ins,
        d_end_risk_life_ins,
        d_end_invest_life_ins,
        recency_travel_ins,
        recency_mtpl_ins,
        recency_household_property_ins,
        i_pool_ins,
        i_liability_ins,
        sum_capital_value_ins,
        c_yearly_payment_ins,
        c_bi_yearly_payment_ins as c_biyearly_payment_ins,
        c_quarterly_payment_ins,
        c_monthly_payment_ins,
        avg_luggage_amt_travel_ins as avg_luggage_travel_ins,
        sum_duration_travel_ins,
        c_europe_travel_ins,
        c_tourist_travel_ins,
        i_risksport_travel_ins,
        i_work_travel_ins,
        i_family_travel_ins,
        i_exclusive_travel_ins,
        sum_owed_ins_m,
        i_owed_ins_m,
        i_opened_ins,
        c_opened_ins,
        sum_payout_ins_q,
        sum_payout_ins_y,
        c_claim_ins_q,
        c_claim_ins_y,
        i_claim_denied_ins_q,
        i_claim_denied_ins_y,
        i_work_travel_ins_q,
        i_work_travel_ins_y,
        rel_change_payment_year_ins_q,
        i_opened_life_ins_q,
        i_opened_nonlife_ins_q,
        i_opened_life_ins_y,
        i_opened_nonlife_ins_y,
        i_life_ins_q,
        i_nonlife_ins_q,
        i_life_ins_y,
        i_nonlife_ins_y,
        i_reminder_ins_q,
        i_reminder_ins_y,
        i_owed_ins_q,
        i_owed_ins_y,
        sum_duration_travel_ins_y,
        i_key_employee_ins,
        i_life_assured_ins
from (
     select by_customer.*
          , case when coalesce(by_customer.C_ACTIVE_INS, 0) > coalesce(prev_vals.C_ACTIVE_INS, 0)
            then 1 else 0 end as i_opened_ins
          , case when coalesce(by_customer.C_ACTIVE_INS, 0) > coalesce(prev_vals.C_ACTIVE_INS, 0)
            then coalesce(by_customer.C_ACTIVE_INS, 0) - coalesce(prev_vals.C_ACTIVE_INS, 0)
            else 0 end as c_opened_ins
          , by_customer.SUM_PAYOUT_INS_M + coalesce(agg_prev_two_m.SUM_PAYOUT_INS, 0) as sum_payout_ins_q
          , by_customer.SUM_PAYOUT_INS_M + coalesce(agg_prev_y.SUM_PAYOUT_INS, 0) as sum_payout_ins_y
          , by_customer.C_CLAIM_INS_M + coalesce(agg_prev_two_m.C_CLAIM_INS, 0) as c_claim_ins_q
          , by_customer.C_CLAIM_INS_M + coalesce(agg_prev_y.C_CLAIM_INS, 0) as c_claim_ins_y
          , greatest_oracle(least_oracle(by_customer.C_CLAIM_DENIED_INS_M, 1),
                            coalesce(agg_prev_two_m.C_CLAIM_DENIED_INS_Q, 0)) as i_claim_denied_ins_q
          , greatest_oracle(least_oracle(by_customer.C_CLAIM_DENIED_INS_M, 1),
                            coalesce(agg_prev_y.C_CLAIM_DENIED_INS_Y, 0)) as i_claim_denied_ins_y
          , greatest_oracle(by_customer.I_WORK_TRAVEL_INS,
                            coalesce(agg_prev_two_m.I_WORK_TRAVEL_INS_Q, 0)) as i_work_travel_ins_q
          , greatest_oracle(by_customer.I_WORK_TRAVEL_INS,
                            coalesce(agg_prev_y.I_WORK_TRAVEL_INS_Y, 0)) as i_work_travel_ins_y
          , case when prev_val_q.SUM_PAYMENT_YEAR_INS <> 0
                 then (by_customer.SUM_PAYMENT_YEAR_INS -
                        prev_val_q.SUM_PAYMENT_YEAR_INS) / prev_val_q.sum_payment_year_ins
                 else 0 end as rel_change_payment_year_ins_q
          , greatest_oracle(case when by_customer.c_opened_RISK_LIFE_INS + by_customer.c_opened_invest_life_ins
                              + by_customer.c_opened_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_two_m.I_OPENED_LIFE_INS_Q, 0)) as i_opened_life_ins_q
          , greatest_oracle(case when by_customer.c_opened_TRAVEL_INS + by_customer.c_opened_MTPL_ins
                              + by_customer.c_opened_CASCO_ins + by_customer.c_opened_household_property_ins
                              + by_customer.c_opened_card_travel_ins + by_customer.c_opened_accident_vehicle_ins
                              + by_customer.c_opened_business_risk_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_two_m.I_OPENED_NONLIFE_INS_Q, 0)) as i_opened_nonlife_ins_q
          , greatest_oracle(case when by_customer.c_opened_RISK_LIFE_INS + by_customer.c_opened_invest_life_ins
                               + by_customer.c_opened_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_y.I_OPENED_LIFE_INS_Y, 0)) as i_opened_life_ins_y
          , greatest_oracle(case when by_customer.c_opened_TRAVEL_INS + by_customer.c_opened_MTPL_ins
                              + by_customer.c_opened_CASCO_ins + by_customer.c_opened_household_property_ins
                              + by_customer.c_opened_card_travel_ins + by_customer.c_opened_accident_vehicle_ins
                              + by_customer.c_opened_business_risk_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_y.I_OPENED_NONLIFE_INS_Y, 0)) as i_opened_nonlife_ins_y
           , greatest_oracle(case when by_customer.c_RISK_LIFE_INS + by_customer.c_invest_life_ins
                               + by_customer.c_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_two_m.I_LIFE_INS_Q, 0)) as i_life_ins_q
          , greatest_oracle(case when by_customer.c_active_ins - by_customer.c_RISK_LIFE_INS
                               - by_customer.c_invest_life_ins
                               - by_customer.c_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_two_m.I_NONLIFE_INS_Q, 0)) as i_nonlife_ins_q
          ,  greatest_oracle(case when by_customer.c_RISK_LIFE_INS + by_customer.c_invest_life_ins
                               + by_customer.c_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_y.I_LIFE_INS_Y, 0)) as i_life_ins_y
          , greatest_oracle(case when by_customer.c_active_ins - by_customer.c_RISK_LIFE_INS
                               - by_customer.c_invest_life_ins
                               - by_customer.c_reg_expenses_ins > 0 then 1 else 0 end,
                    coalesce(agg_prev_y.I_NONLIFE_INS_Y, 0)) as i_nonlife_ins_y
          , greatest_oracle(
              by_customer.I_REMINDER_INS_M, coalesce(agg_prev_two_m.I_REMINDER_INS_Q, 0)) as i_reminder_ins_q
          , greatest_oracle(by_customer.I_REMINDER_INS_M, coalesce(agg_prev_y.I_REMINDER_INS_Y, 0)) as i_reminder_ins_y
          , greatest_oracle(by_customer.I_OWED_INS_M, coalesce(agg_prev_two_m.I_OWE_INS_Q, 0)) as i_owed_ins_q
          , greatest_oracle(by_customer.I_OWED_INS_M, coalesce(agg_prev_y.I_OWE_INS_Y, 0))   as i_owed_ins_y
          , by_customer.SUM_DURATION_TRAVEL_INS
                                + coalesce(agg_prev_y.SUM_DURATION_TRAVEL_INS_Y, 0) as sum_duration_travel_ins_y
          , coalesce(key_employee.I_KEY_EMPLOYEE_INS, 0) as i_key_employee_ins
          , coalesce(life_assured.I_LIFE_ASSURED, 0) as i_life_assured_ins
     from by_customer
     LEFT JOIN key_employee  ON by_customer.customerid = key_employee.customerid
     LEFT JOIN life_assured  ON by_customer.customerid = life_assured.customerid
     LEFT JOIN prev_vals     ON by_customer.customerid = prev_vals.customerid
     LEFT JOIN prev_val_q    ON by_customer.customerid = prev_val_q.customerid
     LEFT JOIN agg_prev_two_m      ON by_customer.customerid = agg_prev_two_m.customerid
     LEFT JOIN agg_prev_y       ON by_customer.customerid = agg_prev_y.customerid
     )
     """)
    return df_attr_insurance


def prepare_b(feature__attr_client_base, spark):
    """
    prepare_b
    :param feature__attr_client_base\
    :param spark\
    :return df_b
    """
    feature__attr_client_base.createOrReplaceTempView("feature__attr_client_base")
    df_b = spark.sql("""
        select month_id, customerid
        from feature__attr_client_base
        where month_id = INT(DATE_FORMAT(${process_dt}, 'yyyyMM'))
    """)
    return df_b


def prepare_by_customer(feature__attr_client_base, df_tmp_insurance_cont, spark):
    """
    prepare_by_customer
    :param feature__attr_client_base\
    :param df_tmp_insurance_cont\
    :param spark\
    :return df_by_customer
    """
    df_b = prepare_b(feature__attr_client_base, spark)
    df_b.createOrReplaceTempView("b")
    df_tmp_insurance_cont.createOrReplaceTempView("feature__tmp_insurance_cont")
    df_by_customer = spark.sql("""
                select
        min(b.month_id) as MONTH_ID
        , b.customerid
        , max(coalesce(insur.ACTIVE, 0)) as I_ACTIVE_INS
        , sum(coalesce(insur.ACTIVE, 0)) as C_ACTIVE_INS

        , least_oracle(min(coalesce(insur.DAYS_FROM_START,
                                datediff(${process_dt}, ${process_dt_hist}))),
                                 datediff(${process_dt}, ${process_dt_hist})) as RECENCY_START_INS
        , least_oracle(max(case when insur.DAYS_TO_END > 0 then insur.DAYS_TO_END
                     else datediff(${process_dt}, ${process_dt_hist}) end),
                    datediff(${process_dt}, ${process_dt_hist})) as DAY_END_INS

        , least_oracle(max(case when insur.DAYS_TO_END < 0 then -1 * insur.DAYS_TO_END
                     else datediff(${process_dt}, ${process_dt_hist}) end),
                      datediff(${process_dt}, ${process_dt_hist})) as RECENCY_END_INS
        , least_oracle(min(coalesce(case when insur.DAYS_FROM_CANCELED >= 0 then insur.DAYS_FROM_CANCELED else null end,
                    datediff(${process_dt}, ${process_dt_hist}))),
                    datediff(${process_dt}, ${process_dt_hist})) as RECENCY_CANCELED_INS
        , least_oracle(min(coalesce(case when insur.DAYS_FROM_PAYOUT >= 0 then insur.DAYS_FROM_PAYOUT else null end,
                 datediff(${process_dt}, ${process_dt_hist}))),
                  datediff(${process_dt}, ${process_dt_hist})) as RECENCY_PAYOUT_INS

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
                  + NVL(insur.domacnost_vedl_budova_m2,0) + NVL(insur.domacnost_prizemie_m2,0)
                  + NVL(insur.domacnost_pivnica_m2,0)
                  + NVL(insur.domacnost_povala_m2,0) + NVL(insur.domacnost_podlazie_m2,0)
                  ) as MAX_AREA_HOUSEHOLD_INS

        , SUM(NVL(insur.dom_obyvana_m2,0) + NVL(insur.dom_neobyvana_m2,0) ) as SUM_AREA_HOUSE_INS
        , MAX(NVL(insur.dom_obyvana_m2,0) + NVL(insur.dom_neobyvana_m2,0) ) as MAX_AREA_HOUSE_INS

        , SUM(NVL(insur.pivnica_plocha,0) + NVL(insur.garaz_plocha,0) + NVL(insur.povala_plocha,0)
                  + NVL(insur.vedl_budova_plocha,0) + NVL(insur.prizemie_plocha,0) + NVL(insur.podkrovie_plocha,0)
                  + NVL(insur.podlazie_plocha,0) + NVL(insur.byt_plocha,0) + NVL(insur.dom_obyvana_m2,0)
                  + NVL(insur.dom_neobyvana_m2,0)
                  ) AS SUM_AREA_INS

        , MAX(NVL(insur.pivnica_plocha,0) + NVL(insur.garaz_plocha,0) + NVL(insur.povala_plocha,0)
                  + NVL(insur.vedl_budova_plocha,0) + NVL(insur.prizemie_plocha,0) + NVL(insur.podkrovie_plocha,0)
                  + NVL(insur.podlazie_plocha,0) + NVL(insur.byt_plocha,0) + NVL(insur.dom_obyvana_m2,0)
                  + NVL(insur.dom_neobyvana_m2,0)
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

        , SUM(case when insur.active = 1 then coalesce(insur.household_property_ins_type0, 0)
              else 0 end ) as c_domos_type_0_ins
        , SUM(case when insur.active = 1 then coalesce(insur.household_property_ins_type1, 0)
              else 0 end ) as c_domos_type_1_ins
        , SUM(case when insur.active = 1 then coalesce(insur.household_property_ins_type2, 0)
              else 0 end ) as c_domos_type_2_ins
        , SUM(case when insur.active = 1 then coalesce(insur.household_property_ins_type3, 0)
              else 0 end ) as c_domos_type_3_ins

        -- active insurances
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.TRAVEL_INS else 0 end) as C_TRAVEL_INS
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.MTPL_INS else 0 end) as C_MTPL_INS
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.CASCO_INS else 0 end) as C_CASCO_INS
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.household_property_ins
         else 0 end) as C_household_property_ins
        --, sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.loan_ins else 0 end) as C_LOAN_INS
        , max(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.reg_expenses_ins
             else 0 end) as C_REG_EXPENSES_INS
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.card_travel_ins
            else 0 end) as C_CARD_TRAVEL_INS
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.drivers_accident_ins
            else 0 end) as C_ACCIDENT_VEHICLE_INS
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.business_risk_ins
            else 0 end) as C_BUSINESS_RISK_INS
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.RISK_LIFE_INS
            else 0 end) as C_RISK_LIFE_INS
        , sum(case when insur.DAYS_TO_END > 0 and insur.ACTIVE = 1 then insur.INVEST_LIFE_INS
            else 0 end) as C_INVEST_LIFE_INS

        -- closed insurances
        , sum(case when insur.ACTIVE = 0 then insur.TRAVEL_INS else 0 end) as C_CLOSED_TRAVEL_INS
        , sum(case when insur.ACTIVE = 0 then insur.MTPL_INS else 0 end) as C_CLOSED_MTPL_INS
        , sum(case when insur.ACTIVE = 0 then insur.CASCO_INS else 0 end) as C_CLOSED_CASCO_INS
        , sum(case when insur.ACTIVE = 0 then insur.household_property_ins
             else 0 end) as C_CLOSED_household_property_ins
        --, sum(case when insur.ACTIVE = 0 then insur.loan_ins else 0 end) as C_CLOSED_LOAN_INS
        , max(case when insur.ACTIVE = 0 then insur.reg_expenses_ins else 0 end) as C_CLOSED_REG_EXPENSES_INS
        , sum(case when insur.ACTIVE = 0 then insur.card_travel_ins else 0 end) as C_CLOSED_CARD_TRAVEL_INS
        , sum(case when insur.ACTIVE = 0 then insur.drivers_accident_ins
              else 0 end) as C_CLOSED_ACCIDENT_VEHICLE_INS
        , sum(case when insur.ACTIVE = 0 then insur.business_risk_ins else 0 end) as C_CLOSED_BUSINESS_RISK_INS
        , sum(case when insur.ACTIVE = 0 then insur.RISK_LIFE_INS else 0 end) as C_CLOSED_RISK_LIFE_INS
        , sum(case when insur.ACTIVE = 0 then insur.INVEST_LIFE_INS else 0 end) as C_CLOSED_INVEST_LIFE_INS

        -- opened insurances
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
                add_months_oracle(${process_dt}, -1)) then insur.TRAVEL_INS else 0 end) as C_OPENED_TRAVEL_INS
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
                add_months_oracle(${process_dt}, -1)) then insur.MTPL_INS else 0 end) as C_OPENED_MTPL_INS
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
                add_months_oracle(${process_dt}, -1)) then insur.CASCO_INS else 0 end) as C_OPENED_CASCO_INS
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
                add_months_oracle(${process_dt}, -1)) then insur.household_property_ins
                    else 0 end) as C_OPENED_household_property_ins
        , max(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
            add_months_oracle(${process_dt}, -1)) then insur.reg_expenses_ins else 0 end) as C_OPENED_REG_EXPENSES_INS
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
            add_months_oracle(${process_dt}, -1)) then insur.card_travel_ins else 0 end) as C_OPENED_CARD_TRAVEL_INS
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
            add_months_oracle(${process_dt}, -1)) then insur.drivers_accident_ins
             else 0 end) as C_OPENED_ACCIDENT_VEHICLE_INS
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
                                     add_months_oracle(${process_dt}, -1)) then insur.business_risk_ins
                    else 0 end) as C_OPENED_BUSINESS_RISK_INS
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
                            add_months_oracle(${process_dt}, -1)) then insur.RISK_LIFE_INS
              else 0 end) as C_OPENED_RISK_LIFE_INS
        , sum(case when 0 < insur.days_from_start AND insur.days_from_start < datediff(${process_dt},
         add_months_oracle(${process_dt}, -1)) then insur.INVEST_LIFE_INS
          else 0 end) as C_OPENED_INVEST_LIFE_INS

        , sum(coalesce(case when insur.active = 1 then insur.PAYMENT_YEAR else 0 end, 0)) as SUM_PAYMENT_YEAR_INS
        , sum(case when insur.active = 1 and insur.I_NONLIFE_INS = 0 then
         coalesce(insur.PAYMENT_YEAR, 0) else 0 end) as SUM_PAYMENT_YEAR_LIFE_INS
        , sum(case when insur.active = 1 and insur.I_NONLIFE_INS = 1 then
         coalesce(insur.PAYMENT_YEAR, 0) else 0 end) as SUM_PAYMENT_YEAR_NONLIFE_INS
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
        , least_oracle(MIN(CASE WHEN (insur.travel_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                    THEN insur.days_to_end else datediff(${process_dt}, ${process_dt_hist}) end),
                     datediff(${process_dt}, ${process_dt_hist})) as D_END_TRAVEL_INS
        , least_oracle(MIN(CASE WHEN (insur.mtpl_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                    THEN insur.days_to_end else datediff(${process_dt}, ${process_dt_hist}) end),
                     datediff(${process_dt}, ${process_dt_hist})) as D_END_MTPL_INS
        , least_oracle(MIN(CASE WHEN (insur.casco_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                    THEN insur.days_to_end else datediff(${process_dt}, ${process_dt_hist}) end),
                     datediff(${process_dt}, ${process_dt_hist})) as D_END_CASCO_INS
        , least_oracle(MIN(CASE WHEN (insur.reg_expenses_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                    THEN insur.days_to_end else datediff(${process_dt}, ${process_dt_hist}) end),
                     datediff(${process_dt}, ${process_dt_hist})) as D_END_REG_EXPENSES_INS
        , least_oracle(MIN(CASE WHEN (insur.card_travel_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                    THEN insur.days_to_end else datediff(${process_dt}, ${process_dt_hist}) end),
                     datediff(${process_dt}, ${process_dt_hist})) as D_END_CARD_TRAVEL_INS
        , least_oracle(MIN(CASE WHEN (insur.business_risk_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                    THEN insur.days_to_end else datediff(${process_dt}, ${process_dt_hist}) end),
                     datediff(${process_dt}, ${process_dt_hist})) as D_END_BUSINESS_RISK_INS
        , least_oracle(MIN(CASE WHEN (insur.risk_life_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                    THEN insur.days_to_end else datediff(${process_dt}, ${process_dt_hist}) end),
                     datediff(${process_dt}, ${process_dt_hist})) as D_END_RISK_LIFE_INS
        , least_oracle(MIN(CASE WHEN (insur.invest_life_ins = 1 and insur.ACTIVE = 1 and insur.days_to_end >= 0)
                    THEN insur.days_to_end else datediff(${process_dt}, ${process_dt_hist}) end),
                    datediff(${process_dt}, ${process_dt_hist})) as D_END_INVEST_LIFE_INS

            ----time_since_opened
        , MIN(CASE WHEN (insur.travel_ins = 1)
                    THEN least_oracle(insur.DAYS_FROM_START, datediff(${process_dt}, ${process_dt_hist}))
                     else datediff(${process_dt}, ${process_dt_hist}) end) as RECENCY_TRAVEL_INS
        , MIN(CASE WHEN (insur.mtpl_ins = 1)
                    THEN least_oracle(insur.DAYS_FROM_START, datediff(${process_dt}, ${process_dt_hist}))
                     else datediff(${process_dt}, ${process_dt_hist}) end) as RECENCY_MTPL_INS
        , MIN(CASE WHEN (insur.household_property_ins = 1)
                    THEN least_oracle(insur.DAYS_FROM_START, datediff(${process_dt}, ${process_dt_hist}))
                     else datediff(${process_dt}, ${process_dt_hist}) end) as RECENCY_HOUSEHOLD_PROPERTY_INS


        , max(coalesce(insur.flg_pool, 0)) as I_POOL_INS
        , max(coalesce(insur.FLG_LIABILITY, 0)) as I_LIABILITY_INS

        , sum(case when insur.active = 1 then coalesce(insur.CAPITAL_VALUE, 0) else 0 end) as SUM_CAPITAL_VALUE_INS
        , SUM(CASE WHEN (insur.active = 1 and insur.num_payment_year=1)
                THEN 1 else 0 end) as  C_YEARLY_PAYMENT_INS
        , SUM(CASE WHEN (insur.active = 1 and insur.num_payment_year=2)
                THEN 1 else 0 end) as  C_BI_YEARLY_PAYMENT_INS
        , SUM(CASE WHEN (insur.active = 1 and insur.num_payment_year=4)
                THEN 1 else 0 end) as  C_QUARTERLY_PAYMENT_INS
        , SUM(CASE WHEN (insur.active = 1 and insur.num_payment_year=12)
                THEN 1 else 0 end) as  C_MONTHLY_PAYMENT_INS

        , case when (SUM(coalesce(insur.travel_ins, 0)) + SUM(coalesce(insur.card_travel_ins, 0))) = 0
                   THEN 0
                   else SUM(coalesce(insur.travel_ins_luggage_amt,0))
                        / (SUM(insur.travel_ins) + SUM(insur.card_travel_ins)) end as AVG_LUGGAGE_AMT_TRAVEL_INS
        , SUM(case when (insur.travel_ins = 1
                         and date_add(${process_dt}, - cast(insur.DAYS_FROM_START as int)) <= ${process_dt}
                         and (
                         date_add(date_add(${process_dt}, -
                         cast (insur.DAYS_FROM_START as int)), cast(insur.duration_travel_ins as int)
                         )) > add_months_oracle(${process_dt}, -1) )
                   THEN insur.duration_travel_ins
                   else 0 end) as SUM_DURATION_TRAVEL_INS

        , sum(case when insur.active = 1 then coalesce(insur.TRAVEL_INS_EUROPE, 0)
                else 0 end) as C_EUROPE_TRAVEL_INS
        , sum(coalesce(insur.travel_ins_tourist, 0)) as C_TOURIST_TRAVEL_INS
        , max(coalesce(insur.travel_ins_risksport, 0)) as I_RISKSPORT_TRAVEL_INS

        , greatest_oracle(max(case when insur.active = 1 then coalesce(insur.travel_ins_manual_labor, 0) else 0 end),
                   max(case when insur.active = 1 then coalesce(insur.travel_ins_nonmanual_labor, 0)
                   else 0 end)) as I_WORK_TRAVEL_INS
        , max(coalesce(insur.travel_ins_family, 0)) as I_family_travel_ins
        , max(coalesce(insur.travel_ins_exclusive, 0)) as I_exclusive_travel_ins

        , sum(case when insur.active = 1 then coalesce(insur.sum_owed, 0) else 0 end) as SUM_OWED_INS_M
        , max(case when insur.active = 1 and coalesce(insur.sum_owed, 0) > 0 then 1 else 0 end) as I_OWED_INS_M

        from b
        LEFT JOIN feature__tmp_insurance_cont insur
        ON b.customerid = insur.customerid
        group by b.customerid
    """)
    return df_by_customer


def prepare_key_employee(df_stg_insurance_cont, df_tmp_life_assured_c, spark):
    """
    prepare_key_employee
    :param df_stg_insurance_cont\
    :param df_tmp_life_assured_c\
    :param spark\
    :return df_key_employee
    """
    df_stg_insurance_cont.createOrReplaceTempView("feature__stg_insurance_cont")
    df_tmp_life_assured_c.createOrReplaceTempView("feature__tmp_life_assured_c")
    df_key_employee = spark.sql("""
            SELECT TMP.customerid,
                (case when count(*) > 0 then 1 else 0 end) as I_KEY_EMPLOYEE_INS
            FROM feature__stg_insurance_cont CONT
            JOIN feature__tmp_life_assured_c TMP
                ON CONT.CONT_ACCT = TMP.CPZ
                AND CONT.ACTIVE = 1
                AND CONT.DT_START_INS <= ${process_dt}
                AND (CONT.DT_CANCELED_INS is null OR CONT.DT_CANCELED_INS > ${process_dt})
                AND CONT.DT_END_INS > ${process_dt}
            GROUP BY TMP.customerid
    """)
    return df_key_employee


def prepare_life_assured(df_stg_insurance_cont, df_tmp_life_assured, spark):
    """
    prepare_life_assured
    :param df_stg_insurance_cont\
    :param df_tmp_life_assured\
    :param spark\
    :return df_life_assured
    """
    df_stg_insurance_cont.createOrReplaceTempView("feature__stg_insurance_cont")
    df_tmp_life_assured.createOrReplaceTempView("feature__tmp_life_assured")
    df_life_assured = spark.sql("""
        SELECT  ASSURED.customerid,
                (case when count(*) > 0 then 1 else 0 end) as I_LIFE_ASSURED
        FROM feature__stg_insurance_cont CONT
        JOIN feature__tmp_life_assured ASSURED
                ON CONT.CONT_ACCT = ASSURED.CPZ
                AND CONT.ACTIVE = 1
                AND CONT.DT_START_INS <= ${process_dt}
                AND CONT.DT_END_INS > ${process_dt}
                AND (CONT.DT_CANCELED_INS is null or CONT.DT_CANCELED_INS > ${process_dt})
        GROUP BY ASSURED.customerid
    """)
    return df_life_assured


def prepare_prev_vals(feature__attr_insurance, spark):
    """
    prepare_prev_vals
    :param feature__attr_insurance\
    :param spark\
    :return df_prev_vals
    """
    feature__attr_insurance.createOrReplaceTempView("feature__attr_insurance")
    df_prev_vals = spark.sql("""
        select customerid, C_ACTIVE_INS
        from feature__attr_insurance
        where month_id = INT(DATE_FORMAT(add_months_oracle(${process_dt}, -1), 'yyyyMM'))
    """)
    return df_prev_vals


def prepare_prev_val_q(feature__attr_insurance, spark):
    """
    prepare_prev_val_q
    :param feature__attr_insurance\
    :param spark\
    :return df_prev_val_q
    """
    feature__attr_insurance.createOrReplaceTempView("feature__attr_insurance")
    df_prev_val_q = spark.sql("""
        select customerid, SUM_PAYMENT_YEAR_INS
        from feature__attr_insurance
        where month_id = INT(DATE_FORMAT(add_months_oracle(${process_dt}, -3), 'yyyyMM'))
    """)
    return df_prev_val_q


def prepare_agg_prev_two_m(feature__attr_insurance, spark):
    """
    prepare_agg_prev_two_m
    :param feature__attr_insurance\
    :param spark\
    :return df_agg_prev_two_m
    """
    feature__attr_insurance.createOrReplaceTempView("feature__attr_insurance")
    df_agg_prev_two_m = spark.sql("""
        select customerid, sum(SUM_PAYOUT_INS_M) as SUM_PAYOUT_INS, sum(C_CLAIM_INS_M) as C_CLAIM_INS,
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
              from feature__attr_insurance
              where month_id >= INT(DATE_FORMAT(add_months_oracle(${process_dt}, -2), 'yyyyMM'))
              and month_id <= INT(DATE_FORMAT(add_months_oracle(${process_dt}, -1), 'yyyyMM'))
              group by customerid
    """)
    return df_agg_prev_two_m


def prepare_agg_prev_y(feature__attr_insurance, spark):
    """
    prepare_agg_prev_y
    :param feature__attr_insurance\
    :param spark\
    :return df_agg_prev_y
    """
    feature__attr_insurance.createOrReplaceTempView("feature__attr_insurance")
    df_agg_prev_y = spark.sql("""
    select customerid, sum(SUM_PAYOUT_INS_M) as SUM_PAYOUT_INS, sum(C_CLAIM_INS_M) as C_CLAIM_INS,
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
    from feature__attr_insurance
    where month_id >= INT(DATE_FORMAT(add_months_oracle(${process_dt}, -11), 'yyyyMM'))
              and month_id <= INT(DATE_FORMAT(add_months_oracle(${process_dt}, -1), 'yyyyMM'))
    group by customerid
     """)
    return df_agg_prev_y


def prepare_tmp_life_assured_prep(
    dmsk__l0_owner__party,
    dmsk__l0_owner__party_indiv,
    dmsk__l0_owner__party_rltd,
    spark,
):
    """
    Prepare temporary life assured dataset.

    :param dmsk__l0_owner__party: Party dataframe
    :param dmsk__l0_owner__party_indiv: Party individual dataframe
    :param dmsk__l0_owner__party_rltd: Party relationship dataframe
    :param spark: Spark session
    :return: df_tmp_life_assured_prep
    """
    dmsk__l0_owner__party.createOrReplaceTempView("dmsk__l0_owner__party")
    dmsk__l0_owner__party_indiv.createOrReplaceTempView("dmsk__l0_owner__party_indiv")
    dmsk__l0_owner__party_rltd.createOrReplaceTempView("dmsk__l0_owner__party_rltd")

    df_tmp_life_assured_prep = spark.sql(
        """
        SELECT
            CUSTOMERID,
            PARTY_INDIV_RC
        FROM (
            SELECT
                PRL.PARTY_ID AS CUSTOMERID,
                PI.PARTY_INDIV_RC,
                ROW_NUMBER() OVER (
                    PARTITION BY PI.PARTY_INDIV_RC
                    ORDER BY SRC_SYST_CD ASC, party_indiv_start_dt DESC
                ) AS row_n
            FROM dmsk__l0_owner__party P
            INNER JOIN dmsk__l0_owner__party_indiv PI
                ON PI.PARTY_ID = P.PARTY_ID
                AND pi.party_indiv_start_dt <= ${process_dt}
                AND pi.party_indiv_end_dt > ${process_dt}
                AND pi.party_indiv_close_dt > ${process_dt}
            INNER JOIN dmsk__l0_owner__party_rltd PRL
                ON PRL.child_party_id = P.party_id
                AND PRL.party_rltd_start_dt <= ${process_dt}
                AND PRL.party_rltd_end_dt > ${process_dt}
                AND PRL.party_rltd_rsn_cd = 4
            WHERE P.PARTY_SUBTP_CD = 0
        )
        WHERE row_n = 1
        """
    )

    return df_tmp_life_assured_prep


def prepare_tmp_life_assured(
    mis__ins__pz_detail_life_person,
    df_tmp_life_assured_prep,
    spark,
):
    """
    Prepare temporary life assured dataset.

    :param mis__ins__pz_detail_life_person: Life person details dataframe
    :param df_tmp_life_assured_prep: Prepared life assured dataframe
    :param spark: Spark session
    :return: df_tmp_life_assured
    """
    mis__ins__pz_detail_life_person.createOrReplaceTempView(
        "mis__ins__pz_detail_life_person"
    )
    df_tmp_life_assured_prep.createOrReplaceTempView("rc")

    df_tmp_life_assured = spark.sql(
        """
        SELECT
            mis.CPZ,
            rc.CUSTOMERID
        FROM mis__ins__pz_detail_life_person mis
        INNER JOIN rc
            ON rc.PARTY_INDIV_RC = mis.NI_NUMBER
        WHERE mis.person_type = 'Life Assured'
        """
    )

    return df_tmp_life_assured


def prepare_tmp_life_assured_c(
    mis__ins__pz_detail_life_person,
    df_tmp_life_assured_prep,
    spark,
):
    """
    Prepare temporary life assured (company) dataset.

    :param mis__ins__pz_detail_life_person: Life person details dataframe
    :param df_tmp_life_assured_prep: Prepared life assured dataframe
    :param spark: Spark session
    :return: df_tmp_life_assured_c
    """
    mis__ins__pz_detail_life_person.createOrReplaceTempView(
        "mis__ins__pz_detail_life_person"
    )
    df_tmp_life_assured_prep.createOrReplaceTempView("rc")

    df_tmp_life_assured_c = spark.sql(
        """
        SELECT
            company.cpz,
            rc.customerid
        FROM mis__ins__pz_detail_life_person company
        INNER JOIN mis__ins__pz_detail_life_person assured
            ON assured.cpz = company.cpz
            AND assured.person_type = 'Life Assured'
        INNER JOIN rc
            ON rc.party_indiv_rc = assured.ni_number
        WHERE company.person_type = 'Payer'
            AND company.sex = 'C'
        """
    )

    return df_tmp_life_assured_c


def prepare_tmp_insurance_cont2(df_contract, feature__product_level_category,
                                df_cp_info, df_cp_osoby2, df_cp_osoby3, df_cp_osoby4,
                                df_domos, df_domos_plocha, df_domos_ps, df_finukaz, df_upom, df_pu,
                                dmsk__je69050__administrative_structure_svk, dmsk__je69050__area_code_gen, spark):
    """
    prepare_tmp_insurance_cont2
    :param df_contract\
    :param feature__product_level_category\
    :param df_cp_info\
    :param df_cp_osoby2\
    :param df_cp_osoby3\
    :param df_cp_osoby4\
    :param df_domos\
    :param df_domos_plocha\
    :param df_domos_ps\
    :param df_finukaz\
    :param df_upom\
    :param df_pu\
    :param dmsk__je69050__administrative_structure_svk\
    :param dmsk__je69050__area_code_gen\
    :param spark\
    :return df_tmp_insurance_cont2
    """
    df_contract.createOrReplaceTempView("CONTRACT")
    feature__product_level_category.createOrReplaceTempView("feature__product_level_category")
    df_cp_info.createOrReplaceTempView("CP_INFO")
    df_cp_osoby2.createOrReplaceTempView("CP_OSOBY2")
    df_cp_osoby3.createOrReplaceTempView("CP_OSOBY3")
    df_cp_osoby4.createOrReplaceTempView("CP_OSOBY4")
    df_domos.createOrReplaceTempView("DOMOS")
    df_domos_plocha.createOrReplaceTempView("DOMOS_PLOCHA")
    df_domos_ps.createOrReplaceTempView("DOMOS_PS")
    df_finukaz.createOrReplaceTempView("finukaz")
    df_upom.createOrReplaceTempView("upom")
    df_pu.createOrReplaceTempView("pu")
    dmsk__je69050__administrative_structure_svk.createOrReplaceTempView("je69050__administrative_structure_svk")
    dmsk__je69050__area_code_gen.createOrReplaceTempView("JE69050__AREA_CODE_GEN")
    df_tmp_insurance_cont2 = spark.sql("""
      select
        distinct
        INT(DATE_FORMAT(${process_dt}, 'yyyyMM')) as MONTHID
        , CONTRACT.CUSTOMERID
        , CONTRACT.CONT_ACCT
        , CONTRACT.ACTIVE
        , CONTRACT.PROD_SRC_VAL
        , case when subquery1.PROD_SRC_VAL is not null then 1 else 0 end as I_NONLIFE_INS
        , datediff(${process_dt}, CONTRACT.DT_NEGOTIATED_INS) as DAYS_FROM_AGREEMENT
        , datediff(${process_dt}, CONTRACT.DT_START_INS) as DAYS_FROM_START
        , least_oracle(datediff(CONTRACT.DT_END_INS, ${process_dt}),
                datediff(${process_dt}, ${process_dt_hist})) as DAYS_TO_END
        , datediff(${process_dt}, coalesce(CONTRACT.DT_CANCELED_INS, ${process_dt_hist})) as DAYS_FROM_CANCELED
        , case when pu.PLNENIE > 0 and TO_DATE(pu.D_REVIDACIE, 'yyyyMMdd') <= ${process_dt}
                then datediff(${process_dt}, coalesce(TO_DATE(pu.D_REVIDACIE, 'yyyyMMdd'), ${process_dt_hist}))
                 else datediff(${process_dt}, ${process_dt_hist}) end as DAYS_FROM_PAYOUT

        , CASE WHEN ( CONTRACT.DT_CANCELED_INS < CONTRACT.DT_END_INS AND
                    (add_months_oracle(${process_dt}, -1) < CONTRACT.DT_CANCELED_INS
                    and CONTRACT.DT_CANCELED_INS <= ${process_dt} )) THEN 1 ELSE 0 END as I_CANCELED

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
        , CASE WHEN prod_cat.LVL4_CATEGORY=3213 then 1 else 0 end as card_travel_ins
         --TODO tu mozno dat 0 as card_travel_ins, pretoze je tam iba jedno customerid a to banka
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
        , CASE WHEN prod_cat.LVL3_CATEGORY=3210 and prod_cat.LVL4_CATEGORY is null
                            and coalesce(datediff(CONTRACT.DT_END_INS, CONTRACT.DT_START_INS), 0) < 180
           then coalesce(datediff(least_oracle(CONTRACT.DT_END_INS,
                    ${process_dt}),  greatest_oracle(CONTRACT.DT_START_INS, add_months_oracle(${process_dt}, -1))), 0)
           else 0 end as duration_travel_ins
        , CASE WHEN CP_INFO.VARIANT in ('Stan. Family','EXC. Family') then 1 else 0 end as travel_ins_family
        , CASE WHEN CP_INFO.VARIANT in ('EXCLUSIVE','EXC. Family','Prémiový')
                then 1 else 0 end as travel_ins_exclusive
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
        , case
          when TO_DATE(pu.DAT_HLASENIA_PU, 'yyyyMMdd') between add_months_oracle(${process_dt}, -1) AND ${process_dt}
            then 1 else 0 end as HLASENIE_PU
        , case when TO_DATE(pu.D_VZNIKU_PU, 'yyyyMMdd') between add_months_oracle(${process_dt}, -1) AND ${process_dt}
            then 1 else 0 end as VZNIK_PU
        , case when TO_DATE(pu.D_REVIDACIE, 'yyyyMMdd') between add_months_oracle(${process_dt}, -1) AND ${process_dt}
            then 1 else 0 end as REVIDACIA_PU
        , case when (
           TO_DATE(pu.D_REVIDACIE, 'yyyyMMdd') between add_months_oracle(${process_dt}, -1) AND ${process_dt} )
            then nvl(pu.PLNENIE,0) else 0 end as PLNENIE_PU
        , case when (TO_DATE(pu.D_REVIDACIE, 'yyyyMMdd') between add_months_oracle(${process_dt}, -1)
                AND ${process_dt} and (pu.likvidny = 'Nelikvidny          ' or pu.plnenie = 0))
            then 1 else 0 end as NELIKVIDNA_PU
        , case when pu.typ_pu_popis in ('Škoda na vozidle spôsobená stretom vozidiel', 'Havária motorového vozidla',
                                    'Stret so zverou', 'Škoda na vozidle spôsobená padnutým-odleteným predmetom',
                                    'Poškodenie skla', 'Poistenie skiel') then 1 else 0 end as I_VEHICLE_EVENT_INS
        , case when pu.typ_pu_popis in ('Živel - stavba', 'Živel - domácnosť', 'Sklo - domácnosť', 'Sklo - stavba',
                                        'Stroj - domácnosť', 'Škoda na nehnuteľnosti', 'Stroj - stavba',
                                        'Odcudzenie - domácnosť')
            then 1 else 0 end as I_HOUSEHOLD_PROPERTY_EVENT_INS
        , case when pu.typ_pu_popis in ('Denné odškodné EUR', 'Denné odškodné',
                        'Hosp. choroby/úrazu EUR', 'Zrýchlené plnenie úrazu EUR', 'Trvalé následky')
            then 1 else 0 end as I_LIFE_EVENT_INS
        , case when subquery2.PROD_SRC_VAL is not null then 1 else 0 end as LEASING
        , case when upom.typ_upom is not null then 1 else 0 end as REMINDER
        , coalesce(upom.DLZNE_POISTNE, 0) as sum_owed
     from CONTRACT
     LEFT JOIN feature__product_level_category prod_cat
            ON prod_cat.PROD_SRC_VAL=contract.PROD_SRC_VAL
            and SRC_SYST_CD=790 --INSURANCE
     LEFT JOIN CP_INFO
            ON CONTRACT.CONT_ACCT = CP_INFO.CPZ
     LEFT JOIN CP_OSOBY2
            ON CONTRACT.CONT_ACCT= CP_OSOBY2.CPZ
     LEFT JOIN CP_OSOBY3
            ON CONTRACT.CONT_ACCT = CP_OSOBY3.CPZ
     LEFT JOIN CP_OSOBY4
            ON CONTRACT.CONT_ACCT= CP_OSOBY4.CPZ
     LEFT JOIN DOMOS
            ON CONTRACT.CONT_ACCT =  DOMOS.CPZ
    LEFT JOIN JE69050__AREA_CODE_GEN gen
            ON gen.zip=DOMOS.REGION_INS_REALITY
    LEFT JOIN je69050__administrative_structure_svk admin
            ON gen.code_municipality = admin.kod_obce
    LEFT JOIN  DOMOS_PLOCHA
            ON CONTRACT.CONT_ACCT = DOMOS_PLOCHA.CPZ
    LEFT JOIN  DOMOS_PS
            ON CONTRACT.CONT_ACCT = DOMOS_PS.CPZ
    LEFT JOIN  finukaz
            ON CONTRACT.cont_id = finukaz.cont_id
    left join  upom
        ON contract.cont_acct = upom.kod_cislo_zmluvy
    left join pu
        on pu.cpz = contract.CONT_ACCT
    LEFT JOIN feature__product_level_category subquery1
             ON subquery1.src_syst_dsc = 'INSURANCE' and subquery1.lvl2_category = 3200 and
              CONTRACT.PROD_SRC_VAL = subquery1.PROD_SRC_VAL
    LEFT JOIN feature__product_level_category subquery2
             ON lower(subquery2.PROD_DSC) like '%leasing%' and CONTRACT.PROD_SRC_VAL = subquery2.PROD_SRC_VAL
    where 1=1
        AND contract.DT_NEGOTIATED_INS < ${process_dt}
        AND contract.DT_START_INS < ${process_dt}
    """)
    return df_tmp_insurance_cont2


def prepare_c(df_stg_insurance_cont, spark):
    """
    prepare_c
    :param df_stg_insurance_cont\
    :param spark\
    :return df_c
    """
    df_stg_insurance_cont.createOrReplaceTempView("feature__stg_insurance_cont")
    df_c = spark.sql("""
     select
        insert_dt, CUSTOMERID, CONT_ID, CONT_ACCT,
        PROD_SRC_VAL,
        DT_NEGOTIATED_INS, DT_START_INS, DT_END_INS,
        DT_CANCELED_INS, PAYMENT_VALUE, INSURANCE_PREMIUM,
        CAPITAL_VALUE, ACTIVE
        from feature__stg_insurance_cont
     """)
    return df_c


def prepare_contract(df_c,
                     mis__ins__cakaren_storien_detail_dat,
                     mis__ins__pzp_storna_dat,
                     mis__ins__poistovna_zmluvy_dat, spark
                     ):
    """
    prepare_contract
    :param df_c\
    :param dmsk__l0_owner__insurance_cakajuce_stor_dat\
    :param dmsk__l0_owner__insurance_pzp_storna_dat\
    :param dmsk__l0_owner__insurance_zmluvy_dat\
    :param spark\
    :return df_contract
    """
    df_c.createOrReplaceTempView("c")
    mis__ins__cakaren_storien_detail_dat.createOrReplaceTempView("l0_owner__insurance_cakajuce_stor_dat")
    mis__ins__pzp_storna_dat.createOrReplaceTempView("l0_owner__insurance_pzp_storna_dat")
    mis__ins__poistovna_zmluvy_dat.createOrReplaceTempView("l0_owner__insurance_zmluvy_dat")
    df_contract = spark.sql("""
    select
        c.insert_dt, c.customerid, c.cont_id, c.cont_acct,
        c.prod_src_val, c.dt_negotiated_ins, c.dt_start_ins, c.dt_end_ins,
        c.payment_value, c.insurance_premium, c.capital_value,
        case when c.dt_end_ins >= ${process_dt} and (c.dt_canceled_ins is null or c.dt_canceled_ins > ${process_dt})
            then c.active else 0 end as active,
        coalesce(coalesce(coalesce(
                c.dt_canceled_ins,
                TO_DATE(pzp_storna.d_storna, 'yyyyMMdd')),
                TO_DATE(cakaren_storien.datum_storna, 'yyyyMMdd')),
                TO_DATE(ins_data.dat_storna, 'yyyyMMdd')) as dt_canceled_ins
        from c
    left join l0_owner__insurance_pzp_storna_dat pzp_storna
        on pzp_storna.kod_cislo_zmluvy=c.cont_acct
    left join l0_owner__insurance_cakajuce_stor_dat cakaren_storien
            on cakaren_storien.kod_cislo_zmluvy=c.cont_acct
    left join l0_owner__insurance_zmluvy_dat ins_data
            on ins_data.c_poj_sml=c.cont_acct
    """)
    return df_contract


def prepare_cp_osoby2(mis__ins__pz_detail_cp_osoby, spark):
    """
    prepare_cp_osoby2
    :param dmsk__l0_owner__pz_detail_cp_osoby_dat\
    :param spark\
    :return df_osoby2
    """
    mis__ins__pz_detail_cp_osoby.createOrReplaceTempView("l0_owner__pz_detail_cp_osoby_dat")
    df_osoby2 = spark.sql("""
    SELECT
        CPZ, count(*) as NUM_INSURED
    FROM (SELECT CPZ
          FROM l0_owner__pz_detail_cp_osoby_dat
          WHERE osobavztah = 'Poistený')
    GROUP BY CPZ
    """)
    return df_osoby2


def prepare_cp_osoby3(mis__ins__pz_detail_cp_osoby, spark):
    """
    prepare_cp_osoby3
    :param dmsk__l0_owner__pz_detail_cp_osoby_dat\
    :param spark\
    :return df_osoby3
    """
    mis__ins__pz_detail_cp_osoby.createOrReplaceTempView("l0_owner__pz_detail_cp_osoby_dat")
    df_osoby3 = spark.sql("""
    SELECT CPZ, count(*) as NUM_INSURED_CHILDREN,
                 sum(case when age_child < 13 then 1 else 0 end) as NUM_INSURED_SMALL_CHILDREN,
                 sum(case when age_child >= 13 then 1 else 0 end) as NUM_INSURED_TEEN_CHILDREN
                 FROM
                    (SELECT * FROM
                        (SELECT CPZ,
                            EXTRACT(year FROM (${process_dt}))-(substr(kod_RC,1,2)+1900
                                +100*(
                                CASE WHEN substr(EXTRACT(year FROM (${process_dt} )),3,4) < substr (kod_RC,1,2 )
                                    THEN 0
                                    ELSE 1
                                    END)) AS age_child
                                FROM l0_owner__pz_detail_cp_osoby_dat a
                                WHERE osobavztah = 'Poistený'
                                and length(kod_rc)=10) deti
                        WHERE age_child < 18)
                    GROUP BY CPZ
    """)
    return df_osoby3


def prepare_cp_osoby4(mis__ins__pz_detail_cp_osoby, spark):
    """
    prepare_cp_osoby4
    :param dmsk__l0_owner__pz_detail_cp_osoby_dat\
    :param spark\
    :return df_osoby4
    """
    mis__ins__pz_detail_cp_osoby.createOrReplaceTempView("l0_owner__pz_detail_cp_osoby_dat")
    df_osoby4 = spark.sql("""
    SELECT CPZ, count(*) as NUM_INSURED_SENIOR
               FROM (SELECT * FROM
                    (SELECT cpz, EXTRACT(year FROM (${process_dt}))
                               -(substr(kod_RC,1,2 )+1900+100*(
                                    CASE WHEN substr(EXTRACT(year FROM (${process_dt} )),3,4) < substr(kod_RC,1,2)
                                        THEN 0 ELSE 1 END)) as age_senior
                          FROM l0_owner__pz_detail_cp_osoby_dat
                          WHERE osobavztah='Poistený')
                    WHERE age_senior>65)
            GROUP BY CPZ
    """)
    return df_osoby4


def prepare_domos(mis__ins__pz_detail_domos_detail, spark):
    """
    prepare_domos
    :param dmsk__l0_owner__pz_detail_domos_detail_dat\
    :param spark\
    :return df_domos
    """
    mis__ins__pz_detail_domos_detail.createOrReplaceTempView("l0_owner__pz_detail_domos_detail_dat")
    df_domos = spark.sql("""
                select CPZ, HOUSE_INS_TYPE, REGION_INS_REALITY, FLG_LIABILITY, PACKAGE_TYPE, FLG_POOL
                   from l0_owner__pz_detail_domos_detail_dat
            """)
    return df_domos


def prepare_domos_plocha(mis__ins__pz_detail_domos_plochy, spark):
    """
    prepare_domos_plocha
    :param dmsk__l0_owner__pz_detail_domos_plochy_dat\
    :param spark\
    :return df_domos_plocha
    """
    mis__ins__pz_detail_domos_plochy.createOrReplaceTempView("l0_owner__pz_detail_domos_plochy_dat")
    df_domos_plocha = spark.sql("""
    SELECT *
    FROM
        (SELECT CPZ, popis, m2
         FROM l0_owner__pz_detail_domos_plochy_dat)
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
                             'Stavba - neobývaná plocha' as dom_neobyvana_m2))
            """)
    return df_domos_plocha


def prepare_domos_ps(mis__ins__pz_detail_domos_plochy, spark):
    """
    prepare_domos_ps
    :param dmsk__l0_owner__pz_detail_domos_plochy_dat\
    :param spark\
    :return df_domos_ps
    """
    mis__ins__pz_detail_domos_plochy.createOrReplaceTempView("l0_owner__pz_detail_domos_plochy_dat")
    df_domos_ps = spark.sql("""
    SELECT *
    FROM
        (SELECT CPZ, popis, m2
         FROM l0_owner__pz_detail_domos_plochy_dat)
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
                             'Stavba - neobývaná plocha' as dom_neobyvana_m2))
            """)
    return df_domos_ps


def prepare_finukaz(dmsk__l0_owner__cont, dmsk__l0_owner__cont_val, dmsk__l0_owner__cont_dt_hist, spark):
    """
    prepare_finukaz
    :param dmsk__l0_owner__cont\
    :param dmsk__l0_owner__cont_val\
    :param dmsk__l0_owner__cont_dt_hist\
    :param spark\
    :return df_finukaz
    """
    dmsk__l0_owner__cont.createOrReplaceTempView("l0_owner__cont")
    dmsk__l0_owner__cont_val.createOrReplaceTempView("l0_owner__cont_val")
    dmsk__l0_owner__cont_dt_hist.createOrReplaceTempView("l0_owner__cont_dt_hist")
    df_finukaz = spark.sql("""
    SELECT
        C.cont_id,
        MAX(CASE WHEN CV.meas_cd = 30654 THEN CV.cont_val_am ELSE NULL END) as PAYMENT_VALUE,
        MAX(CASE WHEN CV.meas_cd = 30655 THEN CV.cont_val_am ELSE NULL END) as INSURANCE_PREMIUM,
        MAX(CASE WHEN CV.meas_cd = 30656 THEN CV.cont_val_am ELSE NULL END) as CAPITAL_VALUE
    FROM l0_owner__cont C
    LEFT JOIN l0_owner__cont_val CV
        ON  CV.cont_id = C.cont_id
        AND CV.cont_val_start_dt <= ${process_dt}
        AND CV.cont_val_end_dt >  ${process_dt}
        AND CV.meas_cd IN(30654, 30655, 30656)
    LEFT JOIN l0_owner__cont_dt_hist CH
        ON  CH.cont_id = C.cont_id
        AND CH.cont_dt_start_dt <=  ${process_dt}
        AND CH.cont_dt_end_dt >  ${process_dt}
        AND CH.meas_cd IN(6022, 6023)
    WHERE C.src_syst_cd = 790 --CSOB_POJIST_SR    Systémy ČSOB Poisťovňi, a.s.  SR
        AND (C.cont_type_cd = 3 OR (C.cont_type_cd = 2 AND C.cont_single_acct_in = 'Y'))
    GROUP BY C.cont_id
    """)
    return df_finukaz


def prepare_upom(mis__ins__banka_upomienky_dat, spark):
    """
    prepare_upom
    :param dmsk__l0_owner__insurance_upomienky_dat\
    :param spark\
    :return df_upom
    """
    mis__ins__banka_upomienky_dat.createOrReplaceTempView("l0_owner__insurance_upomienky_dat")
    df_upom = spark.sql("""
    select * from l0_owner__insurance_upomienky_dat
    where TO_DATE(d_tlac_upom, 'yyyyMMdd') > add_months_oracle(${process_dt}, -1)
    and TO_DATE(d_tlac_upom, 'yyyyMMdd') <= ${process_dt}
    """)
    return df_upom


def prepare_pu(mis__ins__prieskum_spokojnosti_dat, spark):
    """
    prepare_pu
    :param dmsk__l0_owner__insurance_research_lpu_dat\
    :param spark\
    :return df_pu
    """
    mis__ins__prieskum_spokojnosti_dat.createOrReplaceTempView("l0_owner__insurance_research_lpu_dat")
    df_pu = spark.sql("""
    select * from l0_owner__insurance_research_lpu_dat
    where TO_DATE(D_VZNIKU_PU, 'yyyyMMdd') > add_months_oracle(${process_dt}, -1)
      and TO_DATE(D_VZNIKU_PU, 'yyyyMMdd') <= ${process_dt}
      and TO_DATE(D_REVIDACIE, 'yyyyMMdd') > add_months_oracle(${process_dt}, -1)
      and TO_DATE(D_REVIDACIE, 'yyyyMMdd') <= ${process_dt}
    """)
    return df_pu


def prepare_cp_info(mis__ins__pz_detail_cp_detail, spark):
    """
    prepare_cp_info
    :param dmsk__l0_owner__pz_detail_cp_detail_dat\
    :param spark\
    :return df_cp_info
    """
    mis__ins__pz_detail_cp_detail.createOrReplaceTempView("l0_owner__pz_detail_cp_detail_dat")
    df_cp_info = spark.sql("""
    SELECT distinct CPZ, INS_TERRITORY, RISK_TYPE, LUGGAGE_INS_AMT, VARIANT
    FROM l0_owner__pz_detail_cp_detail_dat
    """)
    return df_cp_info


def prepare_tmp_insurance_cont1(df_csv, feature__card, feature__product_level_category,
                                dmsk__l0_owner__prod, df_stg_insurance_cont, spark):
    """
    prepare_tmp_insurance_cont1
    :param df_csv\
    :param dmsk__l0_owner__card\
    :param feature__product_level_category\
    :param dmsk__l0_owner__prod\
    :param df_stg_insurance_cont\
    :param spark\
    :return df_tmp_insurance_cont
    """
    df_csv.createOrReplaceTempView("CSV")
    feature__card.createOrReplaceTempView("feature__card")
    feature__product_level_category.createOrReplaceTempView("feature__product_level_category")
    df_stg_insurance_cont.createOrReplaceTempView("feature__stg_insurance_cont")
    dmsk__l0_owner__prod.createOrReplaceTempView("l0_owner__prod")
    df_tmp_insurance_cont = spark.sql("""
     SELECT
           INT(DATE_FORMAT(${process_dt}, 'yyyyMM')) as MONTHID
           , K.ACCOUNT_OWNER as CUSTOMERID
           , '-1' as CONT_ACCT
           , 1 as ACTIVE
           , '301' as prod_src_val
           , 1 as I_NONLIFE_INS
           , datediff(${process_dt}, start_dt) as DAYS_FROM_AGREEMENT
           , datediff(${process_dt}, start_dt) as DAYS_FROM_START
           , datediff(end_dt, ${process_dt}) as DAYS_to_END
           , datediff(${process_dt}, ${process_dt_hist}) as DAYS_FROM_CANCELED
           , datediff(${process_dt}, ${process_dt_hist}) as DAYS_FROM_PAYOUT

           , 0 as i_canceled
           -----THIS PART IS DUE TO BE CHECKED ACCORDING ACTUAL PRICING OF PRODUCTS !!!!!!!
           , CASE WHEN ((k.prod_id_card in (select prod_id from l0_owner__prod
                                            where prod_src_val in
                                                (select prod_src_val
                                                from feature__product_level_category
                                                where lvl4_category = '1122')) and CSV.PROD_NM_CZ='CSOB Standard')
                        --OR (CSV.PROD_NM_CZ='CSOB Exclusive' and k.card_description='Debit Gold MC debetn� CL')
                        )
                      THEN 0
                  WHEN (CSV.PROD_NM_CZ='CSOB Standard' )
                      THEN (SELECT PAYMENT_VALUE
                            FROM (SELECT PAYMENT_VALUE, row_number() over (order by PAYMENT_VALUE) rn
                                  FROM feature__stg_insurance_cont
                                  WHERE prod_src_val = '301'
                                      AND DT_START_INS between add_months_oracle(${process_dt}, -1) AND ${process_dt}
                                  GROUP BY PAYMENT_VALUE)
                            WHERE rn = 1)
                 WHEN (CSV.PROD_NM_CZ='CSOB Exclusive')
                     THEN (SELECT PAYMENT_VALUE
                           FROM (SELECT PAYMENT_VALUE, row_number() over (order by PAYMENT_VALUE) rn
                                 FROM feature__stg_insurance_cont
                                 WHERE prod_src_val = '301'
                                      AND DT_START_INS between add_months_oracle(${process_dt}, -1) AND ${process_dt}
                                 GROUP BY PAYMENT_VALUE)
                           WHERE rn = 3)
                WHEN (CSV.PROD_NM_CZ='CSOB Standard family' )
                    THEN (SELECT PAYMENT_VALUE
                          FROM (SELECT PAYMENT_VALUE, row_number() over (order by PAYMENT_VALUE) rn
                                FROM feature__stg_insurance_cont
                                WHERE prod_src_val = '301'
                                      AND DT_START_INS between add_months_oracle(${process_dt}, -1) AND ${process_dt} )
                          WHERE rn = 2)
               WHEN (CSV.PROD_NM_CZ='CSOB Exclusive family')
                   THEN (SELECT PAYMENT_VALUE
                         FROM (SELECT PAYMENT_VALUE, row_number() over (order by PAYMENT_VALUE) rn
                               FROM feature__stg_insurance_cont
                               WHERE prod_src_val = '301'
                                      AND DT_START_INS between add_months_oracle(${process_dt}, -1) AND ${process_dt}
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

           FROM   feature__card  K
           JOIN  CSV
           ON k.card_id = csv.card_id
           AND k.month_id = INT(DATE_FORMAT(${process_dt}, 'yyyyMM'))
           WHERE CSV.start_dt <= ${process_dt}
           AND CSV.end_dt > ${process_dt}
    """)
    return df_tmp_insurance_cont


def prepare_csv(dmsk__l0_owner__card_svc, dmsk__l0_owner__prod,
                dmsk__l0_owner__prod_nm, dmsk__l0_owner__prod_hier, spark):
    """
    prepare_csv
    :param dmsk__l0_owner__card_svc\
    :param dmsk__l0_owner__prod\
    :param dmsk__l0_owner__prod_nm\
    :param dmsk__l0_owner__prod_hier\
    :param spark\
    :return df_csv
    """
    dmsk__l0_owner__card_svc.createOrReplaceTempView("l0_owner__card_svc")
    dmsk__l0_owner__prod.createOrReplaceTempView("l0_owner__prod")
    dmsk__l0_owner__prod_nm.createOrReplaceTempView("l0_owner__prod_nm")
    dmsk__l0_owner__prod_hier.createOrReplaceTempView("l0_owner__prod_hier")
    df_csv = spark.sql("""
     SELECT card_id, prod_id, PROD_NM_CZ, min(card_svc_start_dt) as start_dt, max(card_svc_end_dt ) as end_dt
               FROM ( SELECT card_id, CSVCNM.PROD_NM_CZ, card_svc_start_dt, card_svc_end_dt, CSVCNM.prod_id
                      FROM l0_owner__card_svc CSVC
                       JOIN l0_owner__prod CPRD
                           ON CPRD.prod_id = CSVC.prod_id
                               AND CPRD.prod_type_cd IN (16, 136)
                       JOIN l0_owner__prod_nm CSVCNM
                           ON CSVCNM.prod_id = CSVC.prod_id
                               AND CSVCNM.prod_nm_start_dt <= ${process_dt}
                               AND CSVCNM.prod_nm_end_dt > ${process_dt}
                       JOIN l0_owner__prod_hier CPH
                           ON CPH.child_prod_id = CPRD.prod_id
                               AND CPH.prod_hier_start_dt <= ${process_dt}
                               AND CPH.prod_hier_end_dt > ${process_dt}
                       JOIN l0_owner__prod_nm CSVCNM2
                           ON CSVCNM2.prod_id = CPH.prod_id
                               AND CSVCNM2.prod_nm_start_dt <= ${process_dt}
                               AND CSVCNM2.prod_nm_end_dt > ${process_dt})
               GROUP BY card_id, PROD_NM_CZ, prod_id
    """)
    return df_csv


def prepare_dtsk(dmsk__l0_owner__party, dmsk__l0_owner__party_rltd, spark):
    """
    prepare_dtsk
    :param dmsk__l0_owner__party\
    :param dmsk__l0_owner__party_rltd\
    :param spark\
    :return df_dtsk
    """
    dmsk__l0_owner__party_rltd.createOrReplaceTempView("l0_owner__party_rltd")
    dmsk__l0_owner__party.createOrReplaceTempView("l0_owner__party")
    df_dtsk = spark.sql("""
     SELECT
        PRL.party_id as CUSTOMERID,
        P.party_id,
        P.party_src_val,
        P.party_subtp_cd as typ
    FROM l0_owner__party P
    JOIN l0_owner__party_rltd PRL
        ON  PRL.child_party_id = P.party_id
        AND PRL.party_rltd_start_dt <= ${process_dt}
        AND PRL.party_rltd_end_dt > ${process_dt}
        AND PRL.party_rltd_rsn_cd = 4
    WHERE
        P.src_syst_cd = 790
      """)
    return df_dtsk


def prepare_cpz(dmsk__l0_owner__cont, dmsk__l0_owner__cont_rltd, spark):
    """
    prepare_cpz
    :param dmsk__l0_owner__cont\
    :param dmsk__l0_owner__cont_rltd\
    :param spark\
    :return df_cpz
    """
    dmsk__l0_owner__cont_rltd.createOrReplaceTempView("l0_owner__cont_rltd")
    dmsk__l0_owner__cont.createOrReplaceTempView("l0_owner__cont")
    df_cpz = spark.sql("""
     SELECT
        CRL.cont_id,
        C.cont_acct
    FROM l0_owner__cont_rltd CRL
    JOIN l0_owner__cont C
        ON  C.cont_id = CRL.child_cont_id
        AND C.src_syst_cd = 790
    WHERE
        CRL.etl_src_syst_cd = 790
        AND CRL.cont_rltd_rsn_cd = 71
    GROUP BY
        CRL.cont_id,
        C.cont_acct
      """)
    return df_cpz


def prepare_cont_stav(dmsk__l0_owner__cont_stat_type_gen, spark):
    """
    prepare_cont_stav
    :param dmsk__l0_owner__cont_stat_type_gen\
    :param spark\
    :return df_cont_stav
    """
    dmsk__l0_owner__cont_stat_type_gen.createOrReplaceTempView("l0_owner__cont_stat_type_gen")
    df_cont_stav = spark.sql("""
    SELECT
        cont_stat_type_cd,
        cont_stat_type_ds,
        cont_stat_type_gen_start_dt,
        CASE
            WHEN CSTG.cont_stat_type_ds NOT IN (
                                                'Storno pojistky',
                                                'Pojistka nebyla ještě verifikována'
                                                'Pojistka je chybná a nebyla ještě verifikován'
                                                'Pojistka je vrácena z verifikace k opravě'
                                               ) THEN 1
            ELSE 0
        END as OTVORENA
    FROM l0_owner__cont_stat_type_gen CSTG
    WHERE
        CSTG.cont_stat_type_gen_start_dt <= ${process_dt}
        AND CSTG.cont_stat_type_gen_end_dt > ${process_dt}
    """)
    return df_cont_stav


def prepare_ukaz1(dmsk__l0_owner__cont, dmsk__l0_owner__cont_val, dmsk__l0_owner__cont_dt_hist, spark):
    """
    prepare_ukaz1
    :param dmsk__l0_owner__cont\
    :param dmsk__l0_owner__cont_val\
    :param dmsk__l0_owner__cont_dt_hist\
    :param spark\
    :return df_ukaz1
    """
    dmsk__l0_owner__cont.createOrReplaceTempView("l0_owner__cont")
    dmsk__l0_owner__cont_val.createOrReplaceTempView("l0_owner__cont_val")
    dmsk__l0_owner__cont_dt_hist.createOrReplaceTempView("l0_owner__cont_dt_hist")
    df_ukaz1 = spark.sql("""
        SELECT
        C.cont_id,
        MAX(CASE WHEN CV.meas_cd = 30654 THEN CV.cont_val_am ELSE NULL END) as SPLATKA_VYSKA,
        MAX(CASE WHEN CV.meas_cd = 30655 THEN CV.cont_val_am ELSE NULL END) as POISTNE,
        MAX(CASE WHEN CV.meas_cd = 30656 THEN CV.cont_val_am ELSE NULL END) as KAPITALOVA_HODNOTA,
        MAX(CASE WHEN CH.meas_cd = 6023 THEN CH.cont_dt_hist_dt ELSE NULL END) as DT_VLOZENIA_KONTR_POIST,
        MAX(CASE WHEN CH.meas_cd = 6022 THEN CH.cont_dt_hist_dt ELSE NULL END) as DT_PRVEJ_PLATBY
    FROM l0_owner__cont C
    LEFT JOIN l0_owner__cont_val CV
        ON  CV.cont_id = C.cont_id
        AND CV.cont_val_start_dt <= ${process_dt}
        AND CV.cont_val_end_dt > ${process_dt}
        AND CV.meas_cd IN(30654, 30655, 30656)
    LEFT JOIN l0_owner__cont_dt_hist CH
        ON  CH.cont_id = C.cont_id
        AND CH.cont_dt_start_dt <= ${process_dt}
        AND CH.cont_dt_end_dt > ${process_dt}
        AND CH.meas_cd IN(6022, 6023)
    WHERE
        (C.cont_type_cd = 3 OR (C.cont_type_cd = 2 AND C.cont_single_acct_in = 'Y'))
        AND C.src_syst_cd = 790
    GROUP BY
        C.cont_id
    """)
    return df_ukaz1


def prepare_stg_insurance_cont(dmsk__l0_owner__party, df_dtsk, dmsk__l0_owner__party_cont,
                               dmsk__l0_owner__cont, df_cpz, dmsk__l0_owner__cont_gen,
                               dmsk__l0_owner__cont_prod, dmsk__l0_owner__prod, dmsk__l0_owner__cont_stat,
                               df_cont_stav, dmsk__l0_owner__cont_dt_actl, df_ukaz1, spark):
    """
    prepare_stg_insurance_cont
    :param dmsk__l0_owner__party\
    :param df_dtsk\
    :param dmsk__l0_owner__party_cont\
    :param dmsk__l0_owner__cont\
    :param df_cpz\
    :param dmsk__l0_owner__cont_gen\
    :param dmsk__l0_owner__cont_prod\
    :param dmsk__l0_owner__prod\
    :param dmsk__l0_owner__cont_stat\
    :param df_cont_stav\
    :param dmsk__l0_owner__cont_dt_actl\
    :param df_ukaz1\
    :param spark\
    :return df_attr_insurance
    """
    dmsk__l0_owner__party.createOrReplaceTempView("l0_owner__party")
    df_dtsk.createOrReplaceTempView("DTSK")
    dmsk__l0_owner__party_cont.createOrReplaceTempView("l0_owner__party_cont")
    dmsk__l0_owner__cont.createOrReplaceTempView("l0_owner__cont")
    df_cpz.createOrReplaceTempView("CPZ")
    dmsk__l0_owner__cont_gen.createOrReplaceTempView("l0_owner__cont_gen")
    dmsk__l0_owner__cont_prod.createOrReplaceTempView("l0_owner__cont_prod")
    dmsk__l0_owner__prod.createOrReplaceTempView("l0_owner__prod")
    dmsk__l0_owner__cont_stat.createOrReplaceTempView("l0_owner__cont_stat")
    df_cont_stav.createOrReplaceTempView("CONT_STAV")
    dmsk__l0_owner__cont_dt_actl.createOrReplaceTempView("l0_owner__cont_dt_actl")
    df_ukaz1.createOrReplaceTempView("UKAZ1")
    df_stg_insurance_cont = spark.sql("""
    SELECT
        ${process_dt}                              AS insert_dt,
        DTSK.CUSTOMERID,
        C.cont_id,
        COALESCE(CPZ.cont_acct, C.cont_acct)    AS CONT_ACCT,
        PID.prod_src_val,
        CONT_STAV.OTVORENA                      AS ACTIVE,
        CONT_STAV.cont_stat_type_ds             AS STATUS,
        case when CDA.cont_dt_actl_dt < least_oracle(${process_dt}, CG.cont_gen_matur_dt)
             then CDA.cont_dt_actl_dt else null end AS DT_CANCELED_INS,
        CG.cont_gen_sign_dt                     AS DT_NEGOTIATED_INS,
        CG.cont_gen_open_dt                     AS DT_START_INS,
        CG.cont_gen_matur_dt                    AS DT_END_INS,
        UKAZ1.SPLATKA_VYSKA                     AS PAYMENT_VALUE,
        UKAZ1.POISTNE                           AS INSURANCE_PREMIUM,
        coalesce(UKAZ1.KAPITALOVA_HODNOTA, 0)   AS CAPITAL_VALUE,
        UKAZ1.DT_PRVEJ_PLATBY                   AS DT_FIRST_PAYMENT,
        DTSK.TYP
    FROM l0_owner__party P
    JOIN  DTSK
        ON  DTSK.party_id = P.party_id
    JOIN l0_owner__party_cont PC
        ON  PC.party_id = P.party_id
        AND PC.party_role_cd = 168 -- Klient Poisťovni SR
        AND PC.party_cont_start_dt <= ${process_dt}
        AND PC.party_cont_end_dt > ${process_dt}
    JOIN l0_owner__cont C
        ON  C.cont_id = PC.cont_id
        AND (C.cont_type_cd = 3 OR (C.cont_type_cd = 2 AND C.cont_single_acct_in = 'Y'))
        AND C.src_syst_cd = 790
    LEFT JOIN CPZ
        ON  CPZ.cont_id = C.cont_id
    JOIN l0_owner__cont_gen CG
        ON  CG.cont_id = C.cont_id
        AND CG.cont_gen_start_dt <= ${process_dt}
        AND CG.cont_gen_end_dt > ${process_dt}
    JOIN l0_owner__cont_prod PID_C
        ON  PID_C.cont_id = C.cont_id
        AND PID_C.cont_prod_start_dt <= ${process_dt}
        AND PID_C.cont_prod_end_dt > ${process_dt}
    JOIN l0_owner__prod PID
        ON  PID.prod_id = PID_C.prod_id
        AND PID.src_syst_cd = 790
    JOIN l0_owner__cont_stat CONT_STAT
        ON  CONT_STAT.cont_id = C.cont_id
        AND CONT_STAT.cont_stat_start_dt <= ${process_dt}
        AND CONT_STAT.cont_stat_end_dt > ${process_dt}
    JOIN CONT_STAV
        ON  CONT_STAV.cont_stat_type_cd = CONT_STAT.cont_stat_type_cd
    LEFT JOIN l0_owner__cont_dt_actl CDA
        ON  CDA.cont_id = C.cont_id
        AND CDA.meas_cd = 8016
    LEFT JOIN UKAZ1
        ON  UKAZ1.cont_id = C.cont_id
    where (case when CONT_STAV.cont_stat_type_ds = 'Storno pojistky'
           and (CONT_STAT.cont_stat_start_dt < ${process_dt_hist}
                or CG.cont_gen_matur_dt < ${process_dt_hist}) then 0 else 1 end) = 1
     """)
    # workaround protect, unprotected substring cont_acct
    df_stg_insurance_cont.persist()
    df_stg_insurance_cont.count()
    df_stg_insurance_cont = df_stg_insurance_cont.unprotect(
        'CONT_ACCT', reason='processing', policy='de_DSK_AccountNumber')
    df_stg_insurance_cont = df_stg_insurance_cont.withColumn(
        'cont_acct',
        F.when(F.instr("CONT_ACCT", "-") == 0, F.col("CONT_ACCT"))
        .when(F.instr("CONT_ACCT", "-") == 4, F.regexp_replace("CONT_ACCT", "-", ""))
        .when(F.instr("CONT_ACCT", "-").isin(10, 11), F.substring_index("CONT_ACCT", "-", 1))
        .otherwise(F.col('CONT_ACCT'))
    )
    df_stg_insurance_cont = df_stg_insurance_cont.protect(
        'cont_acct', reason='processing', policy='de_DSK_AccountNumber').persist()
    df_stg_insurance_cont.count()
    return df_stg_insurance_cont
