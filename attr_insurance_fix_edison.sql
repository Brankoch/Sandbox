-- DMSK
INSERT INTO PM_OWNER.tmp_life_assured (
	SELECT CPZ, CUSTOMERID
	FROM L0_OWNER.PZ_DETAIL_LIFE_PERSON_DAT
	WHERE PERSON_TYPE = 'Life Assured'
	AND CUSTOMERID IS NOT NULL
);
		
-- Edison
	SELECT
		mis.CPZ,
		RC.CUSTOMERID 
	FROM L0_OWNER.PZ_DETAIL_LIFE_PERSON_DAT mis
		
	JOIN (
		SELECT
			PRL.PARTY_ID CUSTOMERID,
			PI.PARTY_INDIV_RC,
			ROW_NUMBER() OVER(PARTITION BY PI.PARTY_INDIV_RC ORDER BY SRC_SYST_CD ASC, party_indiv_start_dt DESC) row_n
		FROM L0_OWNER.PARTY P
		JOIN l0_owner.party_indiv PI
			ON PI.PARTY_ID = P.PARTY_ID
			and party_indiv_start_dt < sysdate
			and party_indiv_end_dt > sysdate
			AND party_indiv_close_dt > sysdate
		JOIN L0_OWNER.PARTY_RLTD PRL
			ON  PRL.child_party_id = P.party_id
			AND PRL.party_rltd_start_dt <= sysdate
			AND PRL.party_rltd_end_dt > sysdate
			AND PRL.party_rltd_rsn_cd = 4
		WHERE 1=1
			AND P.PARTY_SUBTP_CD  = 0
		) RC
		ON to_char(RC.PARTY_INDIV_RC) = to_char(asu.NI_NUMBER)
		AND RC.row_n = 1
		
	where 1=1
	and mis.person_type = 'Life Assured'
;

------------------------------------------------------
-- DMSK
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

-- Edison
	SELECT
		COMPANY.CPZ,
		RC.CUSTOMERID 
	FROM L0_OWNER.PZ_DETAIL_LIFE_PERSON_DAT COMPANY
	JOIN L0_OWNER.PZ_DETAIL_LIFE_PERSON_DAT ASSURED
		ON COMPANY.CPZ = ASSURED.CPZ	
	JOIN (
		SELECT
			PRL.PARTY_ID CUSTOMERID,
			PI.PARTY_INDIV_RC,
			ROW_NUMBER() OVER(PARTITION BY PI.PARTY_INDIV_RC ORDER BY SRC_SYST_CD ASC, party_indiv_start_dt DESC) row_n
		FROM L0_OWNER.PARTY P
		JOIN l0_owner.party_indiv PI
			ON PI.PARTY_ID = P.PARTY_ID
			and party_indiv_start_dt < sysdate
			and party_indiv_end_dt > sysdate
			AND party_indiv_close_dt > sysdate
		JOIN L0_OWNER.PARTY_RLTD PRL
			ON  PRL.child_party_id = P.party_id
			AND PRL.party_rltd_start_dt <= sysdate
			AND PRL.party_rltd_end_dt > sysdate
			AND PRL.party_rltd_rsn_cd = 4
		WHERE 1=1
			AND P.PARTY_SUBTP_CD  = 0
		) RC
		ON to_char(RC.PARTY_INDIV_RC) = to_char(ASSURED.NI_NUMBER)
		AND RC.row_n = 1
		
	WHERE COMPANY.PERSON_TYPE = 'Payer' 
	AND COMPANY.SEX = 'C'
	AND ASSURED.PERSON_TYPE = 'Life Assured'