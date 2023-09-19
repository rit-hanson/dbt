With Pax_log AS (
    SELECT
        tlog.*,
        LAG(TLOG.UPD_DTM) OVER (PARTITION BY TLOG.OP_SQ ORDER BY tlog.UPD_DTM) AS LAST_STATUS_DTM,
        ROW_NUMBER() OVER (PARTITION BY TLOG.OP_SQ ORDER BY tlog.UPD_DTM DESC) AS row_num,
        COALESCE(LAG(TLOG.GRUP_ST) OVER (PARTITION BY TLOG.OP_SQ ORDER BY tlog.UPD_DTM),-1) AS LAST_STATUS
    FROM TRGPAXLOG tlog

)

SELECT
	bg.GRUP_CD AS 'group_id'
	,bg.MGRUP_CD as 'mother_group'
	,bg.OBJ_QT
	,COALESCE(tg.PAX_CNM,'TBD') as 'team_leader'
    ,TRGPAX.OP_SQ AS 'operation_sequence'
	,TRGPAX.PAX_SQ as 'passenger_sequence'
    ,CASE TRREC.ACCT_NO_A
    WHEN '' THEN 0
    ELSE 1
    END AS 'agency'
    ,CASE
		WHEN MAX(CAST(h.TL_TIMES AS INT)) OVER (PARTITION BY TRGPAX.OP_SQ) > 0 THEN 'TEAM_LEADER'
		WHEN TRREC.SALE_SU LIKE '%務招%' OR TRREC.SALE_SU LIKE '(3)%' OR TRREC.SALE_SU LIKE '%B2C%' THEN 'SALES'
		WHEN TRREC.SALE_SU LIKE '%B2B%' OR (TRREC.SALE_SU='網際網路' AND TRREC.ACCT_NO LIKE 'A%') THEN 'PEERS_ONLINE'
		WHEN TRREC.SALE_SU LIKE '%同業介紹%' THEN 'PEERS_OFFLINE'
		WHEN TRREC.SALE_SU= '網際網路' AND TRREC.ACCT_NO LIKE 'D%' THEN 'PERSONNEL_ONLINE'
		WHEN TRREC.SALE_SU='旅展' THEN 'TOUR'
		WHEN TRREC.SALE_SU='商務客戶' OR TRREC.SALE_SU LIKE '%B2E%' THEN 'B2E'
		WHEN TRREC.SALE_SU='自由行' THEN 'FIT'
		ELSE 'OTHER_SOURCE'
    END AS 'sale_source'
	,COALESCE(bg.PAX_COUNT,0) as 'confirmed'
	,bg.OPEN_DL as 'open_deadline'
	,bg.RESERVABLE_SEATS as 'reservable_seats'
	,bg.SOLD_SEATS as 'sold_seats'
	,bg.WEB_PD as 'website_product'
	,TRGPAX.TLDR_FG as 'team_leader_flag'
	,CASE WHEN TRGPAX.PAX_CD IS NULL OR TRGPAX.PAX_CD = '' THEN 'TBD' ELSE TRGPAX.PAX_CD END AS 'passenger_id'
	,ISNULL(TRPAX.BRTH_DT,CAST('1911-01-01' AS DATE)) AS 'birthday'
	,TRPAX.PAX_SEX as 'sex'
	,TRGPAX.GAR_AM as 'revenue_per_person'
	,TRGPAX.BED_TP as 'bed_type'
	,COALESCE(h.ATTENDING_TIMES,0) as 'attending_times'
	,COALESCE(h.TL_TIMES,0) as 'team_leader_times'
	,COALESCE(h.GAR_AM/h.ATTENDING_TIMES,0) AS 'averaged_contribution'
	,TRREC.ACCT_DR as 'account_description'
    ,TRGPAX.JOIN_TP as 'join_type'
    ,TRGPAX.JOIN_DT as 'join_date'
	,TRGPAX.GRUP_ST AS 'group_status'
    ,TRREC.GPAX_QT AS 'init_head'
    ,TRREC.GPAX_QT2 AS 'confirm_head'
    ,TRREC.GPAX_QT6 AS 'cancel_head'
    ,TRREC.RGST_DT AS 'register_date'
    ,TRGPAX.DORD_DT AS 'deposit_date'
    ,CASE WHEN TRGPAX.UPD_DTM >= bg.LEAV_DT THEN plog.UPD_DTM ELSE TRGPAX.UPD_DTM END  AS 'last_update'
    ,bg.BUD_DTM AS 'group_order_start'
    ,bg.ORDER_DL AS 'group_order_deadline'
    ,bg.LEAV_DT AS 'group_leave'
	,bg.GRUP_LN-1 AS 'days'
	,bg.ITN_CITY as 'city'
	,DATEADD(DAY,bg.GRUP_LN-1,bg.LEAV_DT) AS 'back_date'
    ,bg.OBJ_QT AS 'group_objective_head'
    ,line.GAREA_NM  as 'lines'
    ,series.CHIN_WD as 'series'
	,COALESCE(p.DIRECT_CUSTOMER_PRICE,0) AS 'direct_customer_price' --AS '直客價',
	,COALESCE(p.B2B_MEM_PRICE,0) AS 'B2B_member_price'--AS 'B2B會員價',
	,COALESCE(p.B2C_MEM_PRICE,0) AS 'B2C_member_price' --AS 'B2C會員價',
	,COALESCE(p.AGT_PRICE,0) AS 'agency_price' --AS '同業價',
	,COALESCE(p.ENTERPRISE_PRICE,0) AS 'enterprise_price' --AS '企業價'
	,emp.EMP_CNM as 'sales'
	,emp_group.EMP_CNM AS 'group_controller'
	,COALESCE(plog.LAST_STATUS,-1) as last_status
	,plog.LAST_STATUS_DTM as last_status_date
    ,plog.UPD_DTM as 'cur_update_time'
	, CASE WHEN TRREC.ACCT_DR = TRGPAX.PAX_CNM THEN 1 ELSE 0 END AS payer
FROM {{ref('stg_cowell__groups')}} bg

LEFT JOIN TRGPAX on bg.GRUP_CD = TRGPAX.GRUP_CD
LEFT JOIN TRREC ON TRREC.OP_SQ = TRGPAX.OP_SQ
LEFT JOIN TRGAREA line ON line.GAREA_CD = bg.GAREA_CD
LEFT JOIN TRWORD series ON series.DATA_VALUE = bg.BITN_CD AND series.CLS_CD='BITN_GO'
LEFT JOIN {{ref('stg_cowell__prices')}} p on p.GRUP_CD = TRGPAX.GRUP_CD
LEFT JOIN {{ref('stg_cowell__tour_guides')}} tg on tg.GRUP_CD = TRGPAX.GRUP_CD
LEFT JOIN TREMP emp ON  emp.EMP_CD = trgpax.EMP_CD
LEFT JOIN TREMP emp_group on emp_group.EMP_CD = bg.GCTRL_EMP
LEFT JOIN {{ref('stg_cowell__pax_histories')}} h on h.PAX_CD = TRGPAX.PAX_CD
LEFT JOIN TRPAX ON TRPAX.PAX_CD = TRGPAX.PAX_CD
LEFT JOIN Pax_log plog on TRGPAX.OP_SQ = plog.OP_SQ 
WHERE plog.row_num = '1'