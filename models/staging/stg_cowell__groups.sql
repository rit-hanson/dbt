
With ordered_seats AS (

	SELECT * FROM {{ ref('stg_cowell__seats')}}
),
Groups AS (
		SELECT
			TRGRUP.GRUP_CD,
			MGRUP_CD,
			GRUP_SNM,
			OPEN_DL,
			GCTRL_EMP, -- 團控人
			GAREA_CD,
			BITN_CD,
			SUB_CD,
			OBJ_QT,	-- 預定成團人數
			(ESTM_YQT + ESTM_CQT + ESTM_FQT + ESTM_EDQT)-
			(FOC1_CQT + FOC1_EDQT + FOC1_FQT + FOC1_YQT + FOC2_CQT + FOC2_EDQT + FOC2_FQT +
			FOC2_YQT + KEEP_CQT + KEEP_EDQT + KEEP_FQT + KEEP_YQT)-COALESCE(bt.ORDERED_SEATS,0) AS RESERVABLE_SEATS,
			COALESCE(bt.ORDERED_SEATS,0) AS SOLD_SEATS,
			g.PAX_COUNT,
			TRGRUP.BUD_DTM,
			TRGRUP.ORDER_DL,
			TRGRUP.LEAV_DT,
			TRGRUP.GRUP_LN,
			TRGRUP.ITN_CITY,
			TRGRUP.WEB_PD
		FROM TRGRUP
		LEFT JOIN ordered_seats bt ON bt.GRUP_CD = TRGRUP.GRUP_CD
		LEFT JOIN (
			-- 計算所有參團的人
			SELECT GRUP_CD,COUNT(*) AS PAX_COUNT
			FROM  TRGPAX 
			WHERE TRGPAX.GRUP_ST = '2'
			GROUP BY TRGPAX.GRUP_CD 
		) AS g on g.GRUP_CD = TRGRUP.GRUP_CD

		WHERE TRGRUP.LEAV_DT BETWEEN '{{var("train_start")}}' AND '{{var("train_end")}}'
		AND TRGRUP.SUB_CD = 'GO'
		AND TRGRUP.GRUP_TP <> '1'
)

select * from Groups