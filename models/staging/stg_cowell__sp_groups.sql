With
TL_history AS (
    -- 查看是否一個旅客過去曾擔任領隊, 需要把此類人視為領隊才不會影響特報團判斷
    SELECT
        DISTINCT PAX_CD
    FROM TRGPAX tp
    WHERE TLDR_FG = TRUE
    GROUP BY PAX_CD
),
Attending_times AS (

    SELECT GRUP_CD, ACCT_NO, COUNT(ACCT_NO) AS TIMES FROM TRGPAX
    GROUP BY GRUP_CD,ACCT_NO
),
Passengers AS (
    SELECT
        TRGPAX.GRUP_CD,
        CASE
            WHEN COUNT(DISTINCT TRGPAX.ACCT_NO)=1 THEN 'Y'
            WHEN COUNT(CASE WHEN at.times <=5 then 1 END)*100.0/COUNT(*)<=10 THEN 'Y'
            ELSE 'N' END AS SPECIAL,
        CASE WHEN MAX(TRGPAX.JOIN_TP) <> '3' THEN 'Y' ELSE 'N' END AS JOIN_TP_CONFIRM,
        COUNT(*) AS PEOPLE,
        COUNT(CASE WHEN at.times <=5 then 1 END)*100.0/COUNT(*) AS PERC

    FROM TRGPAX

    LEFT JOIN TRGRUP tg on tg.GRUP_CD =TRGPAX.GRUP_CD
    LEFT JOIN Attending_times at on at.ACCT_NO = TRGPAX.ACCT_NO AND  at.GRUP_CD = TRGPAX.GRUP_CD

    WHERE
        TLDR_FG = '0'
        AND GRUP_ST = '2' --會有不參團的導致ACCT_NO有大於兩個以上的特報團
        AND NOT EXISTS (SELECT PAX_CD FROM TL_history Where TL_history.PAX_CD = TRGPAX.PAX_CD AND TRGPAX.PAX_CD <> '')

    GROUP BY TRGPAX.GRUP_CD),

final as (
    SELECT 
        p.GRUP_CD,
        GRUP_SNM,
        JOIN_TP_CONFIRM,
        SPECIAL,
        LEAV_DT,
        p.PERC,
        trgrup.OBJ_QT,
        CASE WHEN JOIN_TP_CONFIRM ='Y' AND SPECIAL ='Y' AND PERC<=10 AND OBJ_QT>=6 THEN 1 ELSE 0 END AS special_mark

    FROM TRGRUP --僅有當JOIN_TP_CONFIRM = Y AND SPECIAL = Y 才算特報
    LEFT JOIN Passengers p on  p.GRUP_CD = TRGRUP.GRUP_CD
    WHERE TRGRUP.TKT_FG <> '1'  AND TRGRUP.LEAV_DT BETWEEN '{{var("train_start")}}' AND '{{var("train_end")}}'
)

select * from final