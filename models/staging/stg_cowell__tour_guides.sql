With tour_guides AS (
	-- 為了防止一個團有兩個領隊(不確定會不會發生), 因此只選擇該團的第一個領隊
    SELECT GRUP_CD, PAX_CNM, ROW_NUMBER() OVER (PARTITION BY GRUP_CD ORDER BY PAX_CNM) AS rn
    FROM TRGPAX
    WHERE TLDR_FG = TRUE
)

select * from tour_guides