INSERT INTO DM_LOGISTICS_METRICS.METRICS_VARIABILITY
(
       sales_channel,
       business_unit,
       shipment_point_code,
       delivery_point,
       week,
       "month",
       "year",
       total_week_variability,
       total_month_variability,
       sales_channel_week_variability,
       sales_channel_month_variability,
       business_unit_week_variability,
       business_unit_month_variability,
       shipment_point_week_variability,
       shipment_point_month_variability,
       delivery_point_week_variability,
       delivery_point_month_variability,
       all_business_unit_week_variability,
       all_business_unit_month_variability,
       all_shipment_point_week_variability,
       all_shipment_point_month_variability,
       all_delivery_point_week_variability,
       all_delivery_point_month_variability,
       sc_bu_dest_week_variability,
       sc_bu_dest_month_variability,
       sc_dest_week_variability,
       sc_dest_month_variability,
       bu_dest_week_variability,
       bu_dest_month_variability,
       dest_week_variability,
       dest_month_variability
)
WITH /*+ENABLE_WITH_CLAUSE_MATERIALIZATION */ VTTK_ACTUAL AS
       (SELECT *
       FROM
             (SELECT *,
                    ROW_NUMBER() OVER (
                           PARTITION BY
                                  mandt,
                                  tknum
                           ORDER BY tech_load_ts DESC
                    ) AS rn
             FROM ODS_SAPERP.VTTK) A
       WHERE A.rn = 1
             AND A.tech_is_deleted = 0),
--
VTTP_ACTUAL AS
       (SELECT *
       FROM
             (SELECT *,
                    ROW_NUMBER() OVER (
                           PARTITION BY
                                  mandt,
                                  tknum,
                                  tpnum
                           ORDER BY tech_load_ts DESC
                    ) AS rn
             FROM ODS_SAPERP.VTTP) A
       WHERE A.rn = 1
             AND A.tech_is_deleted = 0
             ),
--
LIKP_ACTUAL AS
       (SELECT *
       FROM
             (SELECT *,
                    ROW_NUMBER() OVER (
                           PARTITION BY
                                  vbeln
                           ORDER BY tech_load_ts DESC
                    ) AS rn
             FROM ODS_SAPERP.LIKP) A
       WHERE A.rn = 1
             AND A.tech_is_deleted = 0),
--
OTIF_DELIVERY_ACTUAL AS
       (SELECT *
       FROM
             (SELECT *,
                    ROW_NUMBER() OVER (
                           PARTITION BY
                                  SHIPMENT_NUM
                           ORDER BY TECH_LOAD_TS DESC
                    ) AS rn
             FROM ODS_XLS_LOGISTICSMETRICS.OTIF_DELIVERY) A
       WHERE A.rn = 1),
--
VTTS_ACTUAL AS
       (SELECT *
       FROM
             (SELECT *,
                    ROW_NUMBER() OVER (
                           PARTITION BY
                                  mandt,
                                  tknum
                           ORDER BY tech_load_ts DESC, tstyp DESC
                    ) AS rn
             FROM ODS_SAPERP.VTTS
             WHERE tstyp <= 2) A
       WHERE A.rn = 1
             AND A.tech_is_deleted = 0
             ),
--
DM_CURRENT AS
       (SELECT *
       FROM
             (SELECT *,
                    ROW_NUMBER() OVER (
                           PARTITION BY
                                  sales_channel,
                                  business_unit,
                                  shipment_point_code,
                                  delivery_point,
                                  "year",
                                  "month",
                                  week
                           ORDER BY TECH_LOAD_TS DESC
                    ) AS rn
             FROM DM_LOGISTICS_METRICS.METRICS_VARIABILITY) A
       WHERE A.rn = 1),
--
TECH_1 AS
       (SELECT
             L.wadat AS shipment_date_plan,
             CASE
                    WHEN K.zzmatrix = '03'
                           THEN 'LastMile'
                    WHEN K.zzmatrix = '02' OR K.zzmatrix = '05'
                           THEN 'Экспорт'
                    WHEN K.zzmatrix = '01' OR K.zzmatrix = '04'
                           THEN CASE
                                        WHEN L.vkorg = '9999'
                                               THEN 'Внутренние перемещения РФ'
                                        ELSE 'РФ'
                                  END
                    ELSE 'NO DATA'
             END AS sales_channel,
             CASE
                    WHEN B.business_unit_name IS NOT NULL
                           THEN B.business_unit_name
                    ELSE CASE
                                  WHEN L.vkorg = '9002' OR L.vkorg = '4102'
                                        THEN 'ДСК'
                                  WHEN L.vkorg = '9003' OR L.vkorg = '4103'
                                        THEN 'ДПиОС'
                                  WHEN L.vkorg = '9004' OR L.vkorg = '4104'
                                        THEN 'ДБП'
                                  ELSE 'NO DATA'
                           END
             END AS business_unit,
             CASE
                    WHEN (S.vstel = '' OR S.vstel IS NULL)
                           THEN 'NO DATA'
                    ELSE S.vstel
             END AS shipment_point_code,
             CASE
                    WHEN (D.delivery_point_dashboard_name = '' OR D.delivery_point_dashboard_name IS NULL)
                           THEN 'NO DATA'
                    ELSE D.delivery_point_dashboard_name
             END AS delivery_point_name,
             COUNT(K.tknum) AS day_transportation_quantity
       FROM VTTK_ACTUAL K
             LEFT JOIN VTTP_ACTUAL P ON K.tknum = P.tknum AND P.tpnum = 1
             LEFT JOIN LIKP_ACTUAL L ON P.vbeln = L.vbeln
             LEFT JOIN VTTS_ACTUAL S ON K.tknum = S.tknum
             LEFT JOIN DM_LOGISTICS_METRICS.DELIVERY_POINT_REF D ON K.add02 = D.delivery_point_code
             LEFT JOIN DM_LOGISTICS_METRICS.BUSINESS_UNIT_REF B ON L.vkorg = B.business_unit_code AND S.vstel = B.shipping_point_code
       WHERE L.wadat IS NOT NULL
       GROUP BY L.wadat,
             CASE
                    WHEN K.zzmatrix = '03'
                           THEN 'LastMile'
                    WHEN K.zzmatrix = '02' OR K.zzmatrix = '05'
                           THEN 'Экспорт'
                    WHEN K.zzmatrix = '01' OR K.zzmatrix = '04'
                           THEN CASE
                                        WHEN L.vkorg = '9999'
                                               THEN 'Внутренние перемещения РФ'
                                        ELSE 'РФ'
                                  END
                    ELSE 'NO DATA'
             END,
             CASE
                    WHEN B.business_unit_name IS NOT NULL
                           THEN B.business_unit_name
                    ELSE CASE
                                  WHEN L.vkorg = '9002' OR L.vkorg = '4102'
                                        THEN 'ДСК'
                                  WHEN L.vkorg = '9003' OR L.vkorg = '4103'
                                        THEN 'ДПиОС'
                                  WHEN L.vkorg = '9004' OR L.vkorg = '4104'
                                        THEN 'ДБП'
                                  ELSE 'NO DATA'
                           END
             END,
             CASE
                    WHEN (S.vstel = '' OR S.vstel IS NULL)
                           THEN 'NO DATA'
                    ELSE S.vstel
             END,
             CASE
                    WHEN (D.delivery_point_dashboard_name = '' OR D.delivery_point_dashboard_name IS NULL)
                           THEN 'NO DATA'
                    ELSE D.delivery_point_dashboard_name
             END),
--
TECH_2 AS
       (SELECT T.*,
             ((YEAR_ISO(T.shipment_date_plan))||(WEEK_ISO(T.shipment_date_plan))) AS week_no,
             MONTH(T.shipment_date_plan) AS month_no,
             YEAR(T.shipment_date_plan) AS year_no,
             CASE
                    WHEN MONTH(T.shipment_date_plan) IN (1, 3, 5, 7, 8, 10, 12)
                           THEN CASE
                                        WHEN DAY(T.shipment_date_plan) BETWEEN 4 AND 10
                                               THEN 1
                                        WHEN DAY(T.shipment_date_plan) BETWEEN 11 AND 17
                                               THEN 2
                                        WHEN DAY(T.shipment_date_plan) BETWEEN 18 AND 24
                                               THEN 3
                                        WHEN DAY(T.shipment_date_plan) BETWEEN 25 AND 31
                                               THEN 4
                                        ELSE 0
                                  END
                    WHEN MONTH(T.shipment_date_plan) IN (4, 6, 9, 11)
                           THEN CASE
                                        WHEN DAY(T.shipment_date_plan) BETWEEN 3 AND 9
                                               THEN 1
                                        WHEN DAY(T.shipment_date_plan) BETWEEN 10 AND 16
                                               THEN 2
                                        WHEN DAY(T.shipment_date_plan) BETWEEN 17 AND 23
                                               THEN 3
                                        WHEN DAY(T.shipment_date_plan) BETWEEN 24 AND 30
                                               THEN 4
                                        ELSE 0
                                  END
                    WHEN MONTH(T.shipment_date_plan) = 2
                           THEN CASE
                                        WHEN ((MOD(YEAR(T.shipment_date_plan),4) = 0 AND MOD(YEAR(T.shipment_date_plan),100) <> 0) OR (MOD(YEAR(T.shipment_date_plan),400) = 0))
                                               THEN CASE
                                                             WHEN DAY(T.shipment_date_plan) BETWEEN 2 AND 8
                                                                   THEN 1
                                                             WHEN DAY(T.shipment_date_plan) BETWEEN 9 AND 15
                                                                   THEN 2
                                                             WHEN DAY(T.shipment_date_plan) BETWEEN 16 AND 22
                                                                   THEN 3
                                                             WHEN DAY(T.shipment_date_plan) BETWEEN 23 AND 29
                                                                   THEN 4
                                                             ELSE 0
                                                      END
                                        ELSE CASE
                                                      WHEN DAY(T.shipment_date_plan) BETWEEN 1 AND 7
                                                             THEN 1
                                                      WHEN DAY(T.shipment_date_plan) BETWEEN 8 AND 14
                                                             THEN 2
                                                      WHEN DAY(T.shipment_date_plan) BETWEEN 15 AND 21
                                                             THEN 3
                                                      WHEN DAY(T.shipment_date_plan) BETWEEN 22 AND 28
                                                             THEN 4
                                                      ELSE 0
                                               END
                                  END
                    ELSE 0
             END AS week_sintetic
       FROM TECH_1 T),
--
THCH_MONTH AS
       (SELECT
             T2.year_no,
             T2.month_no,
             T2.week_sintetic,
             T2.sales_channel,
             T2.business_unit,
             T2.shipment_point_code,
             T2.delivery_point_name,
             SUM(T2.day_transportation_quantity) AS sintetic_week_transportation_quantity
       FROM TECH_2 T2
       WHERE T2.week_sintetic <> 0
       GROUP BY T2.year_no,
             T2.month_no,
             T2.week_sintetic,
             T2.sales_channel,
             T2.business_unit,
             T2.shipment_point_code,
             T2.delivery_point_name),
--
TECH_DEST_VARIABILITY_WEEK AS
       (SELECT
             T2.week_no,
             T2.sales_channel,
             T2.business_unit,
             T2.shipment_point_code,
             T2.delivery_point_name,
             SUM(T2.day_transportation_quantity) AS destination_week_all,
             (MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity)) AS destination_week_variability,
       (SUM(T2.day_transportation_quantity)*(MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity))) AS destination_week_significance
       FROM TECH_2 T2
       GROUP BY T2.week_no,
             T2.sales_channel,
             T2.business_unit,
             T2.shipment_point_code,
             T2.delivery_point_name),
--
TECH_ALL_DEST_VARIABILITY_WEEK AS
       (SELECT
             T2.week_no,
             T2.business_unit,
             T2.shipment_point_code,
             T2.delivery_point_name,
             SUM(T2.day_transportation_quantity) AS all_destination_week_all,
             (MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity)) AS all_destination_week_variability,
       (SUM(T2.day_transportation_quantity)*(MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity))) AS all_destination_week_significance
       FROM  TECH_2 T2
       GROUP BY T2.week_no,
             T2.business_unit,
             T2.shipment_point_code,
             T2.delivery_point_name),
--
TECH_DEST_VARIABILITY_MONTH AS
       (SELECT
             TM.year_no,
             TM.month_no,
             TM.sales_channel,
             TM.business_unit,
             TM.shipment_point_code,
             TM.delivery_point_name,
             SUM(TM.sintetic_week_transportation_quantity) AS destination_month_all,
           (MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity)) AS destination_month_variability,
       (SUM(TM.sintetic_week_transportation_quantity)*(MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity))) AS destination_month_significance
       FROM THCH_MONTH TM
       GROUP BY TM.year_no,
             TM.month_no,
             TM.sales_channel,
             TM.business_unit,
             TM.shipment_point_code,
             TM.delivery_point_name),
--
TECH_ALL_DEST_VARIABILITY_MONTH AS
       (SELECT
             TM.year_no,
             TM.month_no,
             TM.business_unit,
             TM.shipment_point_code,
             TM.delivery_point_name,
             SUM(TM.sintetic_week_transportation_quantity) AS all_destination_month_all,
           (MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity)) AS all_destination_month_variability,
       (SUM(TM.sintetic_week_transportation_quantity)*(MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity))) AS all_destination_month_significance
       FROM THCH_MONTH TM
       GROUP BY TM.year_no,
             TM.month_no,
             TM.business_unit,
             TM.shipment_point_code,
             TM.delivery_point_name),
--
TECH_SP_VARIABILITY_WEEK AS
       (SELECT
             TDW.week_no,
             TDW.sales_channel,
             TDW.business_unit,
             TDW.shipment_point_code,
             SUM(TDW.destination_week_all) AS sp_week_all,
             SUM(TDW.destination_week_significance) AS sp_week_significance,
             (SUM(TDW.destination_week_significance)/SUM(TDW.destination_week_all)) AS sp_week_variability
       FROM TECH_DEST_VARIABILITY_WEEK TDW
       GROUP BY TDW.week_no,
             TDW.sales_channel,
             TDW.business_unit,
             TDW.shipment_point_code),
--
TECH_ALL_SP_VARIABILITY_WEEK AS
       (SELECT
             TADW.week_no,
             TADW.business_unit,
             TADW.shipment_point_code,
             SUM(TADW.all_destination_week_all) AS all_sp_week_all,
             SUM(TADW.all_destination_week_significance) AS all_sp_week_significance,
             (SUM(TADW.all_destination_week_significance)/SUM(TADW.all_destination_week_all)) AS all_sp_week_variability
       FROM  TECH_ALL_DEST_VARIABILITY_WEEK TADW
       GROUP BY TADW.week_no,
             TADW.business_unit,
             TADW.shipment_point_code),
--
TECH_SP_VARIABILITY_MONTH AS
       (SELECT
             TDM.year_no,
             TDM.month_no,
             TDM.sales_channel,
             TDM.business_unit,
             TDM.shipment_point_code,
             SUM(TDM.destination_month_all) AS sp_month_all,
             SUM(TDM.destination_month_significance) AS sp_month_significance,
             (SUM(TDM.destination_month_significance)/SUM(TDM.destination_month_all)) AS sp_month_variability
       FROM TECH_DEST_VARIABILITY_MONTH TDM
       GROUP BY TDM.year_no,
             TDM.month_no,
             TDM.sales_channel,
             TDM.business_unit,
             TDM.shipment_point_code),
--
TECH_ALL_SP_VARIABILITY_MONTH AS
       (SELECT
             TADM.year_no,
             TADM.month_no,
             TADM.business_unit,
             TADM.shipment_point_code,
             SUM(TADM.all_destination_month_all) AS all_sp_month_all,
             SUM(TADM.all_destination_month_significance) AS all_sp_month_significance,
             (SUM(TADM.all_destination_month_significance)/SUM(TADM.all_destination_month_all)) AS all_sp_month_variability
       FROM TECH_ALL_DEST_VARIABILITY_MONTH TADM
       GROUP BY TADM.year_no,
             TADM.month_no,
             TADM.business_unit,
             TADM.shipment_point_code),
--
TECH_BU_VARIABILITY_WEEK AS
       (SELECT
             TSW.week_no,
             TSW.sales_channel,
             TSW.business_unit,
             SUM(TSW.sp_week_all) AS bu_week_all,
             SUM(TSW.sp_week_significance) AS bu_week_significance,
             (SUM(TSW.sp_week_significance)/SUM(TSW.sp_week_all)) AS bu_week_variability
       FROM TECH_SP_VARIABILITY_WEEK TSW
       GROUP BY TSW.week_no,
             TSW.sales_channel,
             TSW.business_unit),
--
TECH_ALL_BU_VARIABILITY_WEEK AS
       (SELECT
             TASW.week_no,
             TASW.business_unit,
             SUM(TASW.all_sp_week_all) AS all_bu_week_all,
             SUM(TASW.all_sp_week_significance) AS all_bu_week_significance,
             (SUM(TASW.all_sp_week_significance)/SUM(TASW.all_sp_week_all)) AS all_bu_week_variability
       FROM  TECH_ALL_SP_VARIABILITY_WEEK TASW
       GROUP BY TASW.week_no,
             TASW.business_unit),
--
TECH_BU_VARIABILITY_MONTH AS
       (SELECT
             TSM.year_no,
             TSM.month_no,
             TSM.sales_channel,
             TSM.business_unit,
             SUM(TSM.sp_month_all) AS bu_month_all,
             SUM(TSM.sp_month_significance) AS bu_month_significance,
             (SUM(TSM.sp_month_significance)/SUM(TSM.sp_month_all)) AS bu_month_variability
       FROM TECH_SP_VARIABILITY_MONTH TSM
       GROUP BY TSM.year_no,
             TSM.month_no,
             TSM.sales_channel,
             TSM.business_unit),
--
TECH_ALL_BU_VARIABILITY_MONTH AS
       (SELECT
             TASM.year_no,
             TASM.month_no,
             TASM.business_unit,
             SUM(TASM.all_sp_month_all) AS all_bu_month_all,
             SUM(TASM.all_sp_month_significance) AS all_bu_month_significance,
             (SUM(TASM.all_sp_month_significance)/SUM(TASM.all_sp_month_all)) AS all_bu_month_variability
       FROM TECH_ALL_SP_VARIABILITY_MONTH TASM
       GROUP BY TASM.year_no,
             TASM.month_no,
             TASM.business_unit),
--
TECH_SC_VARIABILITY_WEEK AS
       (SELECT
             TBW.week_no,
             TBW.sales_channel,
             SUM(TBW.bu_week_all) AS sc_week_all,
             SUM(TBW.bu_week_significance) AS sc_week_significance,
             (SUM(TBW.bu_week_significance)/SUM(TBW.bu_week_all)) AS sc_week_variability
       FROM TECH_BU_VARIABILITY_WEEK TBW
       GROUP BY TBW.week_no,
             TBW.sales_channel),
--
TECH_SC_VARIABILITY_MONTH AS
       (SELECT
             TBM.year_no,
             TBM.month_no,
             TBM.sales_channel,
             SUM(TBM.bu_month_all) AS sc_month_all,
             SUM(TBM.bu_month_significance) AS sc_month_significance,
             (SUM(TBM.bu_month_significance)/SUM(TBM.bu_month_all)) AS sc_month_variability
       FROM TECH_BU_VARIABILITY_MONTH TBM
       GROUP BY TBM.year_no,
             TBM.month_no,
             TBM.sales_channel),
--
TECH_TOTAL_VARIABILITY_WEEK AS
       (SELECT
             TSCW.week_no,
             SUM(TSCW.sc_week_all) AS total_week_all,
             SUM(TSCW.sc_week_significance) AS total_week_significance,
             (SUM(TSCW.sc_week_significance)/SUM(TSCW.sc_week_all)) AS total_week_variability
       FROM TECH_SC_VARIABILITY_WEEK TSCW
       GROUP BY TSCW.week_no),
--
TECH_TOTAL_VARIABILITY_MONTH AS
       (SELECT
             TSCM.year_no,
             TSCM.month_no,
             SUM(TSCM.sc_month_all) AS total_month_all,
             SUM(TSCM.sc_month_significance) AS total_month_significance,
             (SUM(TSCM.sc_month_significance)/SUM(TSCM.sc_month_all)) AS total_month_variability
       FROM TECH_SC_VARIABILITY_MONTH TSCM
       GROUP BY TSCM.year_no,
             TSCM.month_no),
--
TECH_SC_BU_DEST_VARIABILITY_WEEK AS
       (SELECT
             T2.week_no,
             T2.sales_channel,
             T2.business_unit,
             T2.delivery_point_name,
             SUM(T2.day_transportation_quantity) AS sc_bu_dest_week_all,
             (MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity)) AS sc_bu_dest_week_variability,
       (SUM(T2.day_transportation_quantity)*(MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity))) AS sc_bu_dest_week_significance
       FROM TECH_2 T2
       GROUP BY T2.week_no,
             T2.sales_channel,
             T2.business_unit,
             T2.delivery_point_name),
--
TECH_SC_BU_DEST_VARIABILITY_MONTH AS
       (SELECT
             TM.year_no,
             TM.month_no,
             TM.sales_channel,
             TM.business_unit,
             TM.delivery_point_name,
             SUM(TM.sintetic_week_transportation_quantity) AS sc_bu_dest_month_all,
           (MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity)) AS sc_bu_dest_month_variability,
       (SUM(TM.sintetic_week_transportation_quantity)*(MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity))) AS sc_bu_dest_month_significance
       FROM THCH_MONTH TM
       GROUP BY TM.year_no,
             TM.month_no,
             TM.sales_channel,
             TM.business_unit,
             TM.delivery_point_name),
--
TECH_SC_DEST_VARIABILITY_WEEK AS
       (SELECT
             T2.week_no,
             T2.sales_channel,
             T2.delivery_point_name,
             SUM(T2.day_transportation_quantity) AS sc_dest_week_all,
             (MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity)) AS sc_dest_week_variability,
       (SUM(T2.day_transportation_quantity)*(MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity))) AS sc_dest_week_significance
       FROM TECH_2 T2
       GROUP BY T2.week_no,
             T2.sales_channel,
             T2.delivery_point_name),
--
TECH_SC_DEST_VARIABILITY_MONTH AS
       (SELECT
             TM.year_no,
             TM.month_no,
             TM.sales_channel,
             TM.delivery_point_name,
             SUM(TM.sintetic_week_transportation_quantity) AS sc_dest_month_all,
           (MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity)) AS sc_dest_month_variability,
       (SUM(TM.sintetic_week_transportation_quantity)*(MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity))) AS sc_dest_month_significance
       FROM THCH_MONTH TM
       GROUP BY TM.year_no,
             TM.month_no,
             TM.sales_channel,
             TM.delivery_point_name),
--
TECH_BU_DEST_VARIABILITY_WEEK AS
       (SELECT
             T2.week_no,
             T2.business_unit,
             T2.delivery_point_name,
             SUM(T2.day_transportation_quantity) AS bu_dest_week_all,
             (MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity)) AS bu_dest_week_variability,
       (SUM(T2.day_transportation_quantity)*(MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity))) AS bu_dest_week_significance
       FROM TECH_2 T2
       GROUP BY T2.week_no,
             T2.business_unit,
             T2.delivery_point_name),
--
TECH_BU_DEST_VARIABILITY_MONTH AS
       (SELECT
             TM.year_no,
             TM.month_no,
             TM.business_unit,
             TM.delivery_point_name,
             SUM(TM.sintetic_week_transportation_quantity) AS bu_dest_month_all,
           (MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity)) AS bu_dest_month_variability,
       (SUM(TM.sintetic_week_transportation_quantity)*(MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity))) AS bu_dest_month_significance
       FROM THCH_MONTH TM
       GROUP BY TM.year_no,
             TM.month_no,
             TM.business_unit,
             TM.delivery_point_name),
--
TECH_ONLY_DEST_VARIABILITY_WEEK AS
       (SELECT
             T2.week_no,
             T2.delivery_point_name,
             SUM(T2.day_transportation_quantity) AS only_dest_week_all,
             (MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity)) AS only_dest_week_variability,
       (SUM(T2.day_transportation_quantity)*(MAX(T2.day_transportation_quantity)/AVG(T2.day_transportation_quantity))) AS only_dest_week_significance
       FROM TECH_2 T2
       GROUP BY T2.week_no,
             T2.delivery_point_name),
--
TECH_ONLY_DEST_VARIABILITY_MONTH AS
       (SELECT
             TM.year_no,
             TM.month_no,
             TM.delivery_point_name,
             SUM(TM.sintetic_week_transportation_quantity) AS only_dest_month_all,
           (MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity)) AS only_dest_month_variability,
       (SUM(TM.sintetic_week_transportation_quantity)*(MAX(TM.sintetic_week_transportation_quantity)/AVG(TM.sintetic_week_transportation_quantity))) AS only_dest_month_significance
       FROM THCH_MONTH TM
       GROUP BY TM.year_no,
             TM.month_no,
             TM.delivery_point_name),
--
BASE_ROWS AS
       (SELECT
       FT.sales_channel,
       FT.business_unit,
       FT.shipment_point_code,
       FT.delivery_point_name,
       FT.week_no,
       FT.month_no,
       FT.year_no
FROM TECH_2 FT
GROUP BY FT.year_no,
       FT.month_no,
       FT.week_no,
       FT.sales_channel,
       FT.business_unit,
       FT.shipment_point_code,
       FT.delivery_point_name)
--
SELECT
       NVL(FT2.sales_channel, 'NO DATA') AS sales_channel,
       NVL(FT2.business_unit, 'NO DATA') AS business_unit,
       NVL(FT2.shipment_point_code, 'NO DATA') AS shipment_point_code,
       NVL(FT2.delivery_point_name, 'NO DATA') AS delivery_point,
       FT2.week_no AS week,
       FT2.month_no AS "month",
       FT2.year_no AS "year",
       TTVW.total_week_variability AS total_week_variability,
       TTVM.total_month_variability AS total_month_variability,
       TSCVW.sc_week_variability AS sales_channel_week_variability,
       TSCVM.sc_month_variability AS sales_channel_month_variability,
       TBVW.bu_week_variability AS business_unit_week_variability,
       TBVM.bu_month_variability AS business_unit_month_variability,
       TSVW.sp_week_variability AS shipment_point_week_variability,
       TSVM.sp_month_variability AS shipment_point_month_variability,
       TDVW.destination_week_variability AS delivery_point_week_variability,
       TDVM.destination_month_variability AS delivery_point_month_variability,
       TABVW.all_bu_week_variability AS all_business_unit_week_variability,
       TABVM.all_bu_month_variability AS all_business_unit_month_variability,
       TASVW.all_sp_week_variability AS all_shipment_point_week_variability,
       TASVM.all_sp_month_variability AS all_shipment_point_month_variability,
       TADVW.all_destination_week_variability AS all_delivery_point_week_variability,
       TADVM.all_destination_month_variability AS all_delivery_point_month_variability,
       TSBDV.sc_bu_dest_week_variability AS sc_bu_dest_week_variability,
       TSBDVM.sc_bu_dest_month_variability AS sc_bu_dest_month_variability,
       TSDV.sc_dest_week_variability AS sc_dest_week_variability,
       TSDVM.sc_dest_month_variability AS sc_dest_month_variability,
       TBDV.bu_dest_week_variability AS bu_dest_week_variability,
       TBDVM.bu_dest_month_variability AS bu_dest_month_variability,
       TODV.only_dest_week_variability AS dest_week_variability,
       TODVM.only_dest_month_variability AS dest_month_variability
FROM BASE_ROWS FT2
       LEFT JOIN TECH_TOTAL_VARIABILITY_WEEK TTVW ON TTVW.week_no = FT2.week_no
       LEFT JOIN TECH_TOTAL_VARIABILITY_MONTH TTVM ON TTVM.year_no = FT2.year_no
             AND TTVM.month_no = FT2.month_no
       LEFT JOIN TECH_SC_VARIABILITY_WEEK TSCVW ON TSCVW.week_no = FT2.week_no
             AND TSCVW.sales_channel = FT2.sales_channel
       LEFT JOIN TECH_SC_VARIABILITY_MONTH TSCVM ON TSCVM.year_no = FT2.year_no
             AND TSCVM.month_no = FT2.month_no
             AND TSCVM.sales_channel = FT2.sales_channel
       LEFT JOIN TECH_BU_VARIABILITY_WEEK TBVW ON TBVW.week_no = FT2.week_no
             AND TBVW.sales_channel = FT2.sales_channel
             AND TBVW.business_unit = FT2.business_unit
       LEFT JOIN TECH_BU_VARIABILITY_MONTH TBVM ON TBVM.year_no = FT2.year_no
             AND TBVM.month_no = FT2.month_no
             AND TBVM.sales_channel = FT2.sales_channel
             AND TBVM.business_unit = FT2.business_unit
       LEFT JOIN TECH_SP_VARIABILITY_WEEK TSVW ON TSVW.week_no = FT2.week_no
             AND TSVW.sales_channel = FT2.sales_channel
             AND TSVW.business_unit = FT2.business_unit
             AND TSVW.shipment_point_code = FT2.shipment_point_code
       LEFT JOIN TECH_SP_VARIABILITY_MONTH TSVM ON TSVM.year_no = FT2.year_no
             AND TSVM.month_no = FT2.month_no
             AND TSVM.sales_channel = FT2.sales_channel
             AND TSVM.business_unit = FT2.business_unit
             AND TSVM.shipment_point_code = FT2.shipment_point_code
       LEFT JOIN TECH_DEST_VARIABILITY_WEEK TDVW ON TDVW.week_no = FT2.week_no
             AND TDVW.sales_channel = FT2.sales_channel
             AND TDVW.business_unit = FT2.business_unit
             AND TDVW.shipment_point_code = FT2.shipment_point_code
             AND TDVW.delivery_point_name = FT2.delivery_point_name
       LEFT JOIN TECH_DEST_VARIABILITY_MONTH TDVM ON TDVM.year_no = FT2.year_no
             AND TDVM.month_no = FT2.month_no
             AND TDVM.sales_channel = FT2.sales_channel
             AND TDVM.business_unit = FT2.business_unit
             AND TDVM.shipment_point_code = FT2.shipment_point_code
             AND TDVM.delivery_point_name = FT2.delivery_point_name
       LEFT JOIN TECH_ALL_BU_VARIABILITY_WEEK TABVW ON TABVW.week_no = FT2.week_no
             AND TABVW.business_unit = FT2.business_unit
       LEFT JOIN TECH_ALL_BU_VARIABILITY_MONTH TABVM ON TABVM.year_no = FT2.year_no
             AND TABVM.month_no = FT2.month_no
             AND TABVM.business_unit = FT2.business_unit
       LEFT JOIN TECH_ALL_SP_VARIABILITY_WEEK TASVW ON TASVW.week_no = FT2.week_no
             AND TASVW.business_unit = FT2.business_unit
             AND TASVW.shipment_point_code = FT2.shipment_point_code
       LEFT JOIN TECH_ALL_SP_VARIABILITY_MONTH TASVM ON TASVM.year_no = FT2.year_no
             AND TASVM.month_no = FT2.month_no
             AND TASVM.business_unit = FT2.business_unit
             AND TASVM.shipment_point_code = FT2.shipment_point_code
       LEFT JOIN TECH_ALL_DEST_VARIABILITY_WEEK TADVW ON TADVW.week_no = FT2.week_no
             AND TADVW.business_unit = FT2.business_unit
             AND TADVW.shipment_point_code = FT2.shipment_point_code
             AND TADVW.delivery_point_name = FT2.delivery_point_name
       LEFT JOIN TECH_ALL_DEST_VARIABILITY_MONTH TADVM ON TADVM.year_no = FT2.year_no
             AND TADVM.month_no = FT2.month_no
             AND TADVM.business_unit = FT2.business_unit
             AND TADVM.shipment_point_code = FT2.shipment_point_code
             AND TADVM.delivery_point_name = FT2.delivery_point_name
       LEFT JOIN TECH_SC_BU_DEST_VARIABILITY_WEEK TSBDV ON FT2.week_no = TSBDV.week_no
             AND FT2.sales_channel = TSBDV.sales_channel
             AND FT2.business_unit = TSBDV.business_unit
             AND FT2.delivery_point_name = TSBDV.delivery_point_name
       LEFT JOIN TECH_SC_BU_DEST_VARIABILITY_MONTH TSBDVM ON FT2.year_no = TSBDVM.year_no
             AND FT2.month_no = TSBDVM.month_no
             AND FT2.sales_channel = TSBDVM.sales_channel
             AND FT2.business_unit = TSBDVM.business_unit
             AND FT2.delivery_point_name = TSBDVM.delivery_point_name
       LEFT JOIN TECH_SC_DEST_VARIABILITY_WEEK TSDV ON FT2.week_no = TSDV.week_no
             AND FT2.sales_channel = TSDV.sales_channel
             AND FT2.delivery_point_name = TSDV.delivery_point_name
       LEFT JOIN TECH_SC_DEST_VARIABILITY_MONTH TSDVM ON FT2.year_no = TSDVM.year_no
             AND FT2.month_no = TSDVM.month_no
             AND FT2.sales_channel = TSDVM.sales_channel
             AND FT2.delivery_point_name = TSDVM.delivery_point_name
       LEFT JOIN TECH_BU_DEST_VARIABILITY_WEEK TBDV ON FT2.week_no = TBDV.week_no
             AND FT2.business_unit = TBDV.business_unit
             AND FT2.delivery_point_name = TBDV.delivery_point_name
       LEFT JOIN TECH_BU_DEST_VARIABILITY_MONTH TBDVM ON FT2.year_no = TBDVM.year_no
             AND FT2.month_no = TBDVM.month_no
             AND FT2.business_unit = TBDVM.business_unit
             AND FT2.delivery_point_name = TBDVM.delivery_point_name
       LEFT JOIN TECH_ONLY_DEST_VARIABILITY_WEEK TODV ON FT2.week_no = TODV.week_no
             AND FT2.delivery_point_name = TODV.delivery_point_name
       LEFT JOIN TECH_ONLY_DEST_VARIABILITY_MONTH TODVM ON FT2.year_no = TODVM.year_no
             AND FT2.month_no = TODVM.month_no
             AND FT2.delivery_point_name = TODVM.delivery_point_name
       LEFT JOIN DM_CURRENT DC ON DC.year = FT2.year_no
             AND DC.month = FT2.month_no
             AND DC.week = FT2.week_no
             AND DC.sales_channel = FT2.sales_channel
             AND DC.business_unit = FT2.business_unit
             AND DC.shipment_point_code = FT2.shipment_point_code
             AND DC.delivery_point = FT2.delivery_point_name
WHERE DC.year IS NULL
       OR (
                    DC.year IS NOT NULL
                    AND
                    HASH(
                           NVL(FT2.sales_channel, 'NO DATA'),
                           NVL(FT2.business_unit, 'NO DATA'),
                           NVL(FT2.shipment_point_code, 'NO DATA'),
                           NVL(FT2.delivery_point_name, 'NO DATA'),
                           FT2.week_no,
                           FT2.month_no,
                           FT2.year_no,
                           TTVW.total_week_variability::NUMERIC(18,6),
                           TTVM.total_month_variability::NUMERIC(18,6),
                           TSCVW.sc_week_variability::NUMERIC(18,6),
                           TSCVM.sc_month_variability::NUMERIC(18,6),
                           TBVW.bu_week_variability::NUMERIC(18,6),
                           TBVM.bu_month_variability::NUMERIC(18,6),
                           TSVW.sp_week_variability::NUMERIC(18,6),
                           TSVM.sp_month_variability::NUMERIC(18,6),
                           TDVW.destination_week_variability::NUMERIC(18,6),
                           TDVM.destination_month_variability::NUMERIC(18,6),
                           TABVW.all_bu_week_variability::NUMERIC(18,6),
                           TABVM.all_bu_month_variability::NUMERIC(18,6),
                           TASVW.all_sp_week_variability::NUMERIC(18,6),
                           TASVM.all_sp_month_variability::NUMERIC(18,6),
                           TADVW.all_destination_week_variability::NUMERIC(18,6),
                           TADVM.all_destination_month_variability::NUMERIC(18,6),
                           TSBDV.sc_bu_dest_week_variability::NUMERIC(18,6),
                           TSBDVM.sc_bu_dest_month_variability::NUMERIC(18,6),
                           TSDV.sc_dest_week_variability::NUMERIC(18,6),
                           TSDVM.sc_dest_month_variability::NUMERIC(18,6),
                           TBDV.bu_dest_week_variability::NUMERIC(18,6),
                           TBDVM.bu_dest_month_variability::NUMERIC(18,6),
                           TODV.only_dest_week_variability::NUMERIC(18,6),
                           TODVM.only_dest_month_variability::NUMERIC(18,6)
                    )
                    !=
                    HASH(
                           DC.sales_channel,
                           DC.business_unit,
                           DC.shipment_point_code,
                           DC.delivery_point,
                           DC.week,
                           DC.month,
                           DC.year,
                           DC.total_week_variability::NUMERIC(18,6),
                           DC.total_month_variability::NUMERIC(18,6),
                           DC.sales_channel_week_variability::NUMERIC(18,6),
                           DC.sales_channel_month_variability::NUMERIC(18,6),
                           DC.business_unit_week_variability::NUMERIC(18,6),
                           DC.business_unit_month_variability::NUMERIC(18,6),
                           DC.shipment_point_week_variability::NUMERIC(18,6),
                           DC.shipment_point_month_variability::NUMERIC(18,6),
                           DC.delivery_point_week_variability::NUMERIC(18,6),
                           DC.delivery_point_month_variability::NUMERIC(18,6),
                           DC.all_business_unit_week_variability::NUMERIC(18,6),
                           DC.all_business_unit_month_variability::NUMERIC(18,6),
                           DC.all_shipment_point_week_variability::NUMERIC(18,6),
                           DC.all_shipment_point_month_variability::NUMERIC(18,6),
                           DC.all_delivery_point_week_variability::NUMERIC(18,6),
                           DC.all_delivery_point_month_variability::NUMERIC(18,6),
                           DC.sc_bu_dest_week_variability::NUMERIC(18,6),
                           DC.sc_bu_dest_month_variability::NUMERIC(18,6),
                           DC.sc_dest_week_variability::NUMERIC(18,6),
                           DC.sc_dest_month_variability::NUMERIC(18,6),
                           DC.bu_dest_week_variability::NUMERIC(18,6),
                           DC.bu_dest_month_variability::NUMERIC(18,6),
                           DC.dest_week_variability::NUMERIC(18,6),
                           DC.dest_month_variability::NUMERIC(18,6)
                    )
             )
;