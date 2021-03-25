INSERT INTO DM_LOGISTICS_METRICS.METRICS_MAIN
(
    transportation_number,
    shipment_date_plan,
    transportation_start_date,
    delivery_date_plan,
    delivery_date_fact,
    shipment_point_code,
    delivery_point,
    business_unit,
    tariff_matrix_code,
    sales_channel,
    consignee_name,
    transportation_creator,
    transportation_creation_date,
    transportation_creation_time,
    net_weight,
    expences_fact_rub,
    expences_matrix_plan_rub,
    expences_business_plan_rub,
    auction_flag,
    forwarder_name,
    lead_time_plan
)
WITH VTTK_ACTUAL AS
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
CONSIGNEE_ACTUAL AS
    (SELECT *
    FROM
        (SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    kunnr
                ORDER BY tech_load_ts DESC
            ) AS rn
        FROM ODS_SAPERP.KNA1) A
    WHERE A.rn = 1
        AND A.tech_is_deleted = 0),
--
COSTS_ACTUAL AS
    (SELECT *
    FROM
        (SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    SHIPMENT_NUMBER
                ORDER BY TECH_LOAD_TS DESC
            ) AS rn
        FROM ODS_XLS_LOGISTICSMETRICS.TRANSPORTATION_COSTS) A
    WHERE A.rn = 1),
--
CURRENCY_ACTUAL AS
    (SELECT *
    FROM
        (SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    rate_date
                ORDER BY TECH_LOAD_TS DESC
            ) AS rn
        FROM DM_LOGISTICS_METRICS.CURRENCY_RATE_REF) A
    WHERE A.rn = 1),
--
TRANSPORTATION_WEIGHT AS
    (SELECT PA.tknum, SUM(LA.ntgew) AS net_weight
    FROM VTTP_ACTUAL PA
        LEFT JOIN LIKP_ACTUAL LA ON PA.vbeln = LA.vbeln
    GROUP BY PA.tknum),
--
LFA1_ACTUAL AS
       (SELECT *
 FROM
  (SELECT *,
   ROW_NUMBER() OVER (
    PARTITION BY
     F.lifnr
    ORDER BY F.tech_load_ts DESC
   ) AS rn
  FROM ODS_SAPERP.LFA1 F) A
WHERE A.rn = 1
  AND A.tech_is_deleted = 0),
--
TVRAB_ACTUAL_CUST AS
       (SELECT --*  --COUNT(*)
             A.route,
             A.knanf,
             A.knend,
             (A.knanf||'_'||A.knend||'_Auto')::VARCHAR(256) AS sap_leg_code
       FROM
             (SELECT *,
                    ROW_NUMBER() OVER (
                    PARTITION BY
                           S.route,
                           S.abnum
                    ORDER BY S.tech_load_ts DESC
             ) AS rn
             FROM ODS_SAPERP.TVRAB S) A
       WHERE A.rn = 1
             AND A.tech_is_deleted = 0
       GROUP BY A.route,
             A.knanf,
             A.knend),
--
SVT_TRANSPORT_LEG_ACTUAL AS
       (SELECT * --COUNT(*)
        FROM
         (SELECT *,
                    ROW_NUMBER() OVER (
                           PARTITION BY
                                  K.id
                           ORDER BY K.tech_load_ts DESC
                    ) AS rn
             FROM ODS_SVT.TRANSPORT_LEG K) A
             WHERE A.rn = 1
                    AND A.tech_is_deleted = 0
                    AND A.primitive_entity_data_state_id = 1),
--
ROUTE_LEAD_TIME AS
       (SELECT --COUNT(*)
             TAC.route,
             SUM(TL.transportation_time) AS svt_route_lead_time
       FROM TVRAB_ACTUAL_CUST TAC
             LEFT JOIN SVT_TRANSPORT_LEG_ACTUAL TL ON TAC.sap_leg_code = TL.code
       GROUP BY TAC.route),
--
DM_CURRENT AS
    (SELECT *
    FROM
        (SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    transportation_number
                ORDER BY TECH_LOAD_TS DESC
            ) AS rn
        FROM DM_LOGISTICS_METRICS.METRICS_MAIN) A
    WHERE A.rn = 1)
--
SELECT
    K.tknum::INTEGER AS transportation_number,
    L.wadat AS shipment_date_plan,
    K.datbg AS transportation_start_date,
    O.ETA_ACCORDING_CLIENT_PO AS delivery_date_plan,
    O.FACT_UNLOADING_DATE AS delivery_date_fact,
    S.vstel AS shipment_point_code,
    D.delivery_point_dashboard_name AS delivery_point,
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
                ELSE NULL
            END
    END AS business_unit,
    K.zzmatrix AS tariff_matrix_code,
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
        ELSE NULL
    END AS sales_channel,
    C.name1 AS consignee_name,
    K.ernam AS transportation_creator,
    K.erdat AS transportation_creation_date,
    K.erzet AS transportation_creation_time,
    T.net_weight AS net_weight,
    CASE
        WHEN CA.CURRENCY_FACT = 'USD'
            THEN (CA.FACT_COST*CU.rate_usd)
        WHEN CA.CURRENCY_FACT = 'EUR'
            THEN (CA.FACT_COST*CU.rate_eur)
        WHEN CA.CURRENCY_FACT = 'RUB'
            THEN CA.FACT_COST
        ELSE NULL
    END AS expences_fact_rub,
    CASE
        WHEN CA.CURRENCY_PLAN = 'USD'
            THEN (CA.PLAN_COST*CU.rate_usd)
        WHEN CA.CURRENCY_PLAN = 'EUR'
            THEN (CA.PLAN_COST*CU.rate_eur)
        WHEN CA.CURRENCY_PLAN = 'RUB'
            THEN CA.PLAN_COST
        ELSE NULL
    END AS expences_matrix_plan_rub,
    CASE
        WHEN CA.CURRENCY_BUSINESS_PLAN = 'USD'
            THEN (CA.BUSINESS_PLAN_COST*CU.rate_usd)
        WHEN CA.CURRENCY_BUSINESS_PLAN = 'EUR'
            THEN (CA.BUSINESS_PLAN_COST*CU.rate_eur)
        WHEN CA.CURRENCY_BUSINESS_PLAN = 'RUB'
            THEN CA.BUSINESS_PLAN_COST
        ELSE NULL
    END AS expences_business_plan_rub,
    CA.AUCTION AS auction_flag,
    F.name1 AS forwarder_name,
    RLT.svt_route_lead_time AS lead_time_plan
FROM VTTK_ACTUAL K
    LEFT JOIN VTTP_ACTUAL P ON K.tknum = P.tknum AND P.tpnum = 1
    LEFT JOIN LIKP_ACTUAL L ON P.vbeln = L.vbeln
    LEFT JOIN OTIF_DELIVERY_ACTUAL O ON K.tknum::INTEGER = O.SHIPMENT_NUM
    LEFT JOIN VTTS_ACTUAL S ON K.tknum = S.tknum
    LEFT JOIN DM_LOGISTICS_METRICS.DELIVERY_POINT_REF D ON K.add02 = D.delivery_point_code
    LEFT JOIN CONSIGNEE_ACTUAL C ON L.kunnr = C.kunnr
    LEFT JOIN COSTS_ACTUAL CA ON K.tknum = CA.SHIPMENT_NUMBER
    LEFT JOIN CURRENCY_ACTUAL CU ON L.wadat = CU.rate_date
    LEFT JOIN DM_LOGISTICS_METRICS.BUSINESS_UNIT_REF B ON L.vkorg = B.business_unit_code AND S.vstel = B.shipping_point_code
    LEFT JOIN TRANSPORTATION_WEIGHT T ON K.tknum = T.tknum
    LEFT JOIN LFA1_ACTUAL F ON F.lifnr = K.tdlnr
    LEFT JOIN ROUTE_LEAD_TIME RLT ON RLT.route = K.route
    LEFT JOIN DM_CURRENT DC ON K.tknum::INTEGER = DC.transportation_number
WHERE
        DC.transportation_number IS NULL
        OR (
            DC.transportation_number IS NOT NULL
            AND
            HASH(
                K.tknum::INTEGER,
                L.wadat,
                K.datbg,
                O.ETA_ACCORDING_CLIENT_PO,
                O.FACT_UNLOADING_DATE,
                S.vstel,
                D.delivery_point_dashboard_name,
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
                            ELSE NULL
                        END
                END,
                K.zzmatrix,
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
                    ELSE NULL
                END,
                C.name1,
                K.ernam,
                K.erdat,
                K.erzet,
                T.net_weight,
                CASE
                    WHEN CA.CURRENCY_FACT = 'USD'
                        THEN (CA.FACT_COST*CU.rate_usd)
                    WHEN CA.CURRENCY_FACT = 'EUR'
                        THEN (CA.FACT_COST*CU.rate_eur)
                    WHEN CA.CURRENCY_FACT = 'RUB'
                        THEN CA.FACT_COST
                    ELSE NULL
                END,
                CASE
                    WHEN CA.CURRENCY_PLAN = 'USD'
                        THEN (CA.PLAN_COST*CU.rate_usd)
                    WHEN CA.CURRENCY_PLAN = 'EUR'
                        THEN (CA.PLAN_COST*CU.rate_eur)
                    WHEN CA.CURRENCY_PLAN = 'RUB'
                        THEN CA.PLAN_COST
                    ELSE NULL
                END,
                CASE
                    WHEN CA.CURRENCY_BUSINESS_PLAN = 'USD'
                        THEN (CA.BUSINESS_PLAN_COST*CU.rate_usd)
                    WHEN CA.CURRENCY_BUSINESS_PLAN = 'EUR'
                        THEN (CA.BUSINESS_PLAN_COST*CU.rate_eur)
                    WHEN CA.CURRENCY_BUSINESS_PLAN = 'RUB'
                        THEN CA.BUSINESS_PLAN_COST
                    ELSE NULL
                END,
                CA.AUCTION,
                F.name1,
                    RLT.svt_route_lead_time
            )
            !=
            HASH(
                DC.transportation_number,
                DC.shipment_date_plan,
                DC.transportation_start_date,
                DC.delivery_date_plan,
                DC.delivery_date_fact,
                DC.shipment_point_code,
                DC.delivery_point,
                DC.business_unit,
                DC.tariff_matrix_code,
                DC.sales_channel,
                DC.consignee_name,
                DC.transportation_creator,
                DC.transportation_creation_date,
                DC.transportation_creation_time,
                DC.net_weight,
                DC.expences_fact_rub,
                DC.expences_matrix_plan_rub,
                DC.expences_business_plan_rub,
                DC.auction_flag,
                DC.forwarder_name,
                    DC.lead_time_plan
            )
        )
;