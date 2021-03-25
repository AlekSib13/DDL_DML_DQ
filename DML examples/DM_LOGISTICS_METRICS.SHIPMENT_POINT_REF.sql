TRUNCATE TABLE DM_LOGISTICS_METRICS.SHIPMENT_POINT_REF;
--
INSERT INTO DM_LOGISTICS_METRICS.SHIPMENT_POINT_REF
(
    shipment_point_code,
    shipment_point_name,
    shipment_point_dashboard_name
)
SELECT
    distinct A.vstel AS shipment_point_code,
    A.vtext AS shipment_point_name,
    CASE
        WHEN A.vstel = '4046' THEN 'Katoen Natie Bulk Chemicals Terminals'
        WHEN A.vstel = '9121' THEN 'Сибур Тобольск'
        WHEN A.vstel = '9131' THEN 'ЗСНХ'
        WHEN A.vstel = '41AT' THEN 'SI Austria'
        WHEN A.vstel = '41BE' THEN 'SI Belgium'
        WHEN A.vstel = '41DE' THEN 'SI Germany'
        WHEN A.vstel = '41DK' THEN 'SI Denmark'
        WHEN A.vstel = '41FI' THEN 'SI Finland'
        WHEN A.vstel = '41FR' THEN 'SI France'
        WHEN A.vstel = '41GB' THEN 'SI United Kingdom'
        WHEN A.vstel = '41LT' THEN 'SI Lithuania'
        WHEN A.vstel = '41NL' THEN 'SI Netherlands'
        WHEN A.vstel = '41PL' THEN 'SI Poland'
        WHEN A.vstel = '41RO' THEN 'SI Romania'
        WHEN A.vstel = '41SE' THEN 'SI Sweden'
        WHEN A.vstel = '41SK' THEN 'SI Slovakia'
        WHEN A.vstel = '43CN' THEN 'SI CN Shanghai'
        WHEN A.vstel = '9W57' THEN 'Сибур-Нефтехим (Дзержинск)'
        ELSE A.vtext
    END AS shipment_point_dashboard_name
FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    vstel
                ORDER BY tech_load_ts DESC
            ) AS rn
        FROM ODS_SAPERP.TVST
     ) AS A
WHERE A.rn = 1
ORDER BY A.vstel, A.vtext
;
