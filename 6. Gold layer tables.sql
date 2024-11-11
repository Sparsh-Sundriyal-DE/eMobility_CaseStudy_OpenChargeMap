-- Databricks notebook source
CREATE or replace VIEW charger_master_info AS
SELECT
    cl.location_id,
    cl.operator_location_id,
    cl.location_type,
    cl.location_sub_type,
    cl.name AS location_name,
    cl.country_code,
    cl.city,
    cl.county,
    cl.postal_code,
    cl.latitude,
    cl.longitude,
    cl.status,
    cl.operator,
    cl.owning_company,
    ce.evse_id,
    ce.manufacturer,
    ce.model,
    cc.connector_id,
    cc.connector_type,
    cc.power_type,
    cc.power_kw,
    cl.commissioned,
    cl.decommissioned
FROM
    charger_location_silver_sparsh cl
JOIN
    charger_evse_silver_sparsh ce ON cl.location_id = ce.location_id
JOIN
    charger_connector_silver_sparsh cc ON ce.evse_id = cc.evse_id;


-- COMMAND ----------

CREATE OR REPLACE VIEW charger_count_by_state AS
SELECT
    cl.county AS state,
    cc.power_type,
    COUNT(cc.connector_id) AS charger_count
FROM
    charger_location_silver_sparsh cl
JOIN
    charger_evse_silver_sparsh ce ON cl.location_id = ce.location_id
JOIN
    charger_connector_silver_sparsh cc ON ce.evse_id = cc.evse_id
GROUP BY
    cl.county, cc.power_type;


-- COMMAND ----------

CREATE OR REPLACE VIEW charger_power_distribution AS
SELECT
    CASE
        WHEN cc.power_kw IS NULL THEN 'Unknown Power'
        WHEN cc.power_kw BETWEEN 0 AND 2 THEN 'Ultra Low Power'
        WHEN cc.power_kw BETWEEN 3 AND 6 THEN 'Low Power'
        WHEN cc.power_kw BETWEEN 7 AND 43 AND cc.power_type = 'AC' THEN 'Slow'
        WHEN cc.power_kw <= 49 AND cc.power_type = 'DC' THEN 'Slow'
        WHEN cc.power_kw BETWEEN 50 AND 100 THEN 'Fast'
        WHEN cc.power_kw BETWEEN 101 AND 150 THEN 'Rapid (Ultra Fast)'
        WHEN cc.power_kw > 150 AND cc.power_kw <= 350 THEN 'Ultra Rapid (High Power)'
        WHEN cc.power_kw > 350 THEN 'Ultra Rapid+ (Kelvin’s definition)'
    END AS charging_speed,
    COUNT(cc.connector_id) AS charger_count
FROM
    charger_connector_silver_sparsh cc
GROUP BY
    CASE
        WHEN cc.power_kw IS NULL THEN 'Unknown Power'
        WHEN cc.power_kw BETWEEN 0 AND 2 THEN 'Ultra Low Power'
        WHEN cc.power_kw BETWEEN 3 AND 6 THEN 'Low Power'
        WHEN cc.power_kw BETWEEN 7 AND 43 AND cc.power_type = 'AC' THEN 'Slow'
        WHEN cc.power_kw <= 49 AND cc.power_type = 'DC' THEN 'Slow'
        WHEN cc.power_kw BETWEEN 50 AND 100 THEN 'Fast'
        WHEN cc.power_kw BETWEEN 101 AND 150 THEN 'Rapid (Ultra Fast)'
        WHEN cc.power_kw > 150 AND cc.power_kw <= 350 THEN 'Ultra Rapid (High Power)'
        WHEN cc.power_kw > 350 THEN 'Ultra Rapid+ (Kelvin’s definition)'
    END;


-- COMMAND ----------

CREATE OR REPLACE VIEW charging_stations_over_time AS
SELECT
    cl.commissioned,
    COUNT(cl.location_id) AS stations_commissioned
FROM
    charger_location_silver_sparsh cl
WHERE
    cl.commissioned IS NOT NULL
GROUP BY
    cl.commissioned
ORDER BY
    cl.commissioned;


-- COMMAND ----------

CREATE OR REPLACE VIEW key_metrics AS
SELECT
    COUNT(DISTINCT cl.location_id) AS total_charging_stations,
    AVG(cc.power_kw) AS average_connector_power,
    COUNT(DISTINCT cl.operator) AS unique_operators
FROM
    charger_location_silver_sparsh cl
JOIN
    charger_evse_silver_sparsh ce ON cl.location_id = ce.location_id
JOIN
    charger_connector_silver_sparsh cc ON ce.evse_id = cc.evse_id;


-- COMMAND ----------

select * from charger_count_by_state

-- COMMAND ----------

select * from charger_power_distribution

-- COMMAND ----------

SELECT power_kw, COUNT(*) 
FROM charger_connector_silver_sparsh 
GROUP BY power_kw;


-- COMMAND ----------


