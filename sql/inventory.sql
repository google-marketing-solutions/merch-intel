# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Creates stored procedure for materializing inventory data.

CREATE OR REPLACE
  PROCEDURE
    `{project_id}.{dataset}.inventory_proc`()
      BEGIN
CREATE OR REPLACE TABLE `{project_id}.{dataset}.InventoryView`
AS
WITH
  PriceCompetitiveness AS (
    SELECT
      _PARTITIONDATE AS date,
      aggregator_id,
      merchant_id,
      id,
      brand,
      offer_id,
      report_country_code AS country_code,
      price AS current_price,
      benchmark_price
    FROM `{project_id}.{dataset}.PriceCompetitiveness_{merchant_id}`
  ),
  PriceInsights AS (
    SELECT
      _PARTITIONDATE AS date,
      aggregator_id,
      merchant_id,
      id,
      brand,
      offer_id,
      suggested_price,
      predicted_impressions_change_fraction,
      predicted_clicks_change_fraction,
      predicted_conversions_change_fraction,
    FROM `{project_id}.{dataset}.PriceInsights_{merchant_id}`
  ),
  GeoTargets AS (
    SELECT DISTINCT
      parent_id,
      country_code
    FROM
      `{project_id}.{dataset}.geo_targets`
  ),
  LanguageCodes AS (
    SELECT DISTINCT
      criterion_id,
      language_code
    FROM
      `{project_id}.{dataset}.language_codes`
  ),
  ShoppingProductStats AS (
    SELECT
      ShoppingProductStats._DATA_DATE,
      ShoppingProductStats._LATEST_DATE,
      ShoppingProductStats.segments_product_merchant_id AS merchant_id,
      LOWER(ShoppingProductStats.segments_product_channel) AS channel,
      ShoppingProductStats.segments_product_item_id AS offer_id,
      LanguageCodes.language_code,
      GeoTargets.country_code AS target_country,
      SUM(ShoppingProductStats.metrics_impressions) AS impressions,
      SUM(ShoppingProductStats.metrics_clicks) AS clicks,
      SAFE_DIVIDE(SUM(ShoppingProductStats.metrics_cost_micros), 1e6) AS cost,
      SUM(ShoppingProductStats.metrics_conversions) AS conversions,
      SUM(ShoppingProductStats.metrics_conversions_value) AS conversions_value
    FROM
      `{project_id}.{dataset}.ads_ShoppingProductStats_{external_customer_id}`
        AS ShoppingProductStats
    INNER JOIN GeoTargets
      ON
        GeoTargets.parent_id = CAST(
          SPLIT(ShoppingProductStats.segments_product_country, '/')[SAFE_OFFSET(1)] AS INT64)
    INNER JOIN LanguageCodes
      ON
        LanguageCodes.criterion_id = CAST(
          SPLIT(ShoppingProductStats.segments_product_language, '/')[SAFE_OFFSET(1)] AS INT64)
    WHERE segments_product_item_id IS NOT NULL
    GROUP BY ALL
  ),
  AggregatedPerformance AS (
    SELECT
      _DATA_DATE AS date,
      merchant_id,
      channel,
      offer_id,
      language_code,
      target_country,
      SUM(impressions)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE) ASC
          RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS impressions_30days,
      SUM(clicks)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE)
          RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS clicks_30days,
      SUM(cost)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE)
          RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS cost_30days,
      SUM(conversions)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE)
          RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS conversions_30days,
      SUM(conversions_value)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE)
          RANGE BETWEEN 30 PRECEDING AND CURRENT ROW
        ) AS conversions_value_30days,
      SUM(impressions)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE) ASC
          RANGE BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS impressions_7days,
      SUM(clicks)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE)
          RANGE BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS clicks_7days,
      SUM(cost)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE)
          RANGE BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS cost_7days,
      SUM(conversions)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE)
          RANGE BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS conversions_7days,
      SUM(conversions_value)
        OVER (
          PARTITION BY merchant_id, channel, offer_id, language_code, target_country
          ORDER BY UNIX_DATE(_DATA_DATE)
          RANGE BETWEEN 7 PRECEDING AND CURRENT ROW
        ) AS conversions_value_7days,
    FROM ShoppingProductStats
  ),
  ProductStatus AS (
    SELECT
      _PARTITIONDATE AS date,
      P.merchant_id,
      P.product_id,
      ARRAY(
        SELECT DISTINCT x
        FROM
          UNNEST(
            ARRAY_CONCAT(
              D.approved_countries, D.pending_countries, D.disapproved_countries)) AS x
      ) AS targeted_countries,
      D AS destinations
    FROM `{project_id}.{dataset}.Products_{merchant_id}` AS P
    LEFT JOIN P.destinations AS D
    WHERE D.name = 'Shopping'
  ),
  ProductStatusCountry AS (
    SELECT
      date,
      PS.merchant_id,
      PS.product_id,
      targeted_country,
      EXISTS(
        SELECT 1
        FROM PS.destinations.disapproved_countries AS disapproved_country
        WHERE disapproved_country = targeted_country
      ) AS is_disapproved
    FROM ProductStatus AS PS
    LEFT JOIN PS.targeted_countries AS targeted_country
  )
SELECT DISTINCT
  P._PARTITIONDATE AS date,
  IFNULL(P.aggregator_id, P.merchant_id) AS aggregator_id,
  P.merchant_id,
  P.product_id,
  P.offer_id,
  PSC.targeted_country,
  PSC.is_disapproved,
  P.title,
  P.link,
  P.availability,
  P.brand,
  P.google_brand_id,
  P.gtin,
  IFNULL(sale_price.value, price.value) AS current_price,
  google_product_category,
  google_product_category_path,
  ARRAY_REVERSE(SPLIT(google_product_category_path, ' > '))[SAFE_OFFSET(0)] AS final_category,
  product_type,
  STRUCT(
    image_link,
    color,
    custom_labels,
    condition,
    gender) AS additional_product_attributes,
  PC.benchmark_price.amount_micros / 1e6 AS benchmark_price,
  PI.suggested_price.amount_micros / 1e6 AS suggested_price,
  PI.predicted_impressions_change_fraction,
  PI.predicted_clicks_change_fraction,
  PI.predicted_conversions_change_fraction,
  AP.impressions_30days,
  AP.clicks_30days,
  AP.cost_30days,
  AP.conversions_30days,
  AP.conversions_value_30days,
  AP.impressions_7days,
  AP.clicks_7days,
  AP.cost_7days,
  AP.conversions_7days,
  AP.conversions_value_7days
FROM `{project_id}.{dataset}.Products_{merchant_id}` AS P
LEFT JOIN ProductStatusCountry AS PSC
  ON
    P._PARTITIONDATE = PSC.date
    AND P.merchant_id = PSC.merchant_id
    AND P.product_id = PSC.product_id
LEFT JOIN PriceCompetitiveness AS PC
  ON
    PC.date = P._PARTITIONDATE
    AND PC.merchant_id = P.merchant_id
    AND PC.id = P.product_id
    AND PC.country_code = PSC.targeted_country
LEFT JOIN PriceInsights AS PI
  ON
    PI.date = P._PARTITIONDATE
    AND PI.merchant_id = P.merchant_id
    AND PI.id = P.product_id
LEFT JOIN AggregatedPerformance AS AP
  ON
    AP.merchant_id = P.merchant_id
    AND LOWER(AP.offer_id) = LOWER(P.offer_id)
    AND AP.target_country = PSC.targeted_country
    AND AP.language_code = P.content_language
    AND DATE_ADD(AP.date, INTERVAL 1 DAY) = P._PARTITIONDATE
WHERE
  P.channel = 'online'
  AND EXISTS(
    SELECT 1
    FROM P.destinations
    WHERE name = 'Shopping'
  );

END;
