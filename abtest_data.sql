SELECT
  e.userid,
  MIN(u.dt) AS min_dt,
  ANY_VALUE(ab_group) AS ab_group,

-- Standard columns : 
  -- purchaser
  -- purchases
  -- playdays
  -- inapp_spend
  
  IF(IFNULL(COUNTIF(eventid = 1400 AND localPrice > 0), 0) > 0, 1, 0) AS purchaser,
  IFNULL(COUNTIF(eventid = 1400 AND localPrice > 0), 0) AS purchases,
  COUNT(DISTINCT DATE(ts)) AS playdays,
  IFNULL(SUM(IF(eventid = 1400 AND localPrice > 0, localPrice / fx.exchange_rate, 0)), 0) AS inapp_spend,

-- Ad views and revenue :
  -- ad_views
  -- ad_revenue
  -- interstitial_ad_views
  -- interstitial_ad_revenue
  -- video_ad_views
  -- video_ad_revenue

  SUM(IFNULL(a.views, 0)   / daily_events) AS ad_views,
  SUM(IFNULL(a.revenue, 0) / daily_events) AS ad_revenue,
  SUM(IFNULL(a.interstitial_views, 0)   / daily_events) AS interstitial_ad_views,
  SUM(IFNULL(a.interstitial_revenue, 0) / daily_events) AS interstitial_ad_revenue,
  SUM(IFNULL(a.video_views, 0)   / daily_events) AS video_ad_views,
  SUM(IFNULL(a.video_revenue, 0) / daily_events) AS video_ad_revenue,

-- Ad views and revenue per active day : 
 -- ad_views_per_day
 -- ad_revenue_arpdau
 -- interstitial_ad_views_per_day
 -- interstitial_ad_revenue_arpdau
 -- video_ad_views_per_day
 -- video_ad_revenue_arpdau

  SUM(IFNULL(a.views, 0)   / daily_events) / COUNT(DISTINCT DATE(ts)) AS ad_views_per_day,
  SUM(IFNULL(a.revenue, 0) / daily_events) / COUNT(DISTINCT DATE(ts)) AS ad_revenue_arpdau,
  SUM(IFNULL(a.interstitial_views, 0)   / daily_events) / COUNT(DISTINCT DATE(ts)) AS interstitial_ad_views_per_day,
  SUM(IFNULL(a.interstitial_revenue, 0) / daily_events) / COUNT(DISTINCT DATE(ts)) AS interstitial_ad_revenue_arpdau,
  SUM(IFNULL(a.video_views, 0)   / daily_events) / COUNT(DISTINCT DATE(ts)) AS video_ad_views_per_day,
  SUM(IFNULL(a.video_revenue, 0) / daily_events) / COUNT(DISTINCT DATE(ts)) AS video_ad_revenue_arpdau,

-- Purchases : 
 -- Revenue
 -- Arpdau

  IFNULL(SUM(localPrice / fx.exchange_rate), 0) + SUM(IFNULL(a.revenue, 0) / daily_events) AS revenue,
  (IFNULL(SUM(localPrice / fx.exchange_rate), 0) + SUM(IFNULL(a.revenue, 0) / daily_events)) / COUNT(DISTINCT DATE(ts)) AS arpdau,

  -- Specific columns to the game (wz)
  COUNTIF(LOWER(eventname) = "collectiontoastbutton") AS collectiontoast,
  COUNTIF(LOWER(eventname) = "gamestarted") AS games_started,
  COUNTIF(LOWER(eventname) = "gamefinished") AS games_finished,
  COUNTIF(LOWER(eventname) = "gamestarted") / COUNT(DISTINCT DATE(ts)) AS games_started_per_day,
  COUNTIF(LOWER(eventname) = "gamefinished") / COUNT(DISTINCT DATE(ts)) AS games_finished_per_day,
  COUNTIF(LOWER(eventname) = "rewardstabopened") AS rewardtabopened,
  COUNTIF(LOWER(eventname) = "eventrewardscollected") AS rewardcollected,
  IFNULL(COUNTIF(localPrice > 0 AND purchaseditem = "qd2_remove_ads_30"), 0) AS purchases_remove_ads_30,
  IFNULL(SUM(PaidSwapsUsed), 0) AS paidswaps,
  IFNULL(SUM(PaidSwapsUsed), 0) / COUNT(DISTINCT DATE(ts)) AS paidswaps_per_day,
  SAFE_DIVIDE(IFNULL(SUM(PaidSwapsUsed), 0), COUNTIF(LOWER(eventname) = "gamefinished")) AS paidswaps_per_game_finished,

FROM
-- Event table
(SELECT * EXCEPT (tags, customEventData), COUNT(ts) OVER (PARTITION BY userid, DATE(ts)) AS daily_events
  -- Game's name must be modified if required
  FROM `maginteractive-se-analytics.bi_0_staging.wz_staging` 
  WHERE
  -- Specify range of dates when the ab test was running
    DATE(_PARTITIONTIME) between "2022-05-18" AND "2022-06-16" 
    ) e

-- Join: A/B test assigmnets table
INNER JOIN (
    SELECT userid, abtestname, `group` AS ab_group, DATE(ts) AS dt
    FROM `maginteractive-se-analytics.analytics_davincigameserver.ab_test_assignments` 
      -- Game's name must be modified
    WHERE abtestname = "wz_ab_test_interstitials_placement" --AND DATE(ts) <= "2022-05-03"   
  ) u
  ON CAST(e.userid AS string) = CAST(u.userid AS string)

-- Join: Ad revenue
LEFT JOIN (SELECT 
              userid, dt, 
              SUM(views) AS views,
              SUM(revenue) AS revenue,
              SUM(IF(ad_class = "interstitials", views, 0)) AS interstitial_views,
              SUM(IF(ad_class = "interstitials", revenue, 0)) AS interstitial_revenue,
              SUM(IF(ad_class = "videos", views, 0)) AS video_views,
              SUM(IF(ad_class = "videos", revenue, 0)) AS video_revenue,       
      --Game's name must be modified if required
  FROM `maginteractive-se-analytics.bi_2_flexible.wz_ad_revs` GROUP BY userid, dt) a 
  ON e.userid = a.userid AND DATE(e.dt) = DATE(a.dt)

-- Join: Exchange rates (to cONvert purchASes to USD)
left join `maginteractive-se-analytics.analytics_external_data.exchange_rates` fx
  ON LOWER(e.localcurrency) = LOWER(fx.currency_code) AND DATE(e.ts) = DATE(fx.dt)

--Where
    -- Filter out dsr up to the minimum max dsr (use last assignment date compared to curent date)
    -- DATE_DIFF(DATE(ts), u.dt, day) <= DATE_DIFF(current_DATE, "2022-05-18", day) 

GROUP BY 1
ORDER BY 5 DESC