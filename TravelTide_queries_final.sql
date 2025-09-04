# Auto detect text files and perform LF normalization
* text=auto
/* =============================================================
   TravelTide — SQL Queries 
   Autor: Katrin Vogt
   Datum: 05.09.2025

   Inhalt
     1) Wachstum: Trips & New Users per Year (+ YoY)
     2) Key Metrics (2 Varianten – je nach Schema)
     3) Kundensegmente (≤28T Neukunden, Familien, Frequent Flyers, Business=P10)
   
============================================================= */


/* =============================================================
   1) Wachstum: Trips & New Users per Year (+ YoY)
   Basistabellen: sessions, users
============================================================= */
WITH prep_sessions AS (
  SELECT
    EXTRACT(YEAR FROM session_start) AS the_year,
    COUNT(DISTINCT trip_id)          AS trips_booked
  FROM sessions
  GROUP BY 1
),
prep_users AS (
  SELECT
    EXTRACT(YEAR FROM sign_up_date)  AS the_year,
    COUNT(user_id)                   AS new_users
  FROM users
  GROUP BY 1
)
SELECT
  ps.the_year,
  ps.trips_booked,
  ROUND(
    (ps.trips_booked - COALESCE(LAG(ps.trips_booked, 1) OVER (ORDER BY ps.the_year), 0))::numeric
    / NULLIF(LAG(ps.trips_booked, 1) OVER (ORDER BY ps.the_year), 0) * 100, 2
  ) AS growth_trips_percent,
  pu.new_users,
  ROUND(
    (pu.new_users - COALESCE(LAG(pu.new_users, 1) OVER (ORDER BY pu.the_year), 0))::numeric
    / NULLIF(LAG(pu.new_users, 1) OVER (ORDER BY pu.the_year), 0) * 100, 2
  ) AS growth_users_percent
FROM prep_sessions ps
JOIN prep_users   pu USING (the_year)
ORDER BY the_year;


/* =============================================================
   2) Key Metrics — Variante A (wenn Tabelle trip_facts existiert)
   trip_facts Spalten (Beispiel):
     - flight_booked (bool/int), hotel_booked (bool/int), trip_cancelled (bool/int)
     - trip_revenue_net_usd (numeric)
============================================================= */
-- RUN THIS ONLY IF YOUR SCHEMA HAS trip_facts
-- SELECT
--   COUNT(*) AS trips,
--   SUM(CASE WHEN (flight_booked = 1 OR flight_booked = TRUE
--                 OR hotel_booked  = 1 OR hotel_booked  = TRUE) THEN 1 ELSE 0 END) AS booked_trips,
--   ROUND(100.0 * SUM(CASE WHEN (flight_booked = 1 OR flight_booked = TRUE
--                              OR hotel_booked  = 1 OR hotel_booked  = TRUE) THEN 1 ELSE 0 END)
--         / NULLIF(COUNT(*), 0), 2) AS conversion_rate_pct,
--   ROUND(100.0 * SUM(CASE WHEN (trip_cancelled = 1 OR trip_cancelled = TRUE) THEN 1 ELSE 0 END)
--         / NULLIF(COUNT(*), 0), 2) AS cancellation_rate_pct,
--   ROUND(AVG(NULLIF(trip_revenue_net_usd, 0)), 2) AS avg_order_value_net_usd
-- FROM trip_facts;


/* =============================================================
   2) Key Metrics — Variante B (falls KEIN trip_facts vorhanden)
   Basistabelle: sessions (und optional flights/hotels für Revenue)
   - conversion: Anteil an Sessions/Trips, in denen flight/hotel gebucht wurde
   - cancellation: Anteil cancellation = true
   - AOV: wenn Net-Revenue nicht verfügbar, bitte überspringen/ersetzen
============================================================= */
WITH base AS (
  SELECT
    trip_id,
    MAX(CASE WHEN flight_booked THEN 1 ELSE 0 END) AS any_flight,
    MAX(CASE WHEN hotel_booked  THEN 1 ELSE 0 END) AS any_hotel,
    MAX(CASE WHEN cancellation  THEN 1 ELSE 0 END) AS cancelled
  FROM sessions
  GROUP BY trip_id
)
SELECT
  COUNT(*) AS trips,
  SUM(CASE WHEN (any_flight = 1 OR any_hotel = 1) THEN 1 ELSE 0 END) AS booked_trips,
  ROUND(100.0 * SUM(CASE WHEN (any_flight = 1 OR any_hotel = 1) THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0), 2) AS conversion_rate_pct,
  ROUND(100.0 * SUM(cancelled) / NULLIF(COUNT(*), 0), 2) AS cancellation_rate_pct
  -- , ROUND(AVG(...), 2) AS avg_order_value_net_usd  -- Optional: falls Nettorevenue verfügbar ist
FROM base;


/* =============================================================
   3) Kundensegmente (≤28T, Familien, Frequent Flyers, Business=P10)
   Basistabellen: users, sessions, flights
   - Frequent Flyer = Top 10% nach Anzahl Flugbuchungen
   - Business      = unterstes Dezil (P10) in total_sessions UND avg_page_clicks (nur aktive)
   - Familien      = total_seats >= 3 UND has_children = true
   - Neu           = (last_booking_date - sign_up_date) <= 28 Tage
============================================================= */
WITH booking_events AS (
  SELECT
    s.user_id,
    s.trip_id,
    MAX(CASE WHEN s.flight_booked THEN 1 ELSE 0 END) AS flight_booked_any,
    MAX(CASE WHEN s.hotel_booked  THEN 1 ELSE 0 END) AS hotel_booked_any,
    MAX(CASE WHEN s.cancellation  THEN 1 ELSE 0 END) AS trip_cancelled_any,
    MAX(DATE(s.session_start))                        AS last_session_date
  FROM sessions s
  GROUP BY s.user_id, s.trip_id
),
last_booking AS (
  -- "Buchung" = Flug ODER Hotel gebucht UND nicht storniert
  SELECT
    be.user_id,
    MAX(be.last_session_date) AS last_booking_date
  FROM booking_events be
  WHERE (be.flight_booked_any = 1 OR be.hotel_booked_any = 1)
    AND COALESCE(be.trip_cancelled_any,0) = 0
  GROUP BY be.user_id
),
user_session_stats AS (
  SELECT
    s.user_id,
    COUNT(*)::numeric                         AS total_sessions,
    AVG(COALESCE(s.page_clicks,0))::numeric  AS avg_page_clicks
  FROM sessions s
  GROUP BY s.user_id
),

-- FIX: keine Verwendung von f.cancellation (gibt es nicht). Storno-Logik kommt aus sessions (s.cancellation).
-- Flug-Buchungen zählen wir über s.flight_booked; Sitzplätze summieren wir aus flights.seats,
-- aber nur für nicht stornierte Sessions.
user_flight_counts AS (
  SELECT
    s.user_id,
    COUNT(DISTINCT CASE WHEN s.flight_booked THEN s.trip_id END)::numeric AS total_flight_bookings,
    COALESCE(SUM(
      CASE
        WHEN s.cancellation = FALSE THEN COALESCE(f.seats, 0)
        ELSE 0
      END
    ),0)::numeric AS total_seats
  FROM sessions s
  LEFT JOIN flights f ON f.trip_id = s.trip_id
  GROUP BY s.user_id
),

user_base AS (
  SELECT
    u.user_id,
    u.sign_up_date,
    u.has_children,
    COALESCE(uss.total_sessions,0)          AS total_sessions,
    COALESCE(uss.avg_page_clicks,0)         AS avg_page_clicks,
    COALESCE(ufc.total_flight_bookings,0)   AS total_flight_bookings,
    COALESCE(ufc.total_seats,0)             AS total_seats,
    CASE WHEN lb.last_booking_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_booking,
    CASE WHEN lb.last_booking_date IS NOT NULL
           AND (lb.last_booking_date::date - u.sign_up_date::date) <= 28
         THEN TRUE ELSE FALSE END AS is_new_customer,
    CASE WHEN COALESCE(uss.total_sessions,0) > 0
           AND COALESCE(uss.avg_page_clicks,0) > 0
         THEN TRUE ELSE FALSE END AS is_active
  FROM users u
  LEFT JOIN user_session_stats  uss ON uss.user_id = u.user_id
  LEFT JOIN user_flight_counts  ufc ON ufc.user_id = u.user_id
  LEFT JOIN last_booking        lb  ON lb.user_id  = u.user_id
),

ff_threshold AS (
  SELECT percentile_disc(0.9)
         WITHIN GROUP (ORDER BY total_flight_bookings) AS p90_flights
  FROM user_base
),

-- P10 nur über aktive Nutzer
p10_thresholds AS (
  SELECT
    percentile_cont(0.1) WITHIN GROUP (ORDER BY total_sessions)  AS p10_sessions,
    percentile_cont(0.1) WITHIN GROUP (ORDER BY avg_page_clicks) AS p10_clicks
  FROM user_base
  WHERE is_active = TRUE
),

user_based_prep AS (
  SELECT
    ub.*,
    CASE WHEN ub.total_flight_bookings > (SELECT p90_flights FROM ff_threshold)
         THEN TRUE ELSE FALSE END AS is_frequent_flyer
  FROM user_base ub
)

SELECT
  -- Counts
  COUNT(DISTINCT CASE WHEN ubp.total_seats >= 3 AND ubp.has_children = TRUE THEN ubp.user_id END) AS familien_users,
  COUNT(DISTINCT CASE WHEN ubp.is_frequent_flyer = TRUE THEN ubp.user_id END)                      AS frequent_flyers,
  COUNT(DISTINCT CASE WHEN ubp.is_new_customer  = TRUE THEN ubp.user_id END)                       AS new_customers,
  COUNT(DISTINCT CASE
                   WHEN ubp.has_booking = TRUE
                    AND ubp.is_active = TRUE
                    AND ubp.total_sessions  < p10.p10_sessions
                    AND ubp.avg_page_clicks < p10.p10_clicks
                   THEN ubp.user_id END) AS business_users,
  COUNT(DISTINCT ubp.user_id) AS all_users,

  -- Prozente (Basis = alle User)
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN ubp.total_seats >= 3 AND ubp.has_children = TRUE THEN ubp.user_id END)
        / NULLIF(COUNT(DISTINCT ubp.user_id), 0), 2) AS familien_pct,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN ubp.is_frequent_flyer = TRUE THEN ubp.user_id END)
        / NULLIF(COUNT(DISTINCT ubp.user_id), 0), 2) AS frequent_flyers_pct,
  ROUND(100.0 * COUNT(DISTINCT CASE WHEN ubp.is_new_customer  = TRUE THEN ubp.user_id END)
        / NULLIF(COUNT(DISTINCT ubp.user_id), 0), 2) AS new_customers_pct,
  ROUND(100.0 * COUNT(DISTINCT CASE
                               WHEN ubp.has_booking = TRUE
                                AND ubp.is_active = TRUE
                                AND ubp.total_sessions  < p10.p10_sessions
                                AND ubp.avg_page_clicks < p10.p10_clicks
                               THEN ubp.user_id END)
        / NULLIF(COUNT(DISTINCT ubp.user_id), 0), 2) AS business_pct
FROM user_based_prep ubp
CROSS JOIN p10_thresholds p10;