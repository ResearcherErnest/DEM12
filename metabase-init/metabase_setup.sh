#!/bin/sh
set -e

MB_URL="http://metabase:3000"
MB_EMAIL="${MB_ADMIN_EMAIL:-admin@data-platform.local}"
MB_PASS="${MB_ADMIN_PASSWORD:-change_me_mb_admin}"
DB_PASS="${MB_DB_PASS:-change_me_metabase}"

# â”€â”€ 1. Wait for Metabase healthy â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo "==> Waiting for Metabase to be healthy..."
until curl -sf "${MB_URL}/api/health" > /dev/null 2>&1; do
  sleep 5
done
echo "==> Metabase is healthy."

# â”€â”€ 2. Setup (first run) or login (subsequent runs) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
HAS_SETUP=$(curl -s "${MB_URL}/api/session/properties" | jq -r '."has-user-setup"')

if [ "$HAS_SETUP" = "true" ]; then
  echo "==> Already set up â€” logging in as ${MB_EMAIL}..."
  SESSION_RESP=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"${MB_EMAIL}\",\"password\":\"${MB_PASS}\"}" \
    "${MB_URL}/api/session")
  SESSION_ID=$(echo "$SESSION_RESP" | jq -r '.id')
else
  echo "==> Fresh instance â€” running initial setup..."
  SETUP_TOKEN=$(curl -s "${MB_URL}/api/session/properties" | jq -r '."setup-token"')
  SETUP_RESP=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "$(jq -n \
      --arg token  "$SETUP_TOKEN" \
      --arg email  "$MB_EMAIL"    \
      --arg pass   "$MB_PASS"     \
      --arg dbpass "$DB_PASS"     \
      '{
        token: $token,
        user: {
          email: $email, first_name: "Admin", last_name: "User",
          password: $pass, site_name: "Data Platform"
        },
        database: {
          engine: "postgres", name: "sales",
          details: {
            host: "postgres", port: 5432, dbname: "sales",
            user: "metabase_user", password: $dbpass
          },
          is_full_sync: true, is_on_demand: false, cache_ttl: null
        },
        prefs: { site_name: "Data Platform", site_locale: "en", allow_tracking: false }
      }')" \
    "${MB_URL}/api/setup")
  SESSION_ID=$(echo "$SETUP_RESP" | jq -r '.id')
  echo "==> Initial setup complete."
fi

if [ -z "$SESSION_ID" ] || [ "$SESSION_ID" = "null" ]; then
  echo "ERROR: Could not obtain Metabase session ID. Aborting."
  exit 1
fi
echo "==> Session acquired."

mb_get()  { curl -s      -H "X-Metabase-Session: $SESSION_ID" "$MB_URL$1"; }
mb_put()  { curl -s -X PUT  -H "X-Metabase-Session: $SESSION_ID" \
              -H "Content-Type: application/json" -d "$2" "$MB_URL$1"; }
mb_post() {
  _body="${2:-{}}"
  curl -s -X POST -H "X-Metabase-Session: $SESSION_ID" \
    -H "Content-Type: application/json" -d "$_body" "$MB_URL$1"
}

# â”€â”€ 3. Ensure 'sales' DB connection exists â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo "==> Checking sales DB connection..."
SALES_DB_ID=$(mb_get "/api/database" \
  | jq -r '.data[] | select(.name=="sales") | .id' | head -1)

if [ -z "$SALES_DB_ID" ] || [ "$SALES_DB_ID" = "null" ]; then
  echo "==> Adding 'sales' DB connection..."
  SALES_DB_ID=$(mb_post "/api/database" \
    "$(jq -n --arg p "$DB_PASS" '{
       name: "sales", engine: "postgres",
       details: {
         host: "postgres", port: 5432, dbname: "sales",
         user: "metabase_user", password: $p
       }
     }')" | jq -r '.id')
fi
echo "==> sales DB id: $SALES_DB_ID"

# â”€â”€ 4. Wait for DB schema sync â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo "==> Waiting for DB schema sync..."
i=0
while [ "$i" -lt 30 ]; do
  SYNC=$(mb_get "/api/database/$SALES_DB_ID" | jq -r '.initial_sync_status')
  [ "$SYNC" = "complete" ] && echo "==> Sync complete." && break
  echo "   sync status: $SYNC (attempt $i/30)..."
  sleep 5
  i=$((i + 1))
done

# â”€â”€ 5. Create 'Sales Analytics' collection (idempotent) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
COLL_ID=$(mb_get "/api/collection" \
  | jq -r '.[] | select(.name=="Sales Analytics") | .id' | head -1)
if [ -z "$COLL_ID" ] || [ "$COLL_ID" = "null" ]; then
  COLL_ID=$(mb_post "/api/collection" \
    '{"name":"Sales Analytics","color":"#509EE3"}' | jq -r '.id')
fi
echo "==> Collection id: $COLL_ID"

# â”€â”€ 6. Card helper â€” idempotent â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
create_card() {
  _name="$1"; _display="$2"; _query="$3"; _viz="$4"
  EXISTING_ID=$(mb_get "/api/card" \
    | jq -r --arg n "$_name" '.[] | select(.name==$n) | .id' | head -1)
  if [ -n "$EXISTING_ID" ] && [ "$EXISTING_ID" != "null" ]; then
    echo "$EXISTING_ID"; return
  fi
  mb_post "/api/card" "$(jq -n \
    --arg  name    "$_name"        \
    --arg  display "$_display"     \
    --arg  query   "$_query"       \
    --argjson db   "$SALES_DB_ID"  \
    --argjson viz  "$_viz"         \
    --argjson coll "$COLL_ID"      \
    '{
      name: $name,
      display: $display,
      collection_id: $coll,
      dataset_query: {
        type: "native",
        native: { query: $query },
        database: $db
      },
      visualization_settings: $viz
    }')" | jq -r '.id'
}

# â”€â”€ 7. Create cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo "==> Creating cards..."

C1=$(create_card "Monthly Revenue Trend" "bar" \
  "SELECT DATE_TRUNC('month', order_date) AS month, SUM(total_revenue) AS monthly_revenue FROM orders WHERE status = 'completed' GROUP BY month ORDER BY month ASC;" \
  '{"graph.dimensions":["month"],"graph.metrics":["monthly_revenue"],"graph.x_axis.title_text":"Month","graph.y_axis.title_text":"Revenue (USD)"}')
echo "   [1]  Monthly Revenue Trend     -> $C1"

C2=$(create_card "Top 10 Products" "bar" \
  "SELECT product_name, total_revenue, total_units_sold FROM purchased_products ORDER BY total_revenue DESC LIMIT 10;" \
  '{"graph.dimensions":["product_name"],"graph.metrics":["total_revenue"]}')
echo "   [2]  Top 10 Products           -> $C2"

C3=$(create_card "Customer Insights" "table" \
  "SELECT name, email, region, signup_date, lifetime_value FROM customers ORDER BY lifetime_value DESC LIMIT 20;" \
  '{}')
echo "   [3]  Customer Insights         -> $C3"

C4=$(create_card "Sales by Region" "pie" \
  "SELECT region, SUM(total_revenue) AS revenue FROM orders WHERE status = 'completed' GROUP BY region ORDER BY revenue DESC;" \
  '{"pie.dimension":"region","pie.metric":"revenue"}')
echo "   [4]  Sales by Region           -> $C4"

C5=$(create_card "Overall Return Rate (%)" "scalar" \
  "SELECT ROUND(COUNT(DISTINCT r.order_id)::NUMERIC / NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 2) AS return_rate_pct FROM orders o LEFT JOIN returned_orders r ON r.order_id = o.order_id WHERE o.status = 'completed';" \
  '{"scalar.show_mini_bar":true}')
echo "   [5a] Overall Return Rate       -> $C5"

C6=$(create_card "Total Returned Orders" "scalar" \
  "SELECT COUNT(*) AS total_returns FROM returned_orders;" \
  '{"scalar.show_mini_bar":true}')
echo "   [5b] Total Returned Orders     -> $C6"

C7=$(create_card "Total Completed Orders" "scalar" \
  "SELECT COUNT(*) AS total_orders FROM orders WHERE status = 'completed';" \
  '{"scalar.show_mini_bar":true}')
echo "   [5c] Total Completed Orders    -> $C7"

C8=$(create_card "Monthly Return Rate Trend" "line" \
  "SELECT DATE_TRUNC('month', o.order_date) AS month, COUNT(DISTINCT o.order_id) AS total_orders, COUNT(DISTINCT r.order_id) AS total_returns, ROUND(COUNT(DISTINCT r.order_id)::NUMERIC / NULLIF(COUNT(DISTINCT o.order_id), 0) * 100, 2) AS return_rate_pct FROM orders o LEFT JOIN returned_orders r ON r.order_id = o.order_id WHERE o.status = 'completed' GROUP BY month ORDER BY month ASC;" \
  '{"graph.dimensions":["month"],"graph.metrics":["return_rate_pct"],"graph.y_axis.title_text":"Return Rate (%)"}')
echo "   [5d] Monthly Return Rate Trend -> $C8"

C9=$(create_card "Pipeline Log" "table" \
  "SELECT run_id, started_at, finished_at, dag_run_id, file_processed, status, rows_inserted, rows_skipped FROM pipeline_runs ORDER BY started_at DESC LIMIT 50;" \
  '{}')
echo "   [6]  Pipeline Log              -> $C9"

# â”€â”€ 8. Create dashboard (idempotent) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
DASH_ID=$(mb_get "/api/dashboard" \
  | jq -r '.[] | select(.name=="Sales Overview") | .id' | head -1)
if [ -z "$DASH_ID" ] || [ "$DASH_ID" = "null" ]; then
  DASH_ID=$(mb_post "/api/dashboard" \
    "$(jq -n --argjson c "$COLL_ID" '{
       name: "Sales Overview",
       description: "End-to-end sales KPIs and pipeline monitoring",
       collection_id: $c
     }')" | jq -r '.id')
fi
echo "==> Dashboard id: $DASH_ID"

# â”€â”€ 9. Place all cards using v49+ PUT /api/dashboard/:id/cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
EXISTING=$(mb_get "/api/dashboard/$DASH_ID" | jq '.dashcards | length')
if [ "${EXISTING:-0}" = "0" ]; then
  echo "==> Placing cards on dashboard (v49+ PUT API)..."
  CARDS_JSON=$(jq -n \
    --argjson c1 "$C1" --argjson c2 "$C2" --argjson c3 "$C3" \
    --argjson c4 "$C4" --argjson c5 "$C5" --argjson c6 "$C6" \
    --argjson c7 "$C7" --argjson c8 "$C8" --argjson c9 "$C9" \
    '{cards:[
      {id:-1,  card_id:$c1, row:0,  col:0,  size_x:16, size_y:6},
      {id:-2,  card_id:$c4, row:0,  col:16, size_x:8,  size_y:6},
      {id:-3,  card_id:$c5, row:6,  col:0,  size_x:8,  size_y:4},
      {id:-4,  card_id:$c6, row:6,  col:8,  size_x:8,  size_y:4},
      {id:-5,  card_id:$c7, row:6,  col:16, size_x:8,  size_y:4},
      {id:-6,  card_id:$c2, row:10, col:0,  size_x:12, size_y:6},
      {id:-7,  card_id:$c3, row:10, col:12, size_x:12, size_y:6},
      {id:-8,  card_id:$c8, row:16, col:0,  size_x:16, size_y:6},
      {id:-9,  card_id:$c9, row:16, col:16, size_x:8,  size_y:6}
    ]}')
  PUT_RESP=$(mb_put "/api/dashboard/$DASH_ID/cards" "$CARDS_JSON")
  PLACED=$(echo "$PUT_RESP" | jq '.cards | length')
  echo "==> Placed $PLACED cards on dashboard."
else
  echo "==> Dashboard already has ${EXISTING} card(s) â€” skipping layout."
fi

# â”€â”€ 10. Enable public sharing â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
echo "==> Enabling public sharing globally..."
mb_put "/api/setting/enable-public-sharing" '{"value":true}' > /dev/null

PUBLIC_UUID=$(mb_get "/api/dashboard/$DASH_ID" | jq -r '.public_uuid')
if [ -z "$PUBLIC_UUID" ] || [ "$PUBLIC_UUID" = "null" ]; then
  PUBLIC_UUID=$(curl -s -X POST \
    -H "X-Metabase-Session: $SESSION_ID" \
    "${MB_URL}/api/dashboard/${DASH_ID}/public_link" | jq -r '.uuid')
fi

echo ""
echo "========================================================================"
echo "  Sales Overview dashboard is live!"
echo "  Admin UI : http://localhost:3000"
if [ -n "$PUBLIC_UUID" ] && [ "$PUBLIC_UUID" != "null" ]; then
  echo "  Public   : http://localhost:3000/public/dashboard/$PUBLIC_UUID"
fi
echo "========================================================================"
echo "==> metabase-init complete."