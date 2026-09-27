#!/usr/bin/env bash
# ==============================================================================
# Trino 交互与 SQL 执行工具 (trino.sh)
# 用法 1 (直接执行 SQL): ./scripts/trino.sh "SELECT * FROM iceberg.finance.raw_sms_records LIMIT 5"
# 用法 2 (执行 SQL 文件): ./scripts/trino.sh -f scripts/schema.sql
# 用法 3 (管道输入):     echo "SHOW TABLES FROM iceberg.finance" | ./scripts/trino.sh
# ==============================================================================

set -eo pipefail

TRINO_URL="${TRINO_URL:-https://trino.jppwl.asia}"
TRINO_USER="${TRINO_USER:-cindy-admin}"

if [ "$1" = "-f" ]; then
  shift
  node "$(dirname "$0")/execute-trino-sql.js" "$1"
  exit 0
fi

SQL="$1"
if [ -z "$SQL" ]; then
  SQL="$(cat)"
fi

if [ -z "$SQL" ]; then
  echo "Usage: $0 \"<SQL statement>\" or $0 -f <sql-file>"
  exit 1
fi

node -e "
async function run() {
  const trinoUrl = process.env.TRINO_URL || 'https://trino.jppwl.asia';
  const trinoUser = process.env.TRINO_USER || 'cindy-admin';
  const sql = process.argv[1];

  let res = await fetch(\`\${trinoUrl}/v1/statement\`, {
    method: 'POST',
    headers: { 'X-Trino-User': trinoUser },
    body: sql
  }).then(r => r.json());

  let allData = [];
  let columns = [];
  while (res.nextUri) {
    if (res.error) {
      console.error('❌ Trino Error:', res.error.message);
      process.exit(1);
    }
    if (res.columns && columns.length === 0) {
      columns = res.columns.map(c => c.name);
    }
    if (res.data) allData.push(...res.data);
    res = await fetch(res.nextUri).then(r => r.json());
  }

  if (res.error) {
    console.error('❌ Trino Error:', res.error.message);
    process.exit(1);
  }
  if (res.columns && columns.length === 0) {
    columns = res.columns.map(c => c.name);
  }
  if (res.data) allData.push(...res.data);

  if (allData.length > 0) {
    console.table(allData.map(row => {
      let obj = {};
      columns.forEach((col, idx) => obj[col] = row[idx]);
      return obj;
    }));
  } else {
    console.log('✅ Query executed successfully (0 rows returned or DDL completed).');
  }
}
run().catch(err => {
  console.error('❌ Request failed:', err.message);
  process.exit(1);
});
" "$SQL"
