#!/usr/bin/env node

/**
 * Trino SQL 文件执行工具
 * 语法: node scripts/execute-trino-sql.js [sql_file_path]
 * 环境变量: TRINO_URL (默认: https://trino.jppwl.asia)
 *          TRINO_USER (默认: schema-sync)
 */

const fs = require('fs');
const path = require('path');

const trinoUrl = process.env.TRINO_URL || 'https://trino.jppwl.asia';
const trinoUser = process.env.TRINO_USER || 'schema-sync';
const filePath = process.argv[2] || path.join(__dirname, 'schema.sql');

if (!fs.existsSync(filePath)) {
  console.error(`❌ SQL file not found: ${filePath}`);
  process.exit(1);
}

async function executeSql(sql) {
  let res = await fetch(`${trinoUrl}/v1/statement`, {
    method: 'POST',
    headers: { 'X-Trino-User': trinoUser },
    body: sql
  }).then(r => r.json());

  while (res.nextUri) {
    if (res.error) {
      throw new Error(`[${res.error.errorName}] ${res.error.message}`);
    }
    res = await fetch(res.nextUri).then(r => r.json());
  }

  if (res.error) {
    throw new Error(`[${res.error.errorName}] ${res.error.message}`);
  }
  return res;
}

async function main() {
  console.log('================================================================');
  console.log(`🚀 Executing SQL File on Trino: ${filePath}`);
  console.log(`🌐 Trino Endpoint: ${trinoUrl}`);
  console.log(`👤 Trino User: ${trinoUser}`);
  console.log('================================================================');

  const content = fs.readFileSync(filePath, 'utf-8');

  // 剔除单行注释 (-- 开头)
  const cleanContent = content
    .split('\n')
    .map(line => line.trim())
    .filter(line => !line.startsWith('--'))
    .join('\n');

  // 以分号拆分独立 SQL 语句
  const statements = cleanContent
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length > 0);

  console.log(`📦 Found ${statements.length} SQL statement(s) to execute.\n`);

  for (let i = 0; i < statements.length; i++) {
    const stmt = statements[i];
    console.log(`[Statement ${i + 1}/${statements.length}] Executing:\n${stmt}`);
    const start = Date.now();
    await executeSql(stmt);
    const duration = Date.now() - start;
    console.log(`✅ Statement ${i + 1} succeeded in ${duration}ms.\n`);
  }

  console.log('================================================================');
  console.log('🎉 All SQL statements executed successfully on Trino!');
  console.log('================================================================');
}

main().catch(err => {
  console.error('\n❌ Execution Failed:', err.message);
  process.exit(1);
});
