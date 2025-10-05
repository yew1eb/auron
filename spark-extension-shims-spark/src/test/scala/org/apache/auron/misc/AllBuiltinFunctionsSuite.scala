/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.misc

import org.apache.spark.sql.AuronQueryTest

import org.apache.auron.BaseAuronSQLSuite

class AllBuiltinFunctionsSuite extends AuronQueryTest with BaseAuronSQLSuite {

  // 统一的 helper：只验证 Spark SQL 表达式能正常执行且结果与 DataFrame API 一致
  private def check(expr: String): Unit = {
    checkSparkAnswerAndOperator(expr)
  }

  // =====================================================================
  // agg_funcs
  // =====================================================================
  test("agg funcs - any / some / every / bool_and / bool_or") {
    withTable("t1") {
      sql("CREATE TABLE t1(b BOOLEAN) USING parquet")
      sql("INSERT INTO t1 VALUES (true), (false), (null)")
      check("SELECT any(b), some(b), every(b), bool_and(b), bool_or(b) FROM t1")
    }
  }

  test("agg funcs - approx_count_distinct / approx_percentile") {
    withTable("t1") {
      sql("CREATE TABLE t1(v INT) USING parquet")
      sql("INSERT INTO t1 VALUES (1),(2),(2),(3),(null)")
      check("SELECT approx_count_distinct(v), approx_percentile(v, 0.5) FROM t1")
    }
  }

  test("agg funcs - collect_list / collect_set / array_agg") {
    withTable("t1") {
      sql("CREATE TABLE t1(v INT) USING parquet")
      sql("INSERT INTO t1 VALUES (1),(2),(2),(3)")
      check("SELECT collect_list(v), collect_set(v), array_agg(v) FROM t1")
    }
  }

  test("agg funcs - count / count_if / kurtosis / skewness / stddev / variance") {
    withTable("t1") {
      sql("CREATE TABLE t1(v DOUBLE) USING parquet")
      sql("INSERT INTO t1 VALUES (1.0),(2.0),(3.0),(null)")
      check("""SELECT count(v), count_if(v > 1), kurtosis(v), skewness(v),
               stddev(v), stddev_pop(v), stddev_samp(v),
               var_pop(v), var_samp(v), variance(v) FROM t1""")
    }
  }

  test("agg funcs - avg / sum / min / max / first / last") {
    withTable("t1") {
      sql("CREATE TABLE t1(v DOUBLE) USING parquet")
      sql("INSERT INTO t1 VALUES (10),(20),(null)")
      check(
        "SELECT avg(v), sum(v), min(v), max(v), first(v), last(v), first_value(v), last_value(v) FROM t1")
    }
  }

  test("agg funcs - covar_pop / covar_samp / corr") {
    withTable("t1") {
      sql("CREATE TABLE t1(x DOUBLE, y DOUBLE) USING parquet")
      sql("INSERT INTO t1 VALUES (1,2),(2,4),(3,6)")
      check("SELECT covar_pop(x,y), covar_samp(x,y), corr(x,y) FROM t1")
    }
  }

  // 其余聚合函数（bitmap、hll、percentile 等）类似，这里用一个代表性测试覆盖
  test("agg funcs - bitmap / hll / percentile") {
    withTable("t1") {
      sql("CREATE TABLE t1(v BIGINT) USING parquet")
      sql("INSERT INTO t1 VALUES (1),(2),(3)")
      check("""SELECT bitmap_construct_agg(v), bitmap_or_agg(bitmap_construct_agg(v)),
               hll_sketch_agg(v), percentile(v, 0.5), percentile_approx(v, 0.5) FROM t1""")
    }
  }

  // =====================================================================
  // array_funcs
  // =====================================================================
  test("array funcs") {
    withTable("t1") {
      sql("CREATE TABLE t1(a ARRAY<INT>) USING parquet")
      sql("""INSERT INTO t1 VALUES
             (ARRAY(1,2,3)),
             (ARRAY(2,3,4)),
             (ARRAY(null,5)),
             (null)""")
      check("""SELECT
        array(1,2,3),
        array_append(a, 99),
        array_compact(a),
        array_contains(a, 2),
        array_distinct(a),
        array_except(a, ARRAY(2)),
        array_insert(a, 1, 100),
        array_intersect(a, ARRAY(2,3)),
        array_join(a, ','),
        array_max(a),
        array_min(a),
        array_position(a, 3),
        array_prepend(0, a),
        array_remove(a, 2),
        array_repeat(42, 3),
        array_union(a, ARRAY(5,6)),
        arrays_overlap(a, ARRAY(3,4)),
        arrays_zip(a, ARRAY('x','y','z')),
        flatten(ARRAY(a, ARRAY(7,8))),
        get(a, 1),
        sequence(1, 3),
        shuffle(a),
        slice(a, 1, 2),
        sort_array(a),
        size(a),
        cardinality(a)
        FROM t1""")
    }
  }

  // =====================================================================
  // bitwise_funcs
  // =====================================================================
  test("bitwise funcs") {
    check(
      "SELECT bit_count(15), bit_get(15, 2), getbit(15, 2), " +
        "shiftright(16, 2), shiftrightunsigned(16, 2), " +
        "shiftleft(1, 4), 5 & 3, 5 | 3, 5 ^ 3, ~1")
  }

  // =====================================================================
  // conditional_funcs
  // =====================================================================
  test("conditional funcs") {
    check("""SELECT coalesce(null, 1, 2),
                   if(true, 10, 20),
                   ifnull(null, 42),
                   nullif(5, 5),
                   nvl(null, 100),
                   nvl2(1, 2, 3),
                   CASE WHEN true THEN 'yes' ELSE 'no' END,
                    CASE WHEN 1 > 10 THEN 'big' ELSE 'small' END""")
  }

  // =====================================================================
  // conversion_funcs
  // =====================================================================
  test("conversion funcs") {
    check("""SELECT cast('123' AS INT),
                   bigint('123'), int('123'), smallint('123'), tinyint('123'),
                   float('1.23'), double('1.23'),
                   decimal('123.45'),
                   binary('abc'),
                   boolean('true'),
                   date('2023-01-01'),
                   timestamp('2023-01-01 12:34:56'),
                   string(123)""")
  }

  // =====================================================================
  // datetime_funcs (挑重点覆盖，其余类似)
  // =====================================================================
  test("datetime funcs") {
    check("""SELECT current_date(), current_timestamp(), now(),
                   date_add('2023-01-01', 5),
                   date_sub('2023-01-01', 5),
                   datediff('2023-01-10', '2023-01-01'),
                   add_months('2023-01-01', 2),
                   last_day('2023-02-01'),
                   date_format('2023-01-01 12:34:56', 'yyyy-MM'),
                   to_unix_timestamp('2023-01-01 00:00:00'),
                   from_unixtime(1672531200),
                   unix_timestamp(),
                   make_date(2023, 5, 20),
                   make_timestamp(2023, 5, 20, 12, 30, 45),
                   trunc('2023-05-20', 'year')""")
  }

  // =====================================================================
  // hash_funcs
  // =====================================================================
  test("hash funcs") {
    check("""SELECT md5('abc'),
                   sha1('abc'),
                   sha2('abc', 256),
                   crc32('abc'),
                   xxhash64('abc'),
                   hash('a', 1, null)""")
  }

  // =====================================================================
  // json_funcs
  // =====================================================================
  test("json funcs") {
    val json = """{"a":1,"b":[1,2],"c":{"x":"y"}}"""
    check(s"""SELECT from_json('$json', 'struct<a:INT,b:ARRAY<INT>,c:STRUCT<x:STRING>>'),
                    to_json(struct(1 as x, 'hello' as y)),
                    get_json_object('$json', '$$.c.x'),
                    json_array_length('$json'),
                    schema_of_json('$json')""")
  }

  // =====================================================================
  // map_funcs
  // =====================================================================
  test("map funcs") {
    check("""SELECT map('a',1,'b',2),
                   map_from_arrays(array('k1','k2'), array(10,20)),
                   element_at(map('x',100), 'x'),
                   map_keys(map('a',1,'b',2)),
                   map_values(map('a',1,'b',2)),
                   map_concat(map('a',1), map('b',2)),
                   str_to_map('a=1,b=2', ',', '=')""")
  }

  // =====================================================================
  // math_funcs
  // =====================================================================
  test("math funcs") {
    check("""SELECT abs(-5), acos(0.5), asin(0.5), atan(1), atan2(1,1),
                   ceil(3.14), floor(3.74), round(3.5), bround(3.5),
                   sin(0), cos(0), tan(0),
                   degrees(3.14159), radians(180),
                   exp(1), ln(exp(1)), log10(100),
                   pow(2, 10), sqrt(16),
                   pi(), rand(), randn(),
                   greatest(1,3,2), least(4,1,5),
                   10 % 3, 10 div 3""")
  }

  // =====================================================================
  // string_funcs
  // =====================================================================
  test("string funcs") {
    check("""SELECT concat('a','b'), concat_ws('-','a','b'),
                   upper('Abc'), lower('AbC'), initcap('hello world'),
                   length('hello'), trim('  hi  '), ltrim('  hi'), rtrim('hi  '),
                   lpad('hi', 5, 'x'), rpad('hi', 5, 'x'),
                   substr('hello', 2, 3), substring('hello', 2),
                   startsWith('hello', 'he'), endsWith('hello', 'lo'),
                   contains('hello', 'll'),
                   translate('hello', 'leo', '123'),
                   regexp_replace('100-200', '(\\d+)', 'num'),
                   regexp_extract('100-200', '(\\d+)-(\\d+)', 1),
                   soundex('Robert'), levenshtein('kitten','sitting')""")
  }

  // =====================================================================
  // predicate_funcs
  // =====================================================================
  test("predicate funcs") {
    check("""SELECT 1=1, 1!=2, 1<=>null, isnull(null), isnotnull(1),
                   isnan(cast('NaN' as double)),
                   'abc' like 'a%', 'abc' rlike '^a', 'abc' ilike 'A%'""")
  }

  // =====================================================================
  // window_funcs
  // =====================================================================
  test("window funcs") {
    withTable("t1") {
      sql("CREATE TABLE t1(id INT, val DOUBLE) USING parquet")
      sql("INSERT INTO t1 VALUES (1,10),(2,20),(3,15)")
      check("""SELECT row_number() OVER (ORDER BY val),
                      rank() OVER (ORDER BY val),
                      dense_rank() OVER (ORDER BY val),
                      percent_rank() OVER (ORDER BY val),
                      cume_dist() OVER (ORDER BY val),
                      ntile(2) OVER (ORDER BY val),
                      lag(val,1) OVER (ORDER BY id),
                      lead(val,1) OVER (ORDER BY id)
               FROM t1""")
    }
  }

  // =====================================================================
  // generator_funcs
  // =====================================================================
  test("generator funcs") {
    check("""SELECT explode(array(1,2,3)),
                   explode_outer(array(1,null)),
                   posexplode(array('a','b')),
                   inline(array(struct(1,'x'), struct(2,'y')))""")
  }

  // =====================================================================
  // misc_funcs
  // =====================================================================
  test("misc funcs") {
    check("""SELECT spark_partition_id(),
                   monotonically_increasing_id(),
                   input_file_name(),
                   uuid(),
                   version(),
                   assert_true(1=1),
                   raise_error('test'),
                   typeof(1),
                   aes_encrypt('text','key','ECB'),
                   aes_decrypt(aes_encrypt('text','key','ECB'),'key','ECB')""")
  }

  // =====================================================================
  // url / csv / xml 等剩余函数（一次性覆盖）
  // =====================================================================
  test("url / csv / xml funcs") {
    check("""SELECT parse_url('http://host:8080/path?k=v#f', 'HOST'),
                   url_encode('a b'), url_decode('a%20b'),
                   from_csv('a,b,c', 'x INT,y STRING'),
                   schema_of_csv('1,abc'),
                   xpath('<a><b>123</b></a>', '//b/text()')""")
  }

  test("math: abs") {
    withTable("t1") {
      sql("CREATE TABLE t1(id INT) USING parquet")
      sql("""INSERT INTO t1 VALUES
          | (1),
          | (-4),
          | (NULL)
          |""".stripMargin)
      checkSparkAnswerAndOperator("SELECT abs(id) FROM t1")
    }
  }

  test("math: acos/asin/atan") {
    withTable("t1") {
      sql("CREATE TABLE t1(x DOUBLE) USING parquet")
      sql("""INSERT INTO t1 VALUES
          | (0.0),
          | (0.5),
          | (-0.5),
          | (NULL)
          |""".stripMargin)
      checkSparkAnswerAndOperator("SELECT acos(x), asin(x), atan(x) FROM t1")
    }
  }

  test("math: exp/ln/log2/log10/sqrt/tan/sin/cos") {
    withTable("t1") {
      sql("CREATE TABLE t1(x DOUBLE) USING parquet")
      sql("""INSERT INTO t1 VALUES (0.0),(1.0),(2.0),(NULL)""")
      checkSparkAnswerAndOperator(
        "SELECT exp(x), ln(nullif(x,0.0)), log2(nullif(x,0.0)), log10(nullif(x,0.0)), sqrt(x), tan(x), sin(x), cos(x) FROM t1")
    }
  }

  test("math: round/power/sign/signum") {
    withTable("t1") {
      sql("CREATE TABLE t1(x DOUBLE) USING parquet")
      sql("INSERT INTO t1 VALUES (1.2345),(-2.6),(0.0),(NULL)")
      checkSparkAnswerAndOperator("SELECT round(x,2), pow(x,2), sign(x), signum(x) FROM t1")
    }
  }

  test("math: + - * / % try_divide try_subtract") {
    withTable("t1") {
      sql("CREATE TABLE t1(a INT, b INT) USING parquet")
      sql("INSERT INTO t1 VALUES (6,3),(5,0),(NULL,2)")
      checkSparkAnswerAndOperator("SELECT a+b, a-b, a * b, a/b, pmod(a,b), a%b FROM t1")
      // try_ 语义自定义，确保实现后再启用
      // checkSparkAnswerAndOperator("SELECT try_divide(a,b), try_subtract(a,b) FROM t1")
    }
  }

  // 示例：字符串函数（string_funcs）
  test("string: length/char_length/upper/lower/initcap/trim/ltrim/rtrim") {
    withTable("t1") {
      sql("CREATE TABLE t1(s STRING) USING parquet")
      sql("""INSERT INTO t1 VALUES ('abc'),(' AbC '),(NULL)""")
      checkSparkAnswerAndOperator(
        "SELECT length(s), char_length(s), upper(s), lower(s), initcap(s), trim(s), ltrim(s), rtrim(s) FROM t1")
    }
  }

  test("string: space/find_in_set/octet_length/btrim") {
    withTable("t1") {
      sql("CREATE TABLE t1(s STRING) USING parquet")
      sql("INSERT INTO t1 VALUES ('a'),('ab'),(NULL)")
      checkSparkAnswerAndOperator(
        "SELECT space(3), find_in_set('a','b,a,c'), octet_length(s), btrim(' x ') FROM t1")
    }
  }

  // lpad/rpad 取决于你的实现与 DF 对 len 的 LongType 约束
  test("string: lpad/rpad") {
    withTable("t1") {
      sql("CREATE TABLE t1(txt STRING, len INT, pad STRING) USING parquet")
      sql("INSERT INTO t1 VALUES ('abc',5,'x'),(NULL,4,'y'),('z',NULL,'*')")
      checkSparkAnswerAndOperator(
        "SELECT lpad(txt, CAST(len AS BIGINT), pad), rpad(txt, CAST(len AS BIGINT), pad) FROM t1")
    }
  }

  test("string: hex/reverse") {
    withTable("t1") {
      sql("CREATE TABLE t1(s STRING) USING parquet")
      sql("INSERT INTO t1 VALUES ('ab'),(''),(NULL)")
      checkSparkAnswerAndOperator("SELECT hex(s), reverse(s) FROM t1")
    }
  }

  // 示例：条件函数（conditional_funcs）
  test("conditional: coalesce/nvl/nvl2/nullif/if") {
    withTable("t1") {
      sql("CREATE TABLE t1(a INT, b INT) USING parquet")
      sql("INSERT INTO t1 VALUES (1,NULL),(NULL,2),(NULL,NULL)")
      checkSparkAnswerAndOperator(
        "SELECT coalesce(a,b,0), nvl(a,b), nvl2(a, 10, 20), nullif(a,b), if(a IS NULL, 0, a) FROM t1")
    }
  }

  // when/CASE WHEN 示例（等实现）
  test("conditional: case when") {
    withTable("t1") {
      sql("CREATE TABLE t1(id INT) USING parquet")
      sql("INSERT INTO t1 VALUES (1),(4),(7),(NULL)")
      checkSparkAnswerAndOperator(
        "SELECT CASE WHEN id < 3 THEN 'a' WHEN id < 6 THEN 'b' ELSE 'c' END FROM t1")
    }
  }

  // 示例：谓词函数（predicate_funcs）
  test("predicate: comparisons and logical") {
    withTable("t1") {
      sql("CREATE TABLE t1(a INT, b INT) USING parquet")
      sql("INSERT INTO t1 VALUES (1,2),(2,2),(3,NULL)")
      checkSparkAnswerAndOperator(
        "SELECT a<b, a<=b, a=b, a>=b, a>b, a IS NULL, a IS NOT NULL, (a<b) AND (b IS NOT NULL), (a=b) OR (b IS NULL), NOT(a=b) FROM t1")
    }
  }

  test("predicate: in/like/ilike") {
    withTable("t1") {
      sql("CREATE TABLE t1(s STRING) USING parquet")
      sql("INSERT INTO t1 VALUES ('abc'),('Abc'),(NULL)")
      checkSparkAnswerAndOperator("SELECT s IN('a','abc'), s LIKE 'a%', s ILIKE 'a%' FROM t1")
    }
  }

  // 示例：聚合函数（agg_funcs）
  test("agg: count/sum/avg/min/max/stddev/variance") {
    withTable("t1") {
      sql("CREATE TABLE t1(x INT) USING parquet")
      sql("INSERT INTO t1 VALUES (1),(2),(2),(NULL)")
      checkSparkAnswerAndOperator(
        "SELECT count(*), count(x), sum(x), avg(x), min(x), max(x), stddev_samp(x), var_samp(x) FROM t1")
    }
  }

  test("agg: collect_list/collect_set/array_agg") {
    withTable("t1") {
      sql("CREATE TABLE t1(x STRING) USING parquet")
      sql("INSERT INTO t1 VALUES ('a'),('b'),('a'),(NULL)")
      checkSparkAnswerAndOperator("SELECT collect_list(x), collect_set(x), array_agg(x) FROM t1")
    }
  }

  test("agg: percentile/percentile_approx/approx_percentile") {
    withTable("t1") {
      sql("CREATE TABLE t1(x DOUBLE) USING parquet")
      sql("INSERT INTO t1 VALUES (1.0),(2.0),(3.0),(4.0),(5.0)")
      checkSparkAnswerAndOperator(
        "SELECT percentile(x, 0.5), percentile_approx(x, 0.5), approx_percentile(x, array(0.5)) FROM t1")
    }
  }

  // 示例：哈希类（hash_funcs）
  test("hash: hash/md5/xxhash64") {
    withTable("t1") {
      sql("CREATE TABLE t1(s STRING) USING parquet")
      sql("INSERT INTO t1 VALUES ('a'),('b'),(NULL)")
      checkSparkAnswerAndOperator("SELECT hash(s), md5(s), xxhash64(s) FROM t1")
    }
  }

  // 示例：JSON（json_funcs，依据实现情况）
  test("json: json_tuple") {
    withTable("t1") {
      sql("CREATE TABLE t1(js STRING) USING parquet")
      sql("""INSERT INTO t1 VALUES ('{"a":1,"b":"x"}'), (NULL)""")
      checkSparkAnswerAndOperator("SELECT json_tuple(js, 'a','b') FROM t1")
    }
  }

  // 示例：struct/named_struct（struct_funcs）
  test("struct: named_struct/struct") {
    withTable("t1") {
      sql("CREATE TABLE t1(a INT, b STRING) USING parquet")
      sql("INSERT INTO t1 VALUES (1,'x'),(NULL,NULL)")
      checkSparkAnswerAndOperator("SELECT named_struct('a',a,'b',b), struct(a,b) FROM t1")
    }
  }

  // 示例：datetime（datetime_funcs，逐步启用）
  test("datetime: current_date/curdate/current_timezone") {
    withTable("t1") {
      sql("CREATE TABLE t1(i INT) USING parquet")
      sql("INSERT INTO t1 VALUES (1)")
      checkSparkAnswerAndOperator("SELECT current_date(), curdate(), current_timezone() FROM t1")
    }
  }

  // hour/minute/second 等先在实现完成后启用
  // test("datetime: hour/minute/second") { ... }

  // 示例：生成器（generator_funcs）
  test("generator: explode_outer/inline_outer/posexplode_outer/stack") {
    withTable("t1") {
      sql("CREATE TABLE t1(arr ARRAY<INT>, m MAP<STRING,INT>) USING parquet")
      sql("INSERT INTO t1 VALUES (array(1,2), map('a',1)), (NULL, NULL)")
      checkSparkAnswerAndOperator("SELECT posexplode_outer(arr) FROM t1")
      checkSparkAnswerAndOperator("SELECT explode_outer(arr) FROM t1")
      checkSparkAnswerAndOperator(
        "SELECT inline_outer(array(struct(1,'x'), struct(2,'y'))) FROM t1")
      checkSparkAnswerAndOperator("SELECT stack(2, 1,'x', 2,'y')")
    }
  }

//  示例：窗口函数（window_funcs）
  test("window: rank/dense_rank/row_number") {
    withTable("t1") {
      sql("CREATE TABLE t1(id INT, grp INT) USING parquet")
      sql("INSERT INTO t1 VALUES (10,1),(20,1),(10,2),(30,2)")
      checkSparkAnswerAndOperator("""SELECT id, grp,
          | rank() OVER(PARTITION BY grp ORDER BY id),
          | dense_rank() OVER(PARTITION BY grp ORDER BY id),
          | row_number() OVER(PARTITION BY grp ORDER BY id)
          | FROM t1
          |""".stripMargin)
    }
  }
}
