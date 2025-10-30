import { parseSqlInterval, getEnv } from '@cubejs-backend/shared';
import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import type { BaseDimension } from './BaseDimension';

const GRANULARITY_VALUE = {
  day: 'DD',
  week: 'IW',
  hour: 'HH24',
  minute: 'mm',
  second: 'ss',
  month: 'MM',
  year: 'YYYY'
};

class OracleFilter extends BaseFilter {
  public castParameter() {
    return ':"?"';
  }

  /**
   * "ILIKE" is not supported
   */
  public likeIgnoreCase(column, not, param, type) {
    const p = (!type || type === 'contains' || type === 'ends') ? '\'%\' || ' : '';
    const s = (!type || type === 'contains' || type === 'starts') ? ' || \'%\'' : '';
    return `${column}${not ? ' NOT' : ''} LIKE ${p}${this.allocateParam(param)}${s}`;
  }
}

export class OracleQuery extends BaseQuery {
  protected resolvedRowLimit(): string | number | null | undefined {
    if (typeof this.rowLimit !== 'undefined') {
      return this.rowLimit;
    }

    return this.options?.limit;
  }

  protected parseRowLimit(rawLimit: string | number | null | undefined): number {
    if (rawLimit === null || typeof rawLimit === 'undefined') {
      return 10000;
    }

    const parsed = typeof rawLimit === 'number' ? rawLimit : parseInt(rawLimit, 10);
    return Number.isFinite(parsed) ? parsed : 10000;
  }

  /**
   * Checks if this Oracle version supports OFFSET/FETCH syntax.
   * Oracle 12.1+: Supports OFFSET/FETCH
   * Oracle 11g and below: Does not support OFFSET/FETCH (must use ROWNUM)
   *
   * By default, assumes Oracle 12c+ (modern version, 95%+ of users).
   * For Oracle 11g users, set environment variable: CUBEJS_DB_ORACLE_VERSION=11.2
   *
   * @returns {boolean}
   */
  protected supportsOffsetFetch(): boolean {
    // Check environment variable (only Oracle 11g users need to set this)
    const envVersion = getEnv('oracleVersion', { dataSource: this.dataSource });
    if (envVersion) {
      const parts = envVersion.split('.').map(v => parseInt(v, 10));
      const major = parts[0] || 12;
      return major >= 12;
    }

    // Default: Support modern OFFSET/FETCH syntax (Oracle 12c+)
    return true;
  }

  /**
   * "LIMIT" on Oracle is illegal
   * TODO replace with limitOffsetClause override
   * Oracle 12c+ uses OFFSET/FETCH syntax
   * Oracle 11g uses ROWNUM-based pagination which requires query wrapping
   */
  public groupByDimensionLimit() {
    // Check if Oracle version supports OFFSET/FETCH
    if (!this.supportsOffsetFetch()) {
      // Oracle 11g uses ROWNUM, which is handled via query wrapping in buildSqlAndParams
      // Return empty string here as pagination will be applied by wrapping the query
      return '';
    }

    // Oracle 12c+ uses standard OFFSET/FETCH syntax
    const rawRowLimit = this.resolvedRowLimit();
    const limitClause = rawRowLimit === null ? '' : ` FETCH NEXT ${this.parseRowLimit(rawRowLimit)} ROWS ONLY`;
    const offsetClause = this.offset ? ` OFFSET ${parseInt(this.offset, 10)} ROWS` : '';
    return `${offsetClause}${limitClause}`;
  }

  /**
   * Wraps query with ROWNUM-based pagination for Oracle 11g.
   * Oracle 11g doesn't support OFFSET/FETCH, so we need to use ROWNUM.
   *
   * For LIMIT only:
   *   SELECT * FROM (original_query) WHERE ROWNUM <= :limit
   *
   * For OFFSET + LIMIT:
   *   SELECT * FROM (
   *     SELECT a.*, ROWNUM rnum FROM (original_query) a WHERE ROWNUM <= :maxRow
   *   ) WHERE rnum > :offset
   */
  protected wrapQueryWithRownum(query: string): string {
    const rawRowLimit = this.resolvedRowLimit();

    if (rawRowLimit === null && !this.offset) {
      return query;
    }

    const limit = this.parseRowLimit(rawRowLimit);
    const offset = this.offset ? parseInt(this.offset, 10) : 0;

    if (offset === 0) {
      // Only LIMIT, no OFFSET - simple ROWNUM filter
      return `SELECT * FROM (${query}) WHERE ROWNUM <= ${limit}`;
    } else {
      // OFFSET + LIMIT - need nested query with ROWNUM alias
      const maxRow = offset + limit;
      return `SELECT * FROM (
  SELECT a.*, ROWNUM rnum FROM (
${query}
  ) a WHERE ROWNUM <= ${maxRow}
) WHERE rnum > ${offset}`;
    }
  }

  /**
   * Override buildSqlAndParams to wrap query with ROWNUM for Oracle 11g
   */
  public buildSqlAndParams(exportAnnotatedSql?: boolean): [string, unknown[]] {
    const [sql, params] = super.buildSqlAndParams(exportAnnotatedSql);

    // If Oracle 11g and pagination is needed, wrap the query with ROWNUM
    const rawRowLimit = this.resolvedRowLimit();
    const needsRownumPagination = !this.supportsOffsetFetch()
      && (rawRowLimit !== null || this.offset);

    if (needsRownumPagination) {
      return [this.wrapQueryWithRownum(sql), params];
    }

    return [sql, params];
  }

  /**
   * "AS" for table aliasing on Oracle it's illegal
   */
  public get asSyntaxTable() {
    return '';
  }

  public get asSyntaxJoin() {
    return this.asSyntaxTable;
  }

  /**
   * Oracle doesn't support group by index,
   * using forSelect dimensions for grouping
   */
  public groupByClause() {
    // Only include dimensions that have select columns
    // Time dimensions without granularity return null from selectColumns()
    const dimensions = this.forSelect().filter((item: any) => (
      !!item.dimension && item.selectColumns && item.selectColumns()
    )) as BaseDimension[];
    if (!dimensions.length) {
      return '';
    }

    return ` GROUP BY ${dimensions.map(item => item.dimensionSql()).join(', ')}`;
  }

  public convertTz(field) {
    /**
     * TODO: add offset timezone
     */
    return field;
  }

  public dateTimeCast(value) {
    // Use timezone-aware parsing for ISO 8601 with milliseconds and trailing 'Z', then cast to DATE
    // to preserve index-friendly comparisons against DATE columns.
    return `CAST(TO_TIMESTAMP_TZ(:"${value}", 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"') AS DATE)`;
  }

  public timeStampCast(value) {
    // Return timezone-aware timestamp for TIMESTAMP comparisons
    return `TO_TIMESTAMP_TZ(:"${value}", 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"')`;
  }

  public timeStampParam(timeDimension) {
    return timeDimension.dateFieldType() === 'string' ? ':"?"' : this.timeStampCast('?');
  }

  public timeGroupedColumn(granularity, dimension) {
    if (!granularity) {
      return dimension;
    }
    return `TRUNC(${dimension}, '${GRANULARITY_VALUE[granularity]}')`;
  }

  /**
   * Oracle uses ADD_MONTHS for year/month/quarter intervals
   * and NUMTODSINTERVAL for day/hour/minute/second intervals
   */
  public addInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    // Handle year/month/quarter using ADD_MONTHS
    let totalMonths = 0;
    if (intervalParsed.year) {
      totalMonths += intervalParsed.year * 12;
    }
    if (intervalParsed.quarter) {
      totalMonths += intervalParsed.quarter * 3;
    }
    if (intervalParsed.month) {
      totalMonths += intervalParsed.month;
    }

    if (totalMonths !== 0) {
      res = `ADD_MONTHS(${res}, ${totalMonths})`;
    }

    // Handle day/hour/minute/second using NUMTODSINTERVAL
    if (intervalParsed.day) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.day}, 'DAY')`;
    }
    if (intervalParsed.hour) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.hour}, 'HOUR')`;
    }
    if (intervalParsed.minute) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.minute}, 'MINUTE')`;
    }
    if (intervalParsed.second) {
      res = `${res} + NUMTODSINTERVAL(${intervalParsed.second}, 'SECOND')`;
    }

    return res;
  }

  /**
   * Oracle subtraction uses ADD_MONTHS with negative values
   * and subtracts NUMTODSINTERVAL for time units
   */
  public subtractInterval(date: string, interval: string): string {
    const intervalParsed = parseSqlInterval(interval);
    let res = date;

    // Handle year/month/quarter using ADD_MONTHS with negative values
    let totalMonths = 0;
    if (intervalParsed.year) {
      totalMonths += intervalParsed.year * 12;
    }
    if (intervalParsed.quarter) {
      totalMonths += intervalParsed.quarter * 3;
    }
    if (intervalParsed.month) {
      totalMonths += intervalParsed.month;
    }

    if (totalMonths !== 0) {
      res = `ADD_MONTHS(${res}, -${totalMonths})`;
    }

    // Handle day/hour/minute/second using NUMTODSINTERVAL with subtraction
    if (intervalParsed.day) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.day}, 'DAY')`;
    }
    if (intervalParsed.hour) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.hour}, 'HOUR')`;
    }
    if (intervalParsed.minute) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.minute}, 'MINUTE')`;
    }
    if (intervalParsed.second) {
      res = `${res} - NUMTODSINTERVAL(${intervalParsed.second}, 'SECOND')`;
    }

    return res;
  }

  public newFilter(filter) {
    return new OracleFilter(this, filter);
  }

  public unixTimestampSql() {
    // eslint-disable-next-line quotes
    return `((cast (systimestamp at time zone 'UTC' as date) - date '1970-01-01') * 86400)`;
  }

  public preAggregationTableName(cube, preAggregationName, skipSchema) {
    const name = super.preAggregationTableName(cube, preAggregationName, skipSchema);
    if (name.length > 128) {
      throw new UserError(`Oracle can not work with table names that longer than 64 symbols. Consider using the 'sqlAlias' attribute in your cube and in your pre-aggregation definition for ${name}.`);
    }
    return name;
  }
}
