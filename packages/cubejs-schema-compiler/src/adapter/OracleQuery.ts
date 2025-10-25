import { MAX_SOURCE_ROW_LIMIT } from '@cubejs-backend/shared';

import { BaseQuery } from './BaseQuery';
import { BaseFilter } from './BaseFilter';
import { UserError } from '../compiler/UserError';
import { BaseDimension } from './BaseDimension';

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
  /**
   * "LIMIT" on Oracle is illegal
   * TODO replace with limitOffsetClause override
   */
  public groupByDimensionLimit() {
    return '';
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
    const dimensions = this.forSelect().filter((item: any) => !!item.dimension) as BaseDimension[];
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

  protected rowLimitSql(): string | null {
    if (this.rowLimit == null) {
      return null;
    }
    if (this.rowLimit === MAX_SOURCE_ROW_LIMIT) {
      return this.paramAllocator.allocateParam(MAX_SOURCE_ROW_LIMIT);
    }
    const numeric = Number.parseInt(String(this.rowLimit), 10);
    if (Number.isNaN(numeric)) {
      return null;
    }
    return `${numeric}`;
  }

  protected offsetValue(): number | null {
    if (this.offset == null) {
      return null;
    }
    const parsed = Number.parseInt(String(this.offset), 10);
    return Number.isNaN(parsed) ? null : parsed;
  }

  protected applyRowNumberPagination(sql: string): string {
    const limitExpression = this.rowLimitSql();
    const offsetValue = this.offsetValue();

    if (!limitExpression && (offsetValue == null || offsetValue <= 0)) {
      return sql;
    }

    const wrappedLimit = limitExpression ? `(${limitExpression})` : null;
    const offsetNumber = offsetValue && offsetValue > 0 ? offsetValue : 0;

    if (offsetNumber === 0 && wrappedLimit) {
      return `SELECT * FROM (${sql}) pagination_q WHERE ROWNUM <= ${wrappedLimit}`;
    }

    const rowNumAlias = this.escapeColumnName('row_num__');
    const upperBoundExpr = wrappedLimit
      ? (offsetNumber > 0 ? `${offsetNumber} + ${wrappedLimit}` : wrappedLimit)
      : null;
    const innerWhere = upperBoundExpr ? ` WHERE ROWNUM <= ${upperBoundExpr}` : '';

    const conditions: string[] = [];
    if (offsetNumber > 0) {
      conditions.push(`${rowNumAlias} > ${offsetNumber}`);
    }
    if (upperBoundExpr) {
      conditions.push(`${rowNumAlias} <= ${upperBoundExpr}`);
    }

    const outerWhere = conditions.length ? ` WHERE ${conditions.join(' AND ')}` : '';

    return `SELECT * FROM (SELECT inner_q.*, ROWNUM ${rowNumAlias} FROM (${sql}) inner_q${innerWhere}) outer_q${outerWhere}`;
  }

  public buildParamAnnotatedSql(): string {
    const sql = super.buildParamAnnotatedSql();
    return this.applyRowNumberPagination(sql);
  }
}
