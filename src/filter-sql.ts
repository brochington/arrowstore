/**
 * SQL-like filter string parser for ArrowStore
 *
 * Supports basic SQL WHERE clause syntax:
 * - Comparison operators: =, !=, >, >=, <, <=
 * - Logical operators: AND, OR, NOT
 * - String operations: LIKE, IN
 * - Parentheses for grouping
 * - String literals in single quotes
 * - Numeric literals
 * - Boolean literals (true, false)
 */

import type {
  AndFilter,
  FieldFilter,
  FilterCondition,
  FilterOperator,
  NotFilter,
  OrFilter,
} from './filter-helpers';

// Define token types for lexer
enum TokenType {
  IDENTIFIER = 'IDENTIFIER',
  OPERATOR = 'OPERATOR',
  STRING = 'STRING',
  NUMBER = 'NUMBER',
  BOOLEAN = 'BOOLEAN',
  LPAREN = 'LPAREN',
  RPAREN = 'RPAREN',
  AND = 'AND',
  OR = 'OR',
  NOT = 'NOT',
  IN = 'IN',
  LIKE = 'LIKE',
  COMMA = 'COMMA',
  EOF = 'EOF',
}

// Define token structure
interface Token {
  type: TokenType;
  value: string;
  position: number;
}

/**
 * Lexer for SQL-like filter strings
 */
class SqlFilterLexer {
  private input: string;
  private position = 0;
  private tokens: Token[] = [];

  constructor(input: string) {
    this.input = input.trim();
  }

  tokenize(): Token[] {
    while (this.position < this.input.length) {
      const char = this.input[this.position];

      // Skip whitespace
      if (/\s/.test(char)) {
        this.position++;
        continue;
      }

      // Handle identifier
      if (/[a-zA-Z_]/.test(char)) {
        this.tokenizeIdentifier();
        continue;
      }

      // Handle numeric literal
      if (/\d/.test(char)) {
        this.tokenizeNumber();
        continue;
      }

      // Handle string literal
      if (char === "'") {
        this.tokenizeString();
        continue;
      }

      // Handle various operators and symbols
      switch (char) {
        case '(':
          this.tokens.push({
            type: TokenType.LPAREN,
            value: '(',
            position: this.position,
          });
          this.position++;
          break;

        case ')':
          this.tokens.push({
            type: TokenType.RPAREN,
            value: ')',
            position: this.position,
          });
          this.position++;
          break;

        case ',':
          this.tokens.push({
            type: TokenType.COMMA,
            value: ',',
            position: this.position,
          });
          this.position++;
          break;

        case '=':
          this.tokens.push({
            type: TokenType.OPERATOR,
            value: '=',
            position: this.position,
          });
          this.position++;
          break;

        case '>':
          if (this.peekNext() === '=') {
            this.tokens.push({
              type: TokenType.OPERATOR,
              value: '>=',
              position: this.position,
            });
            this.position += 2;
          } else {
            this.tokens.push({
              type: TokenType.OPERATOR,
              value: '>',
              position: this.position,
            });
            this.position++;
          }
          break;

        case '<':
          if (this.peekNext() === '=') {
            this.tokens.push({
              type: TokenType.OPERATOR,
              value: '<=',
              position: this.position,
            });
            this.position += 2;
          } else if (this.peekNext() === '>') {
            this.tokens.push({
              type: TokenType.OPERATOR,
              value: '<>',
              position: this.position,
            });
            this.position += 2;
          } else {
            this.tokens.push({
              type: TokenType.OPERATOR,
              value: '<',
              position: this.position,
            });
            this.position++;
          }
          break;

        case '!':
          if (this.peekNext() === '=') {
            this.tokens.push({
              type: TokenType.OPERATOR,
              value: '!=',
              position: this.position,
            });
            this.position += 2;
          } else {
            throw new Error(
              `Unexpected character '!' at position ${this.position}`,
            );
          }
          break;

        default:
          throw new Error(
            `Unexpected character '${char}' at position ${this.position}`,
          );
      }
    }

    // Add EOF token
    this.tokens.push({
      type: TokenType.EOF,
      value: 'EOF',
      position: this.position,
    });
    return this.tokens;
  }

  private tokenizeIdentifier(): void {
    const start = this.position;
    while (
      this.position < this.input.length &&
      /[a-zA-Z0-9_.]/.test(this.input[this.position])
    ) {
      this.position++;
    }

    const value = this.input.substring(start, this.position).toUpperCase();

    // Check for keywords
    if (value === 'AND') {
      this.tokens.push({ type: TokenType.AND, value, position: start });
    } else if (value === 'OR') {
      this.tokens.push({ type: TokenType.OR, value, position: start });
    } else if (value === 'NOT') {
      this.tokens.push({ type: TokenType.NOT, value, position: start });
    } else if (value === 'IN') {
      this.tokens.push({ type: TokenType.IN, value, position: start });
    } else if (value === 'LIKE') {
      this.tokens.push({ type: TokenType.LIKE, value, position: start });
    } else if (value === 'TRUE' || value === 'FALSE') {
      this.tokens.push({
        type: TokenType.BOOLEAN,
        value: value.toLowerCase(),
        position: start,
      });
    } else {
      this.tokens.push({
        type: TokenType.IDENTIFIER,
        value: this.input.substring(start, this.position),
        position: start,
      });
    }
  }

  private tokenizeNumber(): void {
    const start = this.position;
    let hasDot = false;

    while (this.position < this.input.length) {
      const char = this.input[this.position];

      if (char === '.' && !hasDot) {
        hasDot = true;
        this.position++;
      } else if (/\d/.test(char)) {
        this.position++;
      } else {
        break;
      }
    }

    const value = this.input.substring(start, this.position);
    this.tokens.push({ type: TokenType.NUMBER, value, position: start });
  }

  private tokenizeString(): void {
    const start = this.position;
    this.position++; // Skip opening quote

    while (
      this.position < this.input.length &&
      this.input[this.position] !== "'"
    ) {
      // Handle escaped quotes
      if (this.input[this.position] === '\\' && this.peekNext() === "'") {
        this.position += 2;
      } else {
        this.position++;
      }
    }

    if (this.position >= this.input.length) {
      throw new Error(
        `Unterminated string literal starting at position ${start}`,
      );
    }

    // Skip closing quote
    this.position++;

    // Extract the string without the quotes
    const value = this.input
      .substring(start + 1, this.position - 1)
      .replace(/\\'/g, "'"); // Handle escaped quotes

    this.tokens.push({ type: TokenType.STRING, value, position: start });
  }

  private peekNext(): string {
    return this.position + 1 < this.input.length
      ? this.input[this.position + 1]
      : '';
  }
}

/**
 * Parser for SQL-like filter strings
 */
class SqlFilterParser<T extends Record<string, any>> {
  private tokens: Token[] = [];
  private current = 0;

  constructor(tokens: Token[]) {
    this.tokens = tokens;
  }

  parse(): FilterCondition<T>[] {
    const filters: FilterCondition<T>[] = [];

    // Parse until we reach the end
    while (!this.isAtEnd()) {
      filters.push(this.parseExpression());

      // If we see AND, parse another expression
      if (this.match(TokenType.AND)) {
        continue;
      }

      // If we see a token that doesn't belong here, it's an error
      if (!this.isAtEnd() && !this.check(TokenType.EOF)) {
        throw new Error(
          `Unexpected token '${this.peek().value}' at position ${
            this.peek().position
          }`,
        );
      }

      break;
    }

    return filters;
  }

  private parseExpression(): FilterCondition<T> {
    return this.parseOr();
  }

  private parseOr(): FilterCondition<T> {
    let expr = this.parseAnd();

    while (this.match(TokenType.OR)) {
      const right = this.parseAnd();
      expr = this.createOrFilter(expr, right);
    }

    return expr;
  }

  private parseAnd(): FilterCondition<T> {
    let expr = this.parseNot();

    while (this.match(TokenType.AND)) {
      const right = this.parseNot();
      expr = this.createAndFilter(expr, right);
    }

    return expr;
  }

  private parseNot(): FilterCondition<T> {
    if (this.match(TokenType.NOT)) {
      const expr = this.parsePrimary();
      return this.createNotFilter(expr);
    }

    return this.parsePrimary();
  }

  private parsePrimary(): FilterCondition<T> {
    // Handle parentheses for grouping
    if (this.match(TokenType.LPAREN)) {
      const expr = this.parseExpression();
      this.consume(TokenType.RPAREN, "Expect ')' after expression");
      return expr;
    }

    // Handle field comparisons
    if (this.match(TokenType.IDENTIFIER)) {
      const fieldName = this.previous().value;

      // Handle IN operator
      if (this.match(TokenType.IN)) {
        return this.parseInExpression(fieldName);
      }

      // Handle LIKE operator
      if (this.match(TokenType.LIKE)) {
        return this.parseLikeExpression(fieldName);
      }

      // Handle comparison operators
      if (this.match(TokenType.OPERATOR)) {
        return this.parseComparisonExpression(fieldName);
      }

      throw new Error(
        `Expected operator after field name '${fieldName}' at position ${
          this.peek().position
        }`,
      );
    }

    throw new Error(
      `Unexpected token '${this.peek().value}' at position ${
        this.peek().position
      }`,
    );
  }

  private parseComparisonExpression(fieldName: string): FieldFilter<T> {
    const operator = this.previous().value;
    let filterOp: FilterOperator;

    // Map SQL operators to filter operators
    switch (operator) {
      case '=':
        filterOp = 'eq';
        break;
      case '<>':
      case '!=':
        filterOp = 'neq';
        break;
      case '>':
        filterOp = 'gt';
        break;
      case '>=':
        filterOp = 'gte';
        break;
      case '<':
        filterOp = 'lt';
        break;
      case '<=':
        filterOp = 'lte';
        break;
      default:
        throw new Error(`Unsupported operator '${operator}'`);
    }

    // Parse the right-hand value
    const value = this.parseValue();

    return {
      field: fieldName as keyof T,
      filter: {
        op: filterOp,
        value,
      },
    } as FieldFilter<T>;
  }

  private parseInExpression(fieldName: string): FieldFilter<T> {
    this.consume(TokenType.LPAREN, "Expect '(' after IN");

    const values: any[] = [];

    // Parse comma-separated values
    do {
      values.push(this.parseValue());
    } while (this.match(TokenType.COMMA));

    this.consume(TokenType.RPAREN, "Expect ')' after IN values");

    return {
      field: fieldName as keyof T,
      filter: {
        op: 'in',
        value: values,
      },
    } as FieldFilter<T>;
  }

  private parseLikeExpression(fieldName: string): FieldFilter<T> {
    // Consume the pattern
    this.consume(TokenType.STRING, 'Expect string pattern after LIKE');
    const pattern = this.previous().value;

    // Determine the appropriate string operation based on the pattern
    let op: 'contains' | 'startsWith' | 'endsWith';
    let value: string;

    if (pattern.startsWith('%') && pattern.endsWith('%')) {
      op = 'contains';
      value = pattern.slice(1, -1);
    } else if (pattern.startsWith('%')) {
      op = 'endsWith';
      value = pattern.slice(1);
    } else if (pattern.endsWith('%')) {
      op = 'startsWith';
      value = pattern.slice(0, -1);
    } else {
      // For exact match with LIKE, use contains (could be eq for exact matches)
      op = 'contains';
      value = pattern;
    }

    return {
      field: fieldName as keyof T,
      filter: {
        op,
        value,
      },
    } as FieldFilter<T>;
  }

  private parseValue(): any {
    if (this.match(TokenType.STRING)) {
      return this.previous().value;
    }

    if (this.match(TokenType.NUMBER)) {
      return Number.parseFloat(this.previous().value);
    }

    if (this.match(TokenType.BOOLEAN)) {
      return this.previous().value === 'true';
    }

    throw new Error(`Expected value at position ${this.peek().position}`);
  }

  private createAndFilter(
    left: FilterCondition<T>,
    right: FilterCondition<T>,
  ): AndFilter<T> {
    // If left is already an AND filter, add right to its conditions
    if ('AND' in left) {
      return {
        AND: [...left.AND, right],
      } as AndFilter<T>;
    }

    // Otherwise, create a new AND filter with both conditions
    return {
      AND: [left, right],
    } as AndFilter<T>;
  }

  private createOrFilter(
    left: FilterCondition<T>,
    right: FilterCondition<T>,
  ): OrFilter<T> {
    // If left is already an OR filter, add right to its conditions
    if ('OR' in left) {
      return {
        OR: [...left.OR, right],
      } as OrFilter<T>;
    }

    // Otherwise, create a new OR filter with both conditions
    return {
      OR: [left, right],
    } as OrFilter<T>;
  }

  private createNotFilter(expr: FilterCondition<T>): NotFilter<T> {
    return {
      NOT: expr,
    } as NotFilter<T>;
  }

  private match(...types: TokenType[]): boolean {
    for (const type of types) {
      if (this.check(type)) {
        this.advance();
        return true;
      }
    }
    return false;
  }

  private consume(type: TokenType, message: string): Token {
    if (this.check(type)) {
      return this.advance();
    }

    throw new Error(`${message} at position ${this.peek().position}`);
  }

  private check(type: TokenType): boolean {
    if (this.isAtEnd()) {
      return false;
    }
    return this.peek().type === type;
  }

  private advance(): Token {
    if (!this.isAtEnd()) {
      this.current++;
    }
    return this.previous();
  }

  private isAtEnd(): boolean {
    return this.peek().type === TokenType.EOF;
  }

  private peek(): Token {
    return this.tokens[this.current];
  }

  private previous(): Token {
    return this.tokens[this.current - 1];
  }
}

/**
 * Parse a SQL-like filter string into filter conditions
 * @param sql SQL-like WHERE clause filter string
 * @returns Array of filter conditions
 */
export function parseSqlFilter<T extends Record<string, any>>(
  sql: string,
): FilterCondition<T>[] {
  try {
    // Tokenize the input
    const lexer = new SqlFilterLexer(sql);
    const tokens = lexer.tokenize();

    // Parse the tokens
    const parser = new SqlFilterParser<T>(tokens);
    return parser.parse();
  } catch (error) {
    throw new Error(`Error parsing SQL filter: ${(error as Error).message}`);
  }
}
