/**
 * Predefined aggregation functions for use with ArrowStore's groupBy method
 */
export const Aggregations = {
  /**
   * Count the number of records in a group
   */
  count: () => {
    function count(values: any[]) {
      return values.length;
    }

    return count;
  },

  /**
   * Sum a numeric column within a group
   * @param field The name of the field to sum
   */
  sum: (field: string) => {
    function sum(values: any[]) {
      return values
        .map((record) => record[field])
        .filter((val) => val !== null && val !== undefined)
        .reduce((sum, val) => sum + val, 0);
    }

    sum.sourceField = field;
    return sum;
  },

  /**
   * Calculate the average of a numeric column within a group
   * @param field The name of the field to average
   */
  avg: (field: string) => {
    function average(values: any[]) {
      const numbers = values
        .map((record) => record[field])
        .filter((val) => val !== null && val !== undefined);

      return numbers.length
        ? numbers.reduce((sum, val) => sum + val, 0) / numbers.length
        : null;
    }

    average.sourceField = field;
    return average;
  },

  /**
   * Find the minimum value of a column within a group
   * @param field The name of the field to find the minimum value for
   */
  min: (field: string) => {
    function min(values: any[]) {
      const filteredValues = values
        .map((record) => record[field])
        .filter((val) => val !== null && val !== undefined);

      return filteredValues.length ? Math.min(...filteredValues) : null;
    }

    min.sourceField = field;
    return min;
  },

  /**
   * Find the maximum value of a column within a group
   * @param field The name of the field to find the maximum value for
   */
  max: (field: string) => {
    function max(values: any[]) {
      const filteredValues = values
        .map((record) => record[field])
        .filter((val) => val !== null && val !== undefined);

      return filteredValues.length ? Math.max(...filteredValues) : null;
    }

    max.sourceField = field;
    return max;
  },

  /**
   * Count distinct values of a column within a group
   * @param field The name of the field to count distinct values for
   */
  countDistinct: (field: string) => {
    function countDistinct(values: any[]) {
      const distinctValues = new Set(
        values
          .map((record) => record[field])
          .filter((val) => val !== null && val !== undefined),
      );

      return distinctValues.size;
    }

    countDistinct.sourceField = field;
    return countDistinct;
  },
};
