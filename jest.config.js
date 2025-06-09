/** @type {import('ts-jest').JestConfigWithTsJest} **/
export default {
  testEnvironment: 'node',
  verbose: true,
  transform: {
    '^.+.tsx?$': ['ts-jest', {}],
  },
};
