/** @type {import('jest').Config} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/__tests__/**/*.test.ts'],
  moduleFileExtensions: ['ts', 'js', 'json'],
  rootDir: '.',
  verbose: true,
  testTimeout: 30000,
  // Force exit after tests complete to handle native module cleanup
  forceExit: true,
  // Transform TypeScript
  transform: {
    '^.+\\.tsx?$': ['ts-jest', {
      useESM: true,
      tsconfig: {
        module: 'CommonJS',
        moduleResolution: 'Node',
        esModuleInterop: true,
        target: 'ES2020',
      },
    }],
  },
};
