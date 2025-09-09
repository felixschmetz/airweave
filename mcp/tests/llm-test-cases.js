// LLM Test Cases for Enhanced MCP Server (JavaScript version)
// This file contains test cases to evaluate how LLMs interact with the enhanced MCP server

export const llmTestCases = [
    // Basic Functionality Tests
    {
        id: 'basic-001',
        description: 'Simple query with no additional parameters',
        input: 'Search for customer feedback about pricing',
        expectedParams: {
            query: 'customer feedback about pricing'
        },
        expectedBehavior: 'Should use only the query parameter, all others should use defaults',
        category: 'basic',
        difficulty: 'easy'
    },
    {
        id: 'basic-002',
        description: 'Query with response type specification',
        input: 'Find documents about billing and give me a summary',
        expectedParams: {
            query: 'billing',
            response_type: 'completion'
        },
        expectedBehavior: 'Should recognize the need for AI-generated summary and set response_type to completion',
        category: 'basic',
        difficulty: 'easy'
    },
    {
        id: 'basic-003',
        description: 'Query with limit specification',
        input: 'Show me the first 5 results about API documentation',
        expectedParams: {
            query: 'API documentation',
            limit: 5
        },
        expectedBehavior: 'Should extract the limit from the natural language and set it to 5',
        category: 'basic',
        difficulty: 'easy'
    },
    {
        id: 'basic-004',
        description: 'Query with pagination',
        input: 'Show me results 11-20 about support tickets',
        expectedParams: {
            query: 'support tickets',
            limit: 10,
            offset: 10
        },
        expectedBehavior: 'Should calculate limit=10 and offset=10 from "results 11-20"',
        category: 'basic',
        difficulty: 'medium'
    },

    // Advanced Parameter Tests
    {
        id: 'advanced-001',
        description: 'Query with recency bias',
        input: 'Find recent customer complaints about the new feature',
        expectedParams: {
            query: 'customer complaints about the new feature',
            recency_bias: 0.7
        },
        expectedBehavior: 'Should recognize "recent" and apply recency bias to prioritize newer results',
        category: 'advanced',
        difficulty: 'medium'
    },
    {
        id: 'advanced-002',
        description: 'Complex query with multiple parameters',
        input: 'Find the most recent 10 high-priority support tickets and give me a summary',
        expectedParams: {
            query: 'high-priority support tickets',
            response_type: 'completion',
            limit: 10,
            recency_bias: 0.8
        },
        expectedBehavior: 'Should extract all parameters: query, response_type for summary, limit for 10, recency_bias for recent',
        category: 'advanced',
        difficulty: 'hard'
    },

    // Error Handling Tests
    {
        id: 'error-001',
        description: 'Query with invalid limit',
        input: 'Search for feedback with limit 2000',
        expectedParams: {
            query: 'feedback',
            limit: 2000 // Invalid: exceeds max of 1000
        },
        expectedBehavior: 'Should either reject the invalid limit or automatically cap it at 1000',
        category: 'error',
        difficulty: 'medium'
    },
    {
        id: 'error-002',
        description: 'Query with negative offset',
        input: 'Find documents starting from -5',
        expectedParams: {
            query: 'documents',
            offset: -5 // Invalid: negative offset
        },
        expectedBehavior: 'Should either reject negative offset or set it to 0',
        category: 'error',
        difficulty: 'medium'
    },

    // Edge Case Tests
    {
        id: 'edge-001',
        description: 'Query at boundary limits',
        input: 'Search for test with limit 1000 and offset 0',
        expectedParams: {
            query: 'test',
            limit: 1000, // Valid: at max boundary
            offset: 0    // Valid: at min boundary
        },
        expectedBehavior: 'Should handle boundary values correctly',
        category: 'edge',
        difficulty: 'easy'
    },
    {
        id: 'edge-002',
        description: 'Query with maximum recency bias',
        input: 'Find the most recent documents with recency bias 1.0',
        expectedParams: {
            query: 'documents',
            recency_bias: 1.0 // Valid: at max boundary
        },
        expectedBehavior: 'Should handle maximum recency bias correctly',
        category: 'edge',
        difficulty: 'easy'
    }
];

// Test execution helper
export class LLMTestRunner {
    constructor() {
        this.results = [];
    }

    async runTest(testCase, llmFunction) {
        const startTime = Date.now();

        try {
            const actualParams = await llmFunction(testCase.input);
            const responseTime = Date.now() - startTime;

            // Validate parameters
            const errors = this.validateParams(testCase.expectedParams, actualParams);

            this.results.push({
                testCase,
                actualParams,
                success: errors.length === 0,
                errors,
                responseTime
            });
        } catch (error) {
            this.results.push({
                testCase,
                actualParams: null,
                success: false,
                errors: [String(error)],
                responseTime: Date.now() - startTime
            });
        }
    }

    validateParams(expected, actual) {
        const errors = [];

        // Check required parameters
        for (const [key, value] of Object.entries(expected)) {
            if (actual[key] !== value) {
                errors.push(`Expected ${key}=${value}, got ${actual[key]}`);
            }
        }

        // Check for unexpected parameters
        for (const key of Object.keys(actual)) {
            if (!(key in expected)) {
                errors.push(`Unexpected parameter: ${key}`);
            }
        }

        return errors;
    }

    getResults() {
        return this.results;
    }

    getSummary() {
        const total = this.results.length;
        const successful = this.results.filter(r => r.success).length;
        const avgResponseTime = this.results.reduce((sum, r) => sum + r.responseTime, 0) / total;

        return {
            total,
            successful,
            successRate: (successful / total) * 100,
            avgResponseTime,
            errors: this.results.filter(r => !r.success).map(r => ({
                testCase: r.testCase.id,
                errors: r.errors
            }))
        };
    }
}

// Test categories for organized testing
export const testCategories = {
    basic: llmTestCases.filter(tc => tc.category === 'basic'),
    advanced: llmTestCases.filter(tc => tc.category === 'advanced'),
    error: llmTestCases.filter(tc => tc.category === 'error'),
    edge: llmTestCases.filter(tc => tc.category === 'edge')
};

// Difficulty levels for progressive testing
export const difficultyLevels = {
    easy: llmTestCases.filter(tc => tc.difficulty === 'easy'),
    medium: llmTestCases.filter(tc => tc.difficulty === 'medium'),
    hard: llmTestCases.filter(tc => tc.difficulty === 'hard')
};
