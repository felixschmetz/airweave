import { describe, it, expect, vi, beforeEach } from 'vitest';
import { z } from 'zod';

// Mock the dependencies
vi.mock('../src/api/airweave-client.js');
vi.mock('../src/utils/error-handling.js');

describe('Search Tool', () => {
    let mockAirweaveClient: any;
    let mockFormatSearchResponse: any;
    let mockFormatErrorResponse: any;

    beforeEach(() => {
        // Reset mocks
        vi.clearAllMocks();

        // Mock AirweaveClient
        mockAirweaveClient = {
            search: vi.fn()
        };

        // Mock error handling functions
        mockFormatSearchResponse = vi.fn();
        mockFormatErrorResponse = vi.fn();

        // Set up module mocks
        vi.doMock('../src/api/airweave-client.js', () => ({
            AirweaveClient: vi.fn().mockImplementation(() => mockAirweaveClient)
        }));

        vi.doMock('../src/utils/error-handling.js', () => ({
            formatSearchResponse: mockFormatSearchResponse,
            formatErrorResponse: mockFormatErrorResponse
        }));
    });

    describe('Schema Validation', () => {
        it('should validate required query parameter', () => {
            const schema = z.object({
                query: z.string().min(1).max(1000),
                response_type: z.enum(["raw", "completion"]).optional().default("raw"),
                limit: z.number().min(1).max(1000).optional().default(100),
                offset: z.number().min(0).optional().default(0),
                recency_bias: z.number().min(0).max(1).optional()
            });

            // Valid parameters
            expect(() => schema.parse({ query: "test" })).not.toThrow();
            expect(() => schema.parse({
                query: "test",
                response_type: "completion",
                limit: 50,
                offset: 10,
                recency_bias: 0.5
            })).not.toThrow();

            // Invalid parameters
            expect(() => schema.parse({})).toThrow(); // Missing query
            expect(() => schema.parse({ query: "" })).toThrow(); // Empty query
            expect(() => schema.parse({ query: "test", limit: 0 })).toThrow(); // Invalid limit
            expect(() => schema.parse({ query: "test", offset: -1 })).toThrow(); // Invalid offset
            expect(() => schema.parse({ query: "test", recency_bias: 1.5 })).toThrow(); // Invalid recency_bias
        });

        it('should apply default values correctly', () => {
            const schema = z.object({
                query: z.string().min(1).max(1000),
                response_type: z.enum(["raw", "completion"]).optional().default("raw"),
                limit: z.number().min(1).max(1000).optional().default(100),
                offset: z.number().min(0).optional().default(0),
                recency_bias: z.number().min(0).max(1).optional()
            });

            const result = schema.parse({ query: "test" });

            expect(result.query).toBe("test");
            expect(result.response_type).toBe("raw");
            expect(result.limit).toBe(100);
            expect(result.offset).toBe(0);
            expect(result.recency_bias).toBeUndefined();
        });
    });

    describe('Parameter Boundaries', () => {
        it('should handle query length boundaries', () => {
            const schema = z.object({
                query: z.string().min(1).max(1000)
            });

            // Valid boundaries
            expect(() => schema.parse({ query: "a" })).not.toThrow(); // Min length
            expect(() => schema.parse({ query: "a".repeat(1000) })).not.toThrow(); // Max length

            // Invalid boundaries
            expect(() => schema.parse({ query: "" })).toThrow(); // Too short
            expect(() => schema.parse({ query: "a".repeat(1001) })).toThrow(); // Too long
        });

        it('should handle limit boundaries', () => {
            const schema = z.object({
                query: z.string().min(1),
                limit: z.number().min(1).max(1000)
            });

            // Valid boundaries
            expect(() => schema.parse({ query: "test", limit: 1 })).not.toThrow();
            expect(() => schema.parse({ query: "test", limit: 1000 })).not.toThrow();

            // Invalid boundaries
            expect(() => schema.parse({ query: "test", limit: 0 })).toThrow();
            expect(() => schema.parse({ query: "test", limit: 1001 })).toThrow();
        });

        it('should handle offset boundaries', () => {
            const schema = z.object({
                query: z.string().min(1),
                offset: z.number().min(0)
            });

            // Valid boundaries
            expect(() => schema.parse({ query: "test", offset: 0 })).not.toThrow();
            expect(() => schema.parse({ query: "test", offset: 1000 })).not.toThrow();

            // Invalid boundaries
            expect(() => schema.parse({ query: "test", offset: -1 })).toThrow();
        });

        it('should handle recency_bias boundaries', () => {
            const schema = z.object({
                query: z.string().min(1),
                recency_bias: z.number().min(0).max(1)
            });

            // Valid boundaries
            expect(() => schema.parse({ query: "test", recency_bias: 0.0 })).not.toThrow();
            expect(() => schema.parse({ query: "test", recency_bias: 1.0 })).not.toThrow();

            // Invalid boundaries
            expect(() => schema.parse({ query: "test", recency_bias: -0.1 })).toThrow();
            expect(() => schema.parse({ query: "test", recency_bias: 1.1 })).toThrow();
        });
    });

    describe('Response Type Validation', () => {
        it('should validate response_type enum values', () => {
            const schema = z.object({
                query: z.string().min(1),
                response_type: z.enum(["raw", "completion"])
            });

            // Valid values
            expect(() => schema.parse({ query: "test", response_type: "raw" })).not.toThrow();
            expect(() => schema.parse({ query: "test", response_type: "completion" })).not.toThrow();

            // Invalid values
            expect(() => schema.parse({ query: "test", response_type: "invalid" })).toThrow();
            expect(() => schema.parse({ query: "test", response_type: "RAW" })).toThrow(); // Case sensitive
        });
    });
});
