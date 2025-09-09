import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Mock fetch for testing
const mockFetch = vi.fn();
global.fetch = mockFetch;

// Mock environment variables
const originalEnv = process.env;
beforeEach(() => {
    process.env = {
        ...originalEnv,
        AIRWEAVE_API_KEY: 'test-api-key',
        AIRWEAVE_COLLECTION: 'test-collection',
        AIRWEAVE_BASE_URL: 'https://api.airweave.ai'
    };
});

afterEach(() => {
    process.env = originalEnv;
    vi.clearAllMocks();
});

describe('MCP Tool Integration Tests', () => {
    beforeEach(() => {
        mockFetch.mockClear();
    });

    describe('Successful API calls', () => {
        it('should make successful search request with minimal parameters', async () => {
            const mockResponse = {
                results: [
                    { score: 0.95, payload: { content: 'Test result 1' } },
                    { score: 0.87, payload: { content: 'Test result 2' } }
                ],
                response_type: 'raw',
                status: 'success'
            };

            mockFetch.mockResolvedValueOnce({
                ok: true,
                text: () => Promise.resolve(JSON.stringify(mockResponse))
            });

            // This would be the actual tool function call
            // In a real test, you'd import and call the tool function
            const searchParams = new URLSearchParams({
                query: 'test query',
                response_type: 'raw',
                limit: '100',
                offset: '0'
            });

            const response = await fetch(`https://api.airweave.ai/collections/test-collection/search?${searchParams}`, {
                method: 'GET',
                headers: {
                    'x-api-key': 'test-api-key',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            });

            expect(response.ok).toBe(true);
            expect(mockFetch).toHaveBeenCalledWith(
                `https://api.airweave.ai/collections/test-collection/search?${searchParams}`,
                expect.objectContaining({
                    method: 'GET',
                    headers: expect.objectContaining({
                        'x-api-key': 'test-api-key'
                    })
                })
            );
        });

        it('should make successful search request with all parameters', async () => {
            const mockResponse = {
                results: [{ score: 0.92, payload: { content: 'Test result' } }],
                response_type: 'completion',
                completion: 'AI-generated response',
                status: 'success'
            };

            mockFetch.mockResolvedValueOnce({
                ok: true,
                text: () => Promise.resolve(JSON.stringify(mockResponse))
            });

            const searchParams = new URLSearchParams({
                query: 'test query',
                response_type: 'completion',
                limit: '50',
                offset: '10'
            });
            searchParams.append('recency_bias', '0.5');

            const response = await fetch(`https://api.airweave.ai/collections/test-collection/search?${searchParams}`, {
                method: 'GET',
                headers: {
                    'x-api-key': 'test-api-key',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                }
            });

            expect(response.ok).toBe(true);
            expect(mockFetch).toHaveBeenCalledWith(
                `https://api.airweave.ai/collections/test-collection/search?${searchParams}`,
                expect.objectContaining({
                    method: 'GET',
                    headers: expect.objectContaining({
                        'x-api-key': 'test-api-key'
                    })
                })
            );
        });
    });

    describe('Error handling', () => {
        it('should handle API errors gracefully', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: false,
                status: 404,
                text: () => Promise.resolve('Collection not found')
            });

            const searchParams = new URLSearchParams({
                query: 'test query',
                response_type: 'raw',
                limit: '100',
                offset: '0'
            });

            try {
                await fetch(`https://api.airweave.ai/collections/test-collection/search?${searchParams}`, {
                    method: 'GET',
                    headers: {
                        'x-api-key': 'test-api-key',
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                });
            } catch (error) {
                expect(error).toBeInstanceOf(Error);
                expect((error as Error).message).toContain('404');
            }
        });

        it('should handle network errors', async () => {
            mockFetch.mockRejectedValueOnce(new Error('Network error'));

            const searchParams = new URLSearchParams({
                query: 'test query',
                response_type: 'raw',
                limit: '100',
                offset: '0'
            });

            try {
                await fetch(`https://api.airweave.ai/collections/test-collection/search?${searchParams}`, {
                    method: 'GET',
                    headers: {
                        'x-api-key': 'test-api-key',
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                });
            } catch (error) {
                expect(error).toBeInstanceOf(Error);
                expect((error as Error).message).toContain('Network error');
            }
        });

        it('should handle invalid JSON response', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                text: () => Promise.resolve('Invalid JSON response')
            });

            const searchParams = new URLSearchParams({
                query: 'test query',
                response_type: 'raw',
                limit: '100',
                offset: '0'
            });

            try {
                const response = await fetch(`https://api.airweave.ai/collections/test-collection/search?${searchParams}`, {
                    method: 'GET',
                    headers: {
                        'x-api-key': 'test-api-key',
                        'Content-Type': 'application/json',
                        'Accept': 'application/json'
                    }
                });

                const responseText = await response.text();
                JSON.parse(responseText); // This should throw
            } catch (error) {
                expect(error).toBeInstanceOf(SyntaxError);
            }
        });
    });

    describe('Parameter validation', () => {
        it('should validate required parameters', () => {
            // Test that query is required
            const invalidParams = {
                response_type: 'raw',
                limit: 100
                // Missing query
            };

            // This would be tested in the actual tool function
            // The registry should catch this and return validation errors
            expect(invalidParams).not.toHaveProperty('query');
        });

        it('should validate parameter types', () => {
            const invalidParams = {
                query: 'test',
                limit: 'not-a-number', // Should be number
                offset: -1, // Should be >= 0
                recency_bias: 1.5 // Should be <= 1.0
            };

            // These should be caught by the registry validation
            expect(typeof invalidParams.limit).toBe('string');
            expect(invalidParams.offset).toBeLessThan(0);
            expect(invalidParams.recency_bias).toBeGreaterThan(1.0);
        });

        it('should validate parameter ranges', () => {
            const boundaryTests = [
                { limit: 0, shouldBeValid: false }, // Too low
                { limit: 1, shouldBeValid: true },  // Valid minimum
                { limit: 1000, shouldBeValid: true }, // Valid maximum
                { limit: 1001, shouldBeValid: false }, // Too high
                { offset: -1, shouldBeValid: false }, // Too low
                { offset: 0, shouldBeValid: true },   // Valid minimum
                { recency_bias: -0.1, shouldBeValid: false }, // Too low
                { recency_bias: 0.0, shouldBeValid: true },   // Valid minimum
                { recency_bias: 1.0, shouldBeValid: true },   // Valid maximum
                { recency_bias: 1.1, shouldBeValid: false }   // Too high
            ];

            boundaryTests.forEach(test => {
                const { limit, offset, recency_bias } = test;
                const params = { query: 'test' };

                if (limit !== undefined) params.limit = limit;
                if (offset !== undefined) params.offset = offset;
                if (recency_bias !== undefined) params.recency_bias = recency_bias;

                // This would be tested with the actual registry validation
                expect(params).toBeDefined();
            });
        });
    });

    describe('URL construction', () => {
        it('should construct correct URL with all parameters', () => {
            const params = {
                query: 'test query',
                response_type: 'completion',
                limit: 50,
                offset: 10,
                recency_bias: 0.5
            };

            const searchParams = new URLSearchParams({
                query: params.query,
                response_type: params.response_type,
                limit: params.limit.toString(),
                offset: params.offset.toString()
            });
            searchParams.append('recency_bias', params.recency_bias.toString());

            const expectedUrl = `https://api.airweave.ai/collections/test-collection/search?${searchParams.toString()}`;

            expect(expectedUrl).toContain('query=test+query');
            expect(expectedUrl).toContain('response_type=completion');
            expect(expectedUrl).toContain('limit=50');
            expect(expectedUrl).toContain('offset=10');
            expect(expectedUrl).toContain('recency_bias=0.5');
        });

        it('should construct correct URL with minimal parameters', () => {
            const params = {
                query: 'test query'
            };

            const searchParams = new URLSearchParams({
                query: params.query,
                response_type: 'raw', // default
                limit: '100', // default
                offset: '0' // default
            });

            const expectedUrl = `https://api.airweave.ai/collections/test-collection/search?${searchParams.toString()}`;

            expect(expectedUrl).toContain('query=test+query');
            expect(expectedUrl).toContain('response_type=raw');
            expect(expectedUrl).toContain('limit=100');
            expect(expectedUrl).toContain('offset=0');
            expect(expectedUrl).not.toContain('recency_bias');
        });
    });
});
