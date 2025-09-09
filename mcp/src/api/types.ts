// Type definitions for Airweave API responses

export interface SearchResponse {
    results: Record<string, any>[];
    response_type: string;
    completion?: string;
    status: string;
}

export interface SearchRequest {
    query: string;
    response_type?: string;
    limit?: number;
    offset?: number;
    recency_bias?: number;
}

export interface AirweaveConfig {
    apiKey: string;
    collection: string;
    baseUrl: string;
}
