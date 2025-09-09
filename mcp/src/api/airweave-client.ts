// Airweave API client for making requests

import { AirweaveConfig, SearchResponse } from './types.js';

export class AirweaveClient {
    constructor(private config: AirweaveConfig) { }

    async search(searchRequest: any): Promise<SearchResponse> {
        const { query, response_type, limit, offset, recency_bias } = searchRequest;

        // Construct the search endpoint URL
        const endpoint = `/collections/${this.config.collection}/search`;
        const searchParams = new URLSearchParams({
            query,
            response_type,
            limit: limit.toString(),
            offset: offset.toString(),
        });

        // Add recency_bias only if provided
        if (recency_bias !== undefined) {
            searchParams.append('recency_bias', recency_bias.toString());
        }

        const url = `${this.config.baseUrl}${endpoint}?${searchParams.toString()}`;

        const response = await fetch(url, {
            method: 'GET',
            headers: {
                'x-api-key': this.config.apiKey,
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(`Airweave API error (${response.status}): ${errorText}`);
        }

        const responseText = await response.text();
        let searchResponse: SearchResponse;

        try {
            searchResponse = JSON.parse(responseText);
        } catch (parseError) {
            console.error("Failed to parse JSON response:", responseText.substring(0, 500));
            throw new Error(`Invalid JSON response: ${responseText.substring(0, 200)}...`);
        }

        return searchResponse;
    }
}
