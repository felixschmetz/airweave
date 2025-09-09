// Configuration constants for the MCP server

export const DEFAULT_BASE_URL = "https://api.airweave.ai";
export const DEFAULT_LIMIT = 100;
export const DEFAULT_OFFSET = 0;
export const DEFAULT_RESPONSE_TYPE = "raw";

export const PARAMETER_LIMITS = {
    QUERY_MIN_LENGTH: 1,
    QUERY_MAX_LENGTH: 1000,
    LIMIT_MIN: 1,
    LIMIT_MAX: 1000,
    OFFSET_MIN: 0,
    RECENCY_BIAS_MIN: 0.0,
    RECENCY_BIAS_MAX: 1.0
} as const;

export const ERROR_MESSAGES = {
    MISSING_API_KEY: "Error: AIRWEAVE_API_KEY environment variable is required",
    MISSING_COLLECTION: "Error: AIRWEAVE_COLLECTION environment variable is required",
    INVALID_JSON_RESPONSE: "Invalid JSON response",
    API_ERROR: "Airweave API error",
    PARAMETER_VALIDATION_ERROR: "Parameter Validation Errors"
} as const;
