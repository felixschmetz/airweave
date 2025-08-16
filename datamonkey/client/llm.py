"""Lightweight OpenAI client for datamonkey generation (always fresh).

Uses Chat Completions API and prompts the model to return strict JSON. We parse
the JSON string and validate with Pydantic.
"""

import os
from typing import Optional, Type

from openai import OpenAI
from pydantic import BaseModel

from datamonkey.utils.logging import get_logger


class LLMClient:
    """Thin wrapper around OpenAI Responses API.

    - Reads OPENAI_API_KEY from env (required)
    - Uses OPENAI_MODEL from env or provided override (default: gpt-5)
    - Always uses fresh generation (high temperature)
    """

    def __init__(self, model_override: Optional[str] = None):
        if not os.getenv("OPENAI_API_KEY"):
            raise RuntimeError("OPENAI_API_KEY is required in environment")

        base_url = os.getenv("OPENAI_BASE_URL")
        self.client = OpenAI(base_url=base_url) if base_url else OpenAI()
        self.model = model_override or os.getenv("OPENAI_MODEL", "gpt-5")
        self.logger = get_logger("llm_client")

    async def generate_text(self, instruction: str) -> str:
        """Generate free-form text."""
        self.logger.debug("🧠 LLM generate_text called")
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You generate fresh, varied test data."},
                {"role": "user", "content": instruction},
            ],
            temperature=1.0,
        )
        return resp.choices[0].message.content or ""

    async def generate_structured(self, schema: Type[BaseModel], instruction: str) -> BaseModel:
        """Generate JSON that conforms to the given Pydantic schema."""
        self.logger.debug("🧠 LLM generate_structured called")
        import textwrap

        json_schema = schema.model_json_schema()
        prompt = textwrap.dedent(
            f"""
            Produce a JSON object that strictly conforms to this JSON Schema:
            {json_schema}

            Follow the rules:
            - Return ONLY the JSON object, no markdown, no explanations.
            - Ensure all required fields are present and types match.

            Instruction:
            {instruction}
            """
        ).strip()

        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You return only valid JSON matching the provided schema."},
                {"role": "user", "content": prompt},
            ],
            temperature=1.0,
        )

        content = resp.choices[0].message.content or "{}"
        # Strip code fences if present
        if content.strip().startswith("```"):
            content = content.strip().strip("`\n ")
            if content.startswith("json"):
                content = content[4:].lstrip()

        try:
            return schema.model_validate_json(content)
        except Exception:
            # Try extracting JSON substring
            import re
            match = re.search(r"\{[\s\S]*\}", content)
            if not match:
                raise
            return schema.model_validate_json(match.group(0))


