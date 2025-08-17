"""Gmail-specific generation adapter: email generator."""

from typing import Tuple

from monke.generation.schemas.gmail import GmailArtifact
from monke.client.llm import LLMClient


async def generate_gmail_artifact(model: str, token: str, is_update: bool = False) -> Tuple[str, str]:
    """Generate a Gmail email via LLM.

    Returns (subject, body). The token must be embedded in the output by instruction.
    """
    llm = LLMClient(model_override=model)

    if is_update:
        instruction = (
            "You are generating an updated email for a test Gmail inbox. "
            "Create a follow-up email to a synthetic tech product announcement. "
            "Include the literal token '{token}' somewhere in the body. "
            "Keep it professional but synthetic."
        )
    else:
        instruction = (
            "You are generating an email for a test Gmail inbox. "
            "Create a synthetic email about a new tech product announcement. "
            "Include the literal token '{token}' somewhere in the body. "
            "Keep it professional but synthetic."
        )

    instruction = instruction.format(token=token)
    artifact = await llm.generate_structured(GmailArtifact, instruction)
    
    # Add token to body if not already present
    body = artifact.body
    if token not in body:
        body += f"\n\nReference: {token}"
    
    return artifact.subject, body
