"""Asana content generation adapter.

Generates realistic task content for testing Asana integration.
"""

from typing import List, Tuple


async def generate_asana_task(model: str, token: str) -> Tuple[str, str, List[str]]:
    """Generate task content for Asana testing.
    
    Args:
        model: The LLM model to use (unused for now)
        token: A unique token to embed in the content for verification
    
    Returns:
        Tuple of (title, notes, comments)
    """
    # For now, generate simple test content
    # In a real implementation, this would use an LLM
    
    title = f"Test Task {token} - Implement feature XYZ"
    
    notes = f"""## Task Description

This is a test task created by Datamonkey with token: {token}

### Objectives:
- Implement the new feature as specified
- Ensure all tests pass
- Update documentation

### Technical Details:
The feature should integrate with the existing codebase and follow our coding standards.

**Verification Token**: {token}
"""
    
    comments = [
        f"Initial comment for task {token}: Please review the requirements before starting.",
        f"Follow-up comment: Don't forget to include the verification token {token} in your implementation.",
    ]
    
    return title, notes, comments
