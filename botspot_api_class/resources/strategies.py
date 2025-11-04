"""
BotSpot API Client - Strategies Resource

Methods for managing AI-generated trading strategies.
"""

import json
from typing import Any, Callable, Dict, List, Optional

import requests

from ..base import BaseResource
from ..exceptions import APIError
from ..prompt_cache import PromptUsageCache


class StrategiesResource(BaseResource):
    """
    Strategies API resource for AI-generated strategies.

    Provides methods for generating, listing, and managing AI trading strategies.
    Supports real-time strategy generation via Server-Sent Events (SSE).
    """

    def __init__(self, client):
        """Initialize strategies resource with prompt usage tracking."""
        super().__init__(client)
        self._prompt_cache = PromptUsageCache()

    def _check_and_display_prompt_usage(self):
        """
        Check current prompt usage and display if changed.

        This method fetches the current usage from the API and compares it
        to the cached state. If usage has changed (or no cache exists),
        displays a color-coded message:
        - Green: Normal (≥ 50 remaining)
        - Yellow: Warning (< 50 remaining)
        - Red: Critical (< 10 remaining)

        Called automatically before AI generation operations.
        """
        try:
            usage_data = self.get_usage_limits()

            # Extract usage info (API response format may vary)
            prompts_used = usage_data.get("promptsUsed", 0)
            max_prompts = usage_data.get("maxPrompts", 500)

            # Check and display if changed
            self._prompt_cache.check_and_display_if_changed(prompts_used, max_prompts)

        except Exception as e:
            # Don't fail the operation if usage check fails
            import logging

            logging.getLogger(__name__).warning(f"Failed to check prompt usage: {e}")

    def list(self) -> List[Dict[str, Any]]:
        """
        List all AI-generated strategies for the current user.

        Returns:
            List of AI strategy dictionaries with nested strategy objects

        Example:
            >>> client = BotSpot()
            >>> ai_strategies = client.strategies.list()
            >>> for ai_strategy in ai_strategies:
            ...     strategy = ai_strategy['strategy']
            ...     print(f"{strategy['name']}: {ai_strategy['revisionCount']} revisions")
            SMA Crossover: 1 revisions
            TSLA Trend Algo: 1 revisions
        """
        response = self._get("/ai-bot-builder/list-strategies")

        # Response format: {"user_id": "...", "aiStrategies": [...]}
        if isinstance(response, dict) and "aiStrategies" in response:
            return response["aiStrategies"]

        return []

    def get_usage_limits(self) -> Dict[str, Any]:
        """
        Get user's prompt usage limits (remaining prompt quota).

        Returns:
            Usage limits information (used/limit/remaining prompts)

        Example:
            >>> client = BotSpot()
            >>> limits = client.strategies.get_usage_limits()
            >>> print(f"Prompts used: {limits}")
        """
        return self._get("/ai-bot-builder/usage-limits")

    def get(self, strategy_id: str) -> Dict[str, Any]:
        """
        Get details for a specific strategy.

        Args:
            strategy_id: Strategy ID

        Returns:
            Strategy dictionary

        Example:
            >>> client = BotSpot()
            >>> strategy = client.strategies.get("abc123")
            >>> print(strategy['name'])
        """
        response = self._get(f"/strategies/{strategy_id}")

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("strategy", response)
            return response.get("strategy", response)

        return response

    def create(
        self,
        name: str,
        description: Optional[str] = None,
        code: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Create a new strategy.

        Args:
            name: Strategy name
            description: Optional strategy description
            code: Optional strategy code (Python)
            **kwargs: Additional strategy parameters

        Returns:
            Created strategy dictionary

        Example:
            >>> client = BotSpot()
            >>> strategy = client.strategies.create(
            ...     name="My First Strategy",
            ...     description="A simple buy and hold strategy",
            ...     code="# Strategy code here"
            ... )
        """
        data = {
            "name": name,
            "description": description,
            "code": code,
            **kwargs,
        }

        # Remove None values
        data = {k: v for k, v in data.items() if v is not None}

        response = self._post("/strategies", data=data)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("strategy", response)
            return response.get("strategy", response)

        return response

    def update(self, strategy_id: str, **kwargs) -> Dict[str, Any]:
        """
        Update an existing strategy.

        Args:
            strategy_id: Strategy ID
            **kwargs: Strategy fields to update

        Returns:
            Updated strategy dictionary

        Example:
            >>> client = BotSpot()
            >>> strategy = client.strategies.update(
            ...     "abc123",
            ...     name="Updated Strategy Name",
            ...     description="New description"
            ... )
        """
        response = self._put(f"/strategies/{strategy_id}", data=kwargs)

        # Handle response format
        if isinstance(response, dict):
            if response.get("success"):
                return response.get("strategy", response)
            return response.get("strategy", response)

        return response

    def delete(self, strategy_id: str) -> Dict[str, Any]:
        """
        Delete a strategy.

        Args:
            strategy_id: Strategy ID

        Returns:
            Deletion confirmation

        Example:
            >>> client = BotSpot()
            >>> result = client.strategies.delete("abc123")
        """
        return self._delete(f"/strategies/{strategy_id}")

    def get_versions(self, ai_strategy_id: str) -> Dict[str, Any]:
        """
        Get all versions/revisions of an AI strategy.

        **Primary endpoint for viewing strategy results** - returns complete strategy data
        including generated code, Mermaid diagrams, descriptions, and metadata.

        Args:
            ai_strategy_id: AI Strategy ID (UUID)

        Returns:
            Dictionary with complete strategy data:
            {
                "user_id": str,              # User UUID
                "aiStrategyId": str,         # AI Strategy UUID
                "strategy": {                # Strategy metadata
                    "id": str,               # Strategy UUID
                    "name": str,             # Strategy name (editable)
                    "description": str | None,
                    "strategyType": "AI",
                    "isPublic": bool,
                    "createdAt": str,        # ISO-8601 timestamp
                    "updatedAt": str         # ISO-8601 timestamp
                },
                "versions": [                # All revisions
                    {
                        "version": int,      # Version number (1-indexed)
                        "code_in": None,     # (Reserved for future use)
                        "code_out": str,     # Full Python Lumibot strategy code
                        "comments": str | None,  # AI-generated description
                        "mermaidDiagram": str | None,  # Flowchart syntax
                        "backtestMetrics": dict | None  # (If backtested)
                    }
                ]
            }

        Example:
            >>> client = BotSpot()
            >>> # Get complete strategy data (code, diagram, metadata)
            >>> data = client.strategies.get_versions("372cd38b-6d0d-43eb-8035-4701ab2a1692")
            >>>
            >>> # Extract strategy name and metadata
            >>> strategy_name = data['strategy']['name']
            >>> print(f"Strategy: {strategy_name}")
            >>>
            >>> # Get latest version code
            >>> latest = data['versions'][0]
            >>> code = latest['code_out']
            >>> print(f"Version {latest['version']}: {len(code)} characters")
            >>>
            >>> # Get Mermaid diagram (if available)
            >>> if latest['mermaidDiagram']:
            ...     print("Diagram available:", latest['mermaidDiagram'][:50])
            Strategy: SMA Crossover
            Version 1: 8313 characters
            Diagram available: flowchart TB\\n  start[Start] --> check_position...
        """
        return self._get("/ai-bot-builder/list-versions", params={"aiStrategyId": ai_strategy_id})

    def generate_diagram(self, python_code: str, revision_id: str, skip_render: bool = True) -> Dict[str, Any]:
        """
        Generate Mermaid flowchart diagram from strategy code.

        Args:
            python_code: Full Python strategy code
            revision_id: Revision ID (UUID) from strategy version
            skip_render: Skip rendering (default True for API-only response)

        Returns:
            Dictionary with 'mmd' (Mermaid markdown), 'revisionId', 'skipRender'

        Example:
            >>> client = BotSpot()
            >>> code = "from lumibot.strategies.strategy import Strategy\\n..."
            >>> diagram = client.strategies.generate_diagram(code, "revision-uuid")
            >>> print(diagram['mmd'])
            flowchart TB...
        """
        data = {"python": python_code, "revisionId": revision_id, "skipRender": skip_render}

        return self._post("/ai-bot-builder/generate-diagram", data=data)

    def generate(
        self, prompt: str, files: Optional[List] = None, progress_callback: Optional[Callable[[Dict], None]] = None
    ) -> Dict[str, Any]:
        """
        Generate a new AI strategy from natural language prompt via SSE streaming.

        **Automatically checks and displays prompt usage before generation.**

        Uses Server-Sent Events (SSE) to stream real-time progress updates during
        the 2-3 minute generation process powered by GPT-5 (OpenAI).

        Args:
            prompt: Natural language strategy description
            files: Optional list of file attachments
            progress_callback: Optional callback function called for each SSE event
                              Receives event dict with keys: action, phase, content, etc.

        Returns:
            Dictionary with generation results:
            {
                "generated_code": str,  # Full Python Lumibot strategy code
                "strategy_name": str,   # AI-generated strategy name
                "description": str,     # Strategy description
                "events": List[Dict],   # All SSE events received
                "usage": Dict          # Token usage info (model, tokens, etc.)
            }

        Raises:
            APIError: If generation fails or API returns error

        Example:
            >>> client = BotSpot()
            >>> # Automatically displays: "AI Prompt Usage: 3/500 used (497 remaining)"
            >>>
            >>> def on_progress(event):
            ...     print(f"Progress: {event.get('action')} - {event.get('content', '')[:50]}")
            >>>
            >>> result = client.strategies.generate(
            ...     prompt="Create a simple moving average crossover strategy for SPY",
            ...     progress_callback=on_progress
            ... )
            >>> print(f"Generated: {result['strategy_name']}")
            >>> print(f"Code length: {len(result['generated_code'])} characters")

        Generation details:
        - Takes approximately 2-3 minutes
        - Uses GPT-5 (OpenAI) with medium reasoning effort
        - Streams progress events: prompt_to_ai → thinking → code_generation_started →
          code_generation_completed → validation_started → strategy_generated
        - Includes :heartbeat messages during long operations

        Prompt usage warnings:
        - Green: Normal (≥ 50 remaining)
        - Yellow: Warning (< 50 remaining)
        - Red: Critical (< 10 remaining)
        """

        # Check and display prompt usage BEFORE generation
        self._check_and_display_prompt_usage()

        # Prepare SSE request
        url = f"{self.API_BASE}/sse/stream"
        headers = self._get_headers()
        headers["Accept"] = "text/event-stream"

        payload = {
            "type": "generate_strategy",
            "prompt": prompt,
            "aiStrategyId": "",  # Empty for new strategy
            "message": prompt,
            "files": files or [],
        }

        # Stream SSE response
        try:
            response = requests.post(url, headers=headers, json=payload, stream=True, timeout=300)

            if response.status_code != 200:
                raise APIError(
                    f"Strategy generation failed with status {response.status_code}",
                    status_code=response.status_code,
                    response_data=response.text,
                )

            # Parse SSE events
            events = []
            generated_code = None
            usage_info = None
            conversation_id = None

            for line in response.iter_lines(decode_unicode=True):
                if not line:
                    continue

                # Skip heartbeat messages
                if line == ":heartbeat":
                    continue

                # Parse data events
                if line.startswith("data: "):
                    event_data = line[6:]  # Remove "data: " prefix
                    try:
                        event = json.loads(event_data)
                        events.append(event)

                        # Capture conversation ID from first event for potential recovery
                        if conversation_id is None and "conversationId" in event:
                            conversation_id = event["conversationId"]

                        # Call progress callback if provided
                        if progress_callback:
                            progress_callback(event)

                        # Capture final generated code
                        if event.get("action") == "strategy_generated" and event.get("phase") == "complete":
                            generated_code = event.get("generatedCode")
                            usage_info = event.get("usage", {})
                            break  # Stop after final event

                    except json.JSONDecodeError:
                        # Skip invalid JSON lines
                        pass

            # Verify we got the generated code
            if generated_code is None:
                # If we captured a conversation ID, provide it for recovery
                error_msg = "Strategy generation stream ended without returning code."
                if conversation_id:
                    error_msg += (
                        f"\n\nConversation ID: {conversation_id}\n"
                        f"The generation may still be in progress server-side.\n"
                        f"Check client.strategies.list() in a few minutes to see if it completed."
                    )

                raise APIError(
                    error_msg,
                    status_code=response.status_code,
                    response_data=f"Events captured: {len(events)}",
                )

            # Extract strategy name from code (parse class name)
            strategy_name = "Generated Strategy"
            if "class " in generated_code:
                try:
                    class_line = [line for line in generated_code.split("\n") if line.strip().startswith("class ")][0]
                    strategy_name = class_line.split("class ")[1].split("(")[0].strip()
                except (IndexError, AttributeError):
                    pass

            # Extract description from comments
            description = ""
            if '"""' in generated_code:
                try:
                    desc_start = generated_code.find('"""') + 3
                    desc_end = generated_code.find('"""', desc_start)
                    description = generated_code[desc_start:desc_end].strip()
                except (ValueError, AttributeError):
                    pass

            return {
                "generated_code": generated_code,
                "strategy_name": strategy_name,
                "description": description,
                "events": events,
                "usage": usage_info or {},
            }

        except requests.exceptions.Timeout as e:
            raise APIError("Strategy generation timed out after 5 minutes", status_code=408) from e
        except requests.exceptions.RequestException as e:
            raise APIError(f"Request failed during strategy generation: {e}") from e

    def save_to_file(
        self,
        code: str,
        filename: str,
        output_dir: str = "strategies",
        overwrite: bool = False,
    ) -> str:
        """
        Save strategy code to a local Python file.

        Args:
            code: The Python strategy code to save
            filename: Base filename (e.g., "my_strategy" or "my_strategy.py")
            output_dir: Directory to save to (default: "strategies" in current directory)
            overwrite: If True, overwrite existing file (default: False)

        Returns:
            Absolute path to the saved file

        Raises:
            FileExistsError: If file exists and overwrite=False
            OSError: If directory creation or file write fails

        Example:
            >>> client = BotSpot()
            >>> result = client.strategies.generate("Create a SMA crossover strategy")
            >>> code = result['generated_code']
            >>> strategy_name = result['strategy_name']
            >>>
            >>> # Save to strategies/sma_crossover.py
            >>> filepath = client.strategies.save_to_file(
            ...     code=code,
            ...     filename=strategy_name.lower().replace(" ", "_")
            ... )
            >>> print(f"Strategy saved to: {filepath}")
            Strategy saved to: /Users/marvin/repos/lumibot/strategies/smacrossoverstrategy.py
        """
        from pathlib import Path

        # Ensure filename has .py extension
        if not filename.endswith(".py"):
            filename = f"{filename}.py"

        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Full file path
        filepath = output_path / filename

        # Check if file exists
        if filepath.exists() and not overwrite:
            raise FileExistsError(
                f"File already exists: {filepath}\n"
                f"Use overwrite=True to replace it, or choose a different filename."
            )

        # Write code to file
        try:
            filepath.write_text(code, encoding="utf-8")
            return str(filepath.absolute())
        except OSError as e:
            raise OSError(f"Failed to write strategy file: {e}") from e
