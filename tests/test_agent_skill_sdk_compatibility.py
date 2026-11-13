"""Native skill adapters must support the declared ADK BaseTool contract."""
import asyncio

from lumibot.components.agents.runtime import _agent_toolset_with_purpose
from lumibot.components.agents.skills import build_builtin_skill_toolset


def test_builtin_skill_tools_wrap_with_supported_sdk_metadata():
    native = build_builtin_skill_toolset()
    original = asyncio.run(native.get_tools())
    wrapped = asyncio.run(_agent_toolset_with_purpose(native, {}).get_tools_with_prefix())
    assert {tool.name for tool in wrapped} == {tool.name for tool in original}
    assert {"list_skills", "load_skill", "load_skill_resource"}.issubset({tool.name for tool in wrapped})
    for tool in wrapped:
        assert tool.original in original
        assert tool.is_long_running == tool.original.is_long_running
        assert tool.custom_metadata == tool.original.custom_metadata
        assert tool.response_scheduling == tool.original.response_scheduling
        assert tool._defers_response == tool.original._defers_response
        assert "agent_purpose" in tool._get_declaration().parameters_json_schema["required"]


def test_native_skill_adapter_preserves_optional_behavior():
    native = build_builtin_skill_toolset()
    original = asyncio.run(native.get_tools())
    for tool in original:
        tool.behavior = "synthetic-optional-behavior"
    wrapped = asyncio.run(_agent_toolset_with_purpose(native, {}).get_tools_with_prefix())
    assert all(tool.behavior == "synthetic-optional-behavior" for tool in wrapped)
