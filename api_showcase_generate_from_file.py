#!/usr/bin/env python3
"""
BotSpot API Client - AI Strategy Generation from File
Reads prompt/code from TEST_EXPORT.TXT and generates strategy

Usage:
    python api_showcase_generate_from_file.py
"""

import os
import re

from botspot_api_class import BotSpot


def check_for_sensitive_patterns(content):
    """
    Check for common sensitive information patterns in content.
    Returns a list of detected pattern types.
    """
    sensitive_patterns = {
        "API Key": r'(?i)(api[_-]?key|apikey|api[_-]?secret)\s*[:=]\s*[\'"]?[\w\-]{20,}[\'"]?',
        "Password": r'(?i)(password|passwd|pwd)\s*[:=]\s*[\'"][^\'"]+[\'"]',
        "Token": r'(?i)(token|auth[_-]?token|access[_-]?token)\s*[:=]\s*[\'"]?[\w\-\.]{20,}[\'"]?',
        "Private Key": r"-----BEGIN (?:RSA |EC )?PRIVATE KEY-----",
        "AWS Credentials": r"(?i)(aws[_-]?access[_-]?key[_-]?id|aws[_-]?secret[_-]?access[_-]?key)",
    }

    detected = []
    for pattern_name, pattern_regex in sensitive_patterns.items():
        if re.search(pattern_regex, content):
            detected.append(pattern_name)

    return detected


def main():
    """Generate an AI trading strategy from file content."""

    print("\n" + "=" * 70)
    print("  🤖 BotSpot AI Strategy Generator (From File)")
    print("=" * 70)

    # Read the prompt from TEST_EXPORT.TXT
    file_path = os.path.join(os.path.dirname(__file__), "TEST_EXPORT.TXT")

    try:
        with open(file_path) as f:
            prompt = f.read()
    except FileNotFoundError:
        print(f"\n  ❌ Error: Could not find {file_path}")
        print("  Please ensure TEST_EXPORT.TXT exists in the same directory.\n")
        return
    except Exception as e:
        print(f"\n  ❌ Error reading file: {e}\n")
        return

    # Check for sensitive patterns in the content
    sensitive_patterns = check_for_sensitive_patterns(prompt)
    if sensitive_patterns:
        print("\n" + "!" * 70)
        print("  ⚠️  SECURITY WARNING: Potential sensitive data detected!")
        print("!" * 70)
        print(f"\n  Detected patterns: {', '.join(sensitive_patterns)}")
        print("\n  The file content may contain sensitive information that will be")
        print("  sent to the BotSpot API. Please review the file carefully.")
        print("\n  Continue anyway? (yes/no): ", end="")

        response = input().strip().lower()
        if response not in ["yes", "y"]:
            print("\n  ❌ Operation cancelled by user.\n")
            return
        print()

    with BotSpot() as client:
        print(f"\n📝 Prompt loaded from: {file_path}")
        print(f"📏 Prompt length: {len(prompt)} characters")
        print("\n⚠️  WARNING: The file content will be sent to BotSpot API.")
        print("   Ensure TEST_EXPORT.TXT does not contain sensitive information")
        print("   (API keys, credentials, proprietary algorithms, etc.)")
        print("\n" + "-" * 70)
        print("  ⏳ Generating strategy (takes 2-3 minutes)...")
        print("-" * 70 + "\n")

        # Progress callback for real-time updates
        def on_progress(event):
            action = event.get("action", "")
            phase = event.get("phase", "")

            # Display meaningful progress updates
            if action == "prompt_to_ai":
                print(f"  📤 Sending prompt to AI ({phase})...")
            elif action == "thinking":
                print(f"  🤔 AI processing request ({phase})...")
            elif action == "code_generation_started":
                print("  ⚙️  Code generation started...")
            elif action == "code_generation_completed":
                print("  ✅ Code generation completed!")
            elif action == "validation_started":
                print("  🔍 Validating generated code...")
            elif action == "strategy_generated":
                print("  🎉 Strategy generation complete!")

        # Generate strategy with progress tracking
        try:
            result = client.strategies.generate(prompt=prompt, progress_callback=on_progress)

            # Display results
            print("\n" + "=" * 70)
            print("  ✅ Generation Complete!")
            print("=" * 70)

            print(f"\n  📛 Strategy Name: {result['strategy_name']}")
            print(f"  📏 Code Length: {len(result['generated_code'])} characters")
            print(f"  📊 Events Captured: {len(result['events'])}")

            # Show usage info
            usage = result.get("usage", {})
            if usage:
                print("\n  🔢 Token Usage:")
                print(f"     Model: {usage.get('model', 'N/A')}")
                print(f"     Input Tokens: {usage.get('input_tokens', 'N/A')}")
                print(f"     Output Tokens: {usage.get('output_tokens', 'N/A')}")
                print(f"     Total Tokens: {usage.get('total_tokens', 'N/A')}")

            # Show code preview
            print("\n  📄 Code Preview (first 500 chars):")
            print("  " + "-" * 66)
            code_preview = result["generated_code"][:500].replace("\n", "\n  ")
            print(f"  {code_preview}...")
            print("  " + "-" * 66)

            print("\n  💾 Full code available in result['generated_code']")
            print("=" * 70 + "\n")

        except Exception as e:
            print(f"\n  ❌ Error: {e}\n")
            raise


if __name__ == "__main__":
    main()
