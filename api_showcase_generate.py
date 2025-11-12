#!/usr/bin/env python3
"""
BotSpot API Client - AI Strategy Generation Showcase

Demonstrates real-time AI strategy generation with SSE streaming:
- Automatic prompt usage tracking
- Real-time progress updates
- 2-3 minute generation process
- Powered by GPT-5 (OpenAI)

Usage:
    python api_showcase_generate.py
"""

from botspot_api_class import BotSpot


def main():
    """Generate an AI trading strategy in real-time."""

    print("\n" + "=" * 70)
    print("  🤖 BotSpot AI Strategy Generator")
    print("=" * 70)

    with BotSpot() as client:
        # Define the strategy prompt
        prompt = (
            "Create a simple RSI oversold/overbought strategy for QQQ that buys when RSI < 30 and sells when RSI > 70"
        )

        print(f"\n📝 Prompt: {prompt}")
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
