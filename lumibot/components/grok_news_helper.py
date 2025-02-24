import json
import datetime
from openai import OpenAI

class GrokNewsHelper:
    """
    GrokNewsHelper queries xAI's Grok API via the OpenAI-compatible client to fetch 
    financial/news data in a structured JSON format.

    How it Works:
    -------------
    1. Build a system prompt describing the desired JSON schema in detail.
    2. Use xAI's OpenAI-compatible client to create a chat completion with:
       - A system message (detailed instructions).
       - A user message (the actual user query).
    3. Parse the assistant's text content as JSON. 
    4. Return a Python dictionary with fields:
       {
         "query": ...,
         "timestamp_utc": ...,
         "analysis_summary": ...,
         "items": [...]
       }

    Example:
    --------
    from grok_news_helper import GrokNewsHelper

    helper = GrokNewsHelper(api_key="YOUR_XAI_API_KEY")
    result = helper.execute_query("What stocks are trending right now on Twitter?")
    print(json.dumps(result, indent=2))
    """

    def __init__(self, api_key: str):
        """
        Initialize the GrokNewsHelper with your xAI API key. 
        Internally creates an xAI OpenAI-compatible client.

        Parameters
        ----------
        api_key : str
            The xAI API key (required).
        """
        if not api_key:
            # Try to get the API key from the environment if not provided
            import os

            # Might be called GROK_API_KEY or XAI_API_KEY
            api_key = os.getenv("GROK_API_KEY")
            if not api_key:
                api_key = os.getenv("XAI_API_KEY")

            if not api_key:
                raise ValueError("API key is required for GrokNewsHelper. Get one from x.ai and set it as GROK_API_KEY or XAI_API_KEY in your secrets, environment variables, .env file or directly in the code.")
        
        self.api_key = api_key

        # Create the xAI client (OpenAI-compatible) 
        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://api.x.ai/v1",
        )

    def build_prompt(self, user_query: str) -> str:
        """
        Constructs a system prompt describing the required JSON schema in detail
        and instructing the model to avoid hallucinations and remain factual.

        The schema includes fields like 'symbol', 'confidence', 'magnitude', etc.
        Now also includes an optional 'price_targets' object with float fields.

        Parameters
        ----------
        user_query : str
            The question or prompt about financial/news data.

        Returns
        -------
        str
            The system message content with instructions for Grok.
        """
        system_prompt = f"""\
You are a financial news aggregator assistant with real-time access to Twitter, news feeds, 
and other finance data. Remain factual and accurate in your response (avoid hallucinations).

Your task is to return ONLY valid JSON, following this exact schema:

JSON Schema:
{{
  "query": "<string, required - echo the user's query>",
  "timestamp_utc": "<string, required - current UTC time in ISO 8601 format>",
  "analysis_summary": "<string, required - short summary of the findings>",
  "items": [
    {{
      "symbol": "<string, required - e.g. 'AAPL', 'BTC'. Always neds to be a valid symbol>",
      "asset_type": "<string, required - 'stock', 'crypto', 'index', 'commodity', 'forex', 'none'>",
      "headline": "<string, required - short note on why it's trending>",
      "confidence": "<integer, 0-10, required - reliability score>",
      "sentiment_score": "<integer, -10 to 10, required - negative is bearish, positive is bullish>",
      "popularity_metric": "<integer >= 0, required - measure of mentions>",
      "volume_of_messages": "<integer, optional - if known>",
      "magnitude": "<integer, 0-10, required - overall impact level>",
      "type_of_news": "<string, optional - e.g. 'earnings', 'ipo', 'macro', 'ceo_tweet'>",
      "price_targets": {{
        "low": "<float, optional>",
        "high": "<float, optional>",
        "average": "<float, optional>"
      }},
      "additional_info": {{
        "sector": "<string, optional>",
        "recent_events": "<string, optional>",
        "notable_executive_actions": "<string, optional>",
        "macro_support": "<string, optional>",
        "related_tickers": ["<string>", "..."],
        "external_links": ["<string>", "..."]
      }}
    }}
  ]
}}

Instructions:
1) Output MUST be valid JSON (no extra text or markdown).
2) If no data found, 'items' can be empty.
3) The 'magnitude' field is required (0-10).
4) Provide minimal text in 'analysis_summary' (1-3 sentences).
5) The field 'query' must repeat the user's question exactly.

Now, the user's query is:
\"{user_query}\"

Return only valid JSON following the schema.
"""
        return system_prompt

    def execute_query(self, user_query: str) -> dict:
        """
        Executes a query by creating a chat completion via the xAI OpenAI-compatible client.
        We pass a system message (JSON schema instructions) + user message (user_query).

        Steps:
        ------
        1) Build the system prompt with `build_prompt(user_query)`.
        2) Call `client.chat.completions.create(...)` with:
           - model = "grok-2-latest" (or any available model)
           - messages (system + user).
           - temperature=0 to reduce hallucinations.
        3) Parse the assistant's output text as JSON.
        4) Return a dictionary matching the schema. If parsing fails, return an error structure.

        Parameters
        ----------
        user_query : str
            The textual query about financial/news data (e.g. "Which stocks are trending now?").

        Returns
        -------
        dict
            A dictionary conforming to the described JSON schema:
            {
              "query": "...",
              "timestamp_utc": "...",
              "analysis_summary": "...",
              "items": [...]
            }
        """
        # 1) Build system prompt
        system_msg = self.build_prompt(user_query)

        # 2) Create the chat completion using the xAI client
        try:
            completion = self.client.chat.completions.create(
                model="grok-2-latest",
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_query}
                ],
                temperature=0,
                stream=False
            )
        except Exception as e:
            # If there's a network or API error, fallback with an error structure
            return {
                "query": user_query,
                "timestamp_utc": datetime.datetime.now(datetime.UTC).isoformat()
                    if hasattr(datetime, 'UTC') else datetime.datetime.utcnow().isoformat(),
                "analysis_summary": f"Error calling xAI API: {str(e)}",
                "items": []
            }

        # 3) Extract the assistant's text (the content of the first choice)
        try:
            assistant_text = completion.choices[0].message.content
        except (AttributeError, IndexError, KeyError) as e:
            return {
                "query": user_query,
                "timestamp_utc": datetime.datetime.now(datetime.UTC).isoformat()
                    if hasattr(datetime, 'UTC') else datetime.datetime.utcnow().isoformat(),
                "analysis_summary": f"Error: Unexpected response format from Grok API. {str(e)}",
                "items": []
            }

        # 4) Parse the assistant's text as JSON
        try:
            data = json.loads(assistant_text)
        except json.JSONDecodeError as e:
            return {
                "query": user_query,
                "timestamp_utc": datetime.datetime.now(datetime.UTC).isoformat()
                    if hasattr(datetime, 'UTC') else datetime.datetime.utcnow().isoformat(),
                "analysis_summary": f"Error: LLM output was not valid JSON. {str(e)}",
                "items": []
            }

        # 5) Post-process to ensure numeric fields are integers
        self._post_process_data(data)

        return data

    def _post_process_data(self, data: dict) -> None:
        """
        Enforces that certain fields are integers:
         - confidence
         - sentiment_score
         - popularity_metric
         - magnitude
         - volume_of_messages (if present)

        Also handles optional price_targets as floats if present.

        Modifies the 'data' dictionary in-place.

        Parameters
        ----------
        data : dict
            The parsed JSON dictionary from the assistant.
        """
        items = data.get("items", [])
        for item in items:
            for int_field in ("confidence", "sentiment_score", "popularity_metric", "magnitude"):
                if int_field in item:
                    try:
                        item[int_field] = int(item[int_field])
                    except (ValueError, TypeError):
                        item[int_field] = 0

            if "volume_of_messages" in item:
                try:
                    item["volume_of_messages"] = int(item["volume_of_messages"])
                except (ValueError, TypeError):
                    item["volume_of_messages"] = 0

            # Optional price_targets handling
            if "price_targets" in item and isinstance(item["price_targets"], dict):
                for float_field in ("low", "high", "average"):
                    if float_field in item["price_targets"]:
                        try:
                            item["price_targets"][float_field] = float(item["price_targets"][float_field])
                        except (ValueError, TypeError):
                            item["price_targets"][float_field] = None


# ------------------------------------------------------------------------------
# Example usage in a standalone script:
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Attempt to retrieve xAI API key from environment
    import os
    import dotenv

    dotenv.load_dotenv()
    xai_api_key = os.getenv("XAI_API_KEY")
    if not xai_api_key or xai_api_key == "YOUR_XAI_API_KEY":
        print("WARNING: No valid XAI_API_KEY found in environment!")
        xai_api_key = "YOUR_XAI_API_KEY"  # fallback

    nq_helper = GrokNewsHelper(api_key=xai_api_key)

    # Example user query
    # user_query = "What drugs from small biotech companies are expected to get FDA approvals soon?"
    user_query = "what is the twitter account @Banana3Stocks recommending to buy or sell right now and at what price targets? he usually says things like 'see you at XX price' or 'XX price soon' or 'run to XX' or 'pivot is XX' or 'XX later this year' or 'ready for $XX'"
    result = nq_helper.execute_query(user_query)

    # Print the structured response
    print(json.dumps(result, indent=2))